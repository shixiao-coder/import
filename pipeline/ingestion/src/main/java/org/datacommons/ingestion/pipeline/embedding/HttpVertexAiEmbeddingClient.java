package org.datacommons.ingestion.pipeline.embedding;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link VertexAiEmbeddingClient} implementation that invokes the Vertex AI REST Prediction
 * API ({@code publishers/google/models/{modelEndpoint}:predict}) over HTTPS using Application
 * Default Credentials.
 */
public class HttpVertexAiEmbeddingClient implements VertexAiEmbeddingClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpVertexAiEmbeddingClient.class);
  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";
  private static final Pattern SAFE_IDENTIFIER_PATTERN =
      Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._@-]{0,127}$");
  private static final int MAX_RETRIES = 3;
  private static final long INITIAL_BACKOFF_MS = 1000L;

  private final String projectId;
  private final String location;
  private transient HttpClient httpClient;
  private transient GoogleCredentials credentials;
  private transient Gson gson;

  public HttpVertexAiEmbeddingClient(String projectId, String location) {
    this.projectId = validateIdentifier(projectId, "projectId");
    this.location = validateIdentifier(location, "location");
  }

  static String validateIdentifier(String value, String fieldName) {
    if (value == null || !SAFE_IDENTIFIER_PATTERN.matcher(value).matches()) {
      throw new IllegalArgumentException(
          "Invalid " + fieldName + " for Vertex AI endpoint: " + value);
    }
    return value;
  }

  private synchronized void ensureInitialized() throws IOException {
    if (httpClient == null) {
      httpClient =
          HttpClient.newBuilder()
              .connectTimeout(Duration.ofSeconds(30))
              .version(HttpClient.Version.HTTP_2)
              .build();
    }
    if (credentials == null) {
      credentials =
          GoogleCredentials.getApplicationDefault()
              .createScoped(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
    }
    if (gson == null) {
      gson = new Gson();
    }
  }

  public URI buildEndpointUri(String modelEndpoint) {
    String safeModel = validateIdentifier(modelEndpoint, "modelEndpoint");
    String url =
        String.format(
            "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
            location, projectId, location, safeModel);
    return URI.create(url);
  }

  public static String buildRequestJson(List<String> texts, String taskType) {
    JsonArray instances = new JsonArray();
    for (String text : texts) {
      JsonObject instance = new JsonObject();
      instance.addProperty("content", Objects.requireNonNullElse(text, ""));
      if (taskType != null && !taskType.isBlank()) {
        instance.addProperty("task_type", taskType);
      }
      instances.add(instance);
    }
    JsonObject root = new JsonObject();
    root.add("instances", instances);
    return new Gson().toJson(root);
  }

  public static List<List<Double>> parseResponseJson(String responseBody, int expectedCount) {
    JsonObject root = JsonParser.parseString(responseBody).getAsJsonObject();
    JsonArray predictions = root.getAsJsonArray("predictions");
    if (predictions == null) {
      throw new IllegalStateException(
          "Vertex AI response missing 'predictions' field: " + responseBody);
    }
    if (predictions.size() != expectedCount) {
      throw new IllegalStateException(
          String.format(
              "Expected %d embedding predictions from Vertex AI, but got %d",
              expectedCount, predictions.size()));
    }
    List<List<Double>> results = new ArrayList<>(predictions.size());
    for (JsonElement predElem : predictions) {
      JsonObject predObj = predElem.getAsJsonObject();
      JsonObject embeddingsObj =
          predObj.has("embeddings") ? predObj.getAsJsonObject("embeddings") : predObj;
      JsonArray valuesArray = embeddingsObj.getAsJsonArray("values");
      if (valuesArray == null) {
        throw new IllegalStateException(
            "Vertex AI prediction entry missing 'embeddings.values': " + predObj);
      }
      List<Double> vector = new ArrayList<>(valuesArray.size());
      for (JsonElement val : valuesArray) {
        vector.add(val.getAsDouble());
      }
      results.add(vector);
    }
    return results;
  }

  @Override
  public List<List<Double>> predictEmbeddings(
      List<String> texts, String modelEndpoint, String taskType) {
    if (texts == null || texts.isEmpty()) {
      return List.of();
    }
    try {
      ensureInitialized();
      URI uri = buildEndpointUri(modelEndpoint);
      String requestPayload = buildRequestJson(texts, taskType);

      int attempt = 0;
      long backoffMs = INITIAL_BACKOFF_MS;
      while (true) {
        attempt++;
        credentials.refreshIfExpired();
        String token = credentials.getAccessToken().getTokenValue();

        HttpRequest request =
            HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofSeconds(120))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(requestPayload, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> response =
            httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        int status = response.statusCode();
        if (status >= 200 && status < 300) {
          return parseResponseJson(response.body(), texts.size());
        }
        boolean retryable = (status == 429 || status >= 500);
        if (!retryable || attempt > MAX_RETRIES) {
          throw new RuntimeException(
              String.format(
                  "Vertex AI predict request failed with HTTP %d after %d attempt(s): %s",
                  status, attempt, response.body()));
        }
        LOGGER.warn(
            "Vertex AI predict returned HTTP {} (attempt {}/{}); retrying in {} ms...",
            status,
            attempt,
            MAX_RETRIES,
            backoffMs);
        Thread.sleep(backoffMs);
        backoffMs *= 2;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while calling Vertex AI prediction API", e);
    } catch (IOException e) {
      throw new RuntimeException("I/O error while calling Vertex AI prediction API", e);
    }
  }
}
