// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.ingestion.pipeline.embedding;

import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;

/**
 * Generates {@link EmbeddingContent} payloads and FarmHash64 content keys from filtered {@link
 * EmbeddingNodeCandidate} rows, matching aggregation-helper's {@code EmbeddingGenerator} logic.
 */
public class GenerateEmbeddingContentFn extends DoFn<EmbeddingNodeCandidate, EmbeddingContent> {
  private static final Gson GSON = new Gson();

  private final EmbeddingSpec spec;
  private final Map<String, List<String>> nlStatVarSentencesByDcid;
  private final Counter contentCounter;

  public GenerateEmbeddingContentFn(
      EmbeddingSpec spec, Map<String, List<String>> nlStatVarSentencesByDcid) {
    this.spec = spec;
    this.nlStatVarSentencesByDcid =
        nlStatVarSentencesByDcid == null ? Map.of() : nlStatVarSentencesByDcid;
    this.contentCounter =
        Metrics.counter(
            GenerateEmbeddingContentFn.class,
            "embedding_content_generated:" + spec.embeddingLabel());
  }

  @ProcessElement
  public void processElement(
      @Element EmbeddingNodeCandidate candidate, OutputReceiver<EmbeddingContent> receiver) {
    if (candidate == null
        || candidate.subjectId() == null
        || candidate.subjectId().isEmpty()
        || candidate.name() == null
        || candidate.name().isEmpty()) {
      return;
    }

    for (EmbeddingContent content :
        buildEmbeddingContents(candidate, spec, nlStatVarSentencesByDcid)) {
      contentCounter.inc();
      receiver.output(content);
    }
  }

  /**
   * Builds the {@link EmbeddingContent} item(s) for a single {@link EmbeddingNodeCandidate}.
   *
   * <ul>
   *   <li>{@link NodeFilterType#NL_STATISTICAL_VARIABLE}: emits one {@link EmbeddingContent} per
   *       sentence mapped to the node's {@code subjectId}, where {@code textToEmbed} is the raw
   *       {@code sentence}, {@code embeddingContentKey} is {@code FARM_FINGERPRINT(sentence)}, and
   *       {@code embeddingContentJson} is {@code {"title": subjectId, "name": sentence}}.
   *   <li>{@link NodeFilterType#NO_FILTER} and {@link NodeFilterType#ENTITY_TYPES}:
   *       <ul>
   *         <li>When no additional predicate properties are present: {@code embeddingContentJson}
   *             is {@code {"title": subjectId, "name": name}}, {@code embeddingContentKey} is
   *             {@code FARM_FINGERPRINT(name)}, and {@code textToEmbed} is {@code
   *             embeddingContentJson}.
   *         <li>When predicate properties (e.g. {@code description}) are present: {@code
   *             embeddingContentJson} is {@code {"subject_id": subjectId, "name": name,
   *             "properties": {...}}}, {@code embeddingContentKey} is {@code
   *             FARM_FINGERPRINT(embeddingContentJson)}, and {@code textToEmbed} is {@code
   *             embeddingContentJson}.
   *       </ul>
   * </ul>
   */
  public static List<EmbeddingContent> buildEmbeddingContents(
      EmbeddingNodeCandidate candidate,
      EmbeddingSpec spec,
      Map<String, List<String>> nlStatVarSentencesByDcid) {
    String subjectId = candidate.subjectId();
    String name = candidate.name();
    List<String> nodeTypes = candidate.nodeTypes();

    if (spec.nodeFilterType() == NodeFilterType.NL_STATISTICAL_VARIABLE) {
      List<String> sentences =
          nlStatVarSentencesByDcid != null
              ? nlStatVarSentencesByDcid.getOrDefault(subjectId, Collections.emptyList())
              : Collections.emptyList();
      return sentences.stream()
          .filter(s -> s != null && !s.trim().isEmpty())
          .map(
              sentence -> {
                JsonObject jsonObj = new JsonObject();
                jsonObj.addProperty("title", subjectId);
                jsonObj.addProperty("name", sentence);
                String jsonStr = GSON.toJson(jsonObj);
                String contentKey = computeFarmFingerprintKey(sentence);
                return new EmbeddingContent(
                    subjectId,
                    spec.embeddingLabel(),
                    contentKey,
                    jsonStr,
                    sentence,
                    nodeTypes,
                    spec.modelEndpoint(),
                    spec.taskType());
              })
          .toList();
    }

    if (candidate.properties() != null && !candidate.properties().isEmpty()) {
      JsonObject jsonObj = new JsonObject();
      jsonObj.addProperty("subject_id", subjectId);
      jsonObj.addProperty("name", name);
      JsonObject propsObj = new JsonObject();
      for (Map.Entry<String, String> entry : candidate.properties().entrySet()) {
        propsObj.addProperty(entry.getKey(), entry.getValue());
      }
      jsonObj.add("properties", propsObj);
      String jsonStr = GSON.toJson(jsonObj);
      String contentKey = computeFarmFingerprintKey(jsonStr);
      return List.of(
          new EmbeddingContent(
              subjectId,
              spec.embeddingLabel(),
              contentKey,
              jsonStr,
              jsonStr,
              nodeTypes,
              spec.modelEndpoint(),
              spec.taskType()));
    }

    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty("title", subjectId);
    jsonObj.addProperty("name", name);
    String jsonStr = GSON.toJson(jsonObj);
    String contentKey = computeFarmFingerprintKey(name);
    return List.of(
        new EmbeddingContent(
            subjectId,
            spec.embeddingLabel(),
            contentKey,
            jsonStr,
            jsonStr,
            nodeTypes,
            spec.modelEndpoint(),
            spec.taskType()));
  }

  /**
   * Computes GoogleSQL's {@code CAST(FARM_FINGERPRINT(input) AS STRING)} using Guava's 64-bit
   * FarmHash Fingerprint64 implementation.
   */
  public static String computeFarmFingerprintKey(String input) {
    String safeInput = input == null ? "" : input;
    long fingerprint =
        Hashing.farmHashFingerprint64().hashString(safeInput, StandardCharsets.UTF_8).asLong();
    return Long.toString(fingerprint);
  }
}
