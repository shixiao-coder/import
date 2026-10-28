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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads and parses the NL StatisticalVariable CSV file (mapping semicolon-separated {@code dcid}
 * values to natural-language {@code sentence} strings), mirroring {@code _extract_nl_stat_var()} in
 * aggregation-helper.
 */
public final class NlStatVarLoader implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(NlStatVarLoader.class);
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

  /** Represents a single deduplicated (dcid, sentence) pair. */
  public record NlStatVarRecord(String dcid, String sentence) implements Serializable {}

  private NlStatVarLoader() {}

  /**
   * Loads deduplicated {@link NlStatVarRecord} entries from a GCS URI ({@code gs://...}) or local
   * file path.
   */
  public static List<NlStatVarRecord> loadRecords(String filePath) {
    if (filePath == null || filePath.trim().isEmpty()) {
      return List.of();
    }
    String rawCsv = readCsvContent(filePath.trim());
    return parseCsvContent(rawCsv);
  }

  /**
   * Groups {@link NlStatVarRecord} entries into an ordered map from {@code dcid} to its list of
   * associated sentences.
   */
  public static Map<String, List<String>> groupByDcid(List<NlStatVarRecord> records) {
    Map<String, List<String>> grouped = new LinkedHashMap<>();
    if (records == null) {
      return grouped;
    }
    for (NlStatVarRecord rec : records) {
      grouped.computeIfAbsent(rec.dcid(), k -> new ArrayList<>()).add(rec.sentence());
    }
    return grouped;
  }

  /**
   * Parses CSV content containing {@code dcid} and {@code sentence} columns, splitting
   * semicolon-separated DCIDs and deduplicating {@code (dcid, sentence)} pairs while preserving
   * encounter order.
   */
  public static List<NlStatVarRecord> parseCsvContent(String csvContent) {
    if (csvContent == null || csvContent.trim().isEmpty()) {
      return List.of();
    }

    Set<NlStatVarRecord> seen = new LinkedHashSet<>();
    try (Reader reader = new StringReader(csvContent);
        CSVParser parser =
            CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withIgnoreHeaderCase()
                .withTrim()
                .parse(reader)) {
      Map<String, Integer> headerMap = parser.getHeaderMap();
      if (headerMap == null
          || !headerMap.containsKey("dcid")
          || !headerMap.containsKey("sentence")) {
        return List.of();
      }
      for (CSVRecord row : parser) {
        if (!row.isMapped("dcid") || !row.isMapped("sentence")) {
          continue;
        }
        String dcidStr = row.get("dcid");
        String sentence = row.get("sentence");
        if (dcidStr == null || sentence == null) {
          continue;
        }
        String trimmedSentence = sentence.trim();
        if (trimmedSentence.isEmpty()) {
          continue;
        }
        for (String part : dcidStr.split(";")) {
          String trimmedDcid = part.trim();
          if (!trimmedDcid.isEmpty()) {
            seen.add(new NlStatVarRecord(trimmedDcid, trimmedSentence));
          }
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse NL StatVar CSV content.", e);
    }
    return List.copyOf(seen);
  }

  private static String readCsvContent(String path) {
    if (path.startsWith("gs://")) {
      String withoutScheme = path.substring("gs://".length());
      // 1. Try public HTTPS fetch first, matching aggregation-helper's _extract_nl_stat_var
      try {
        URI httpsUri = URI.create("https://storage.googleapis.com/" + withoutScheme);
        HttpClient client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
        HttpRequest request = HttpRequest.newBuilder(httpsUri).timeout(HTTP_TIMEOUT).GET().build();
        HttpResponse<String> response =
            client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
          return response.body();
        }
      } catch (Exception e) {
        LOGGER.info(
            "Public HTTPS fetch for {} failed ({}), falling back to GCS client.",
            path,
            e.getMessage());
      }

      // 2. Fallback to Beam FileSystems / Google Cloud Storage client
      try {
        ResourceId resourceId = FileSystems.matchNewResource(path, false);
        try (InputStream is = Channels.newInputStream(FileSystems.open(resourceId))) {
          return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
      } catch (Exception beamEx) {
        int slashIdx = withoutScheme.indexOf('/');
        if (slashIdx <= 0) {
          throw new IllegalArgumentException("Invalid GCS URI: " + path, beamEx);
        }
        String bucket = withoutScheme.substring(0, slashIdx);
        String object = withoutScheme.substring(slashIdx + 1);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        byte[] bytes = storage.readAllBytes(BlobId.of(bucket, object));
        return new String(bytes, StandardCharsets.UTF_8);
      }
    }

    Path localPath = Paths.get(path);
    if (Files.exists(localPath)) {
      try {
        return Files.readString(localPath, StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to read NL StatVar CSV file: " + path, e);
      }
    }

    try (InputStream is = NlStatVarLoader.class.getClassLoader().getResourceAsStream(path)) {
      if (is != null) {
        return new String(is.readAllBytes(), StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read NL StatVar CSV resource: " + path, e);
    }

    throw new IllegalArgumentException("NL StatVar CSV file not found: " + path);
  }
}
