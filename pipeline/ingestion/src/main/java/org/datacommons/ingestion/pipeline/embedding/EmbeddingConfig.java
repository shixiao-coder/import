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

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Global configuration for embedding ingestion, parsing YAML/JSON embedding setups from
 * aggregation-helper ({@code embedding.yaml}) or standalone spec lists.
 */
public record EmbeddingConfig(
    String embeddingTable, List<EmbeddingSpec> specs, String nlStatVarFilePath)
    implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingConfig.class);
  private static final Pattern SAFE_IDENTIFIER_PATTERN =
      Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

  public static final String DEFAULT_EMBEDDING_TABLE = "NodeEmbedding";
  public static final String DEFAULT_NL_STAT_VAR_FILE =
      "gs://datcom-nl-models/base_uae_mem_2025_11_03_07_10_42/embeddings.csv";

  public static final List<EmbeddingSpec> DEFAULT_EMBEDDING_SPECS =
      List.of(
          new EmbeddingSpec(
              "base_text_embedding",
              EmbeddingSpec.DEFAULT_MODEL_NAME,
              EmbeddingSpec.DEFAULT_MODEL_ENDPOINT,
              EmbeddingSpec.DEFAULT_TASK_TYPE,
              Map.of("StatisticalVariable", List.of(), "Topic", List.of()),
              NodeFilterType.NO_FILTER));

  public EmbeddingConfig {
    embeddingTable =
        (embeddingTable == null || embeddingTable.trim().isEmpty())
            ? DEFAULT_EMBEDDING_TABLE
            : validateSqlIdentifier(embeddingTable.trim(), "embeddingTable");
    specs = (specs == null || specs.isEmpty()) ? DEFAULT_EMBEDDING_SPECS : List.copyOf(specs);
    nlStatVarFilePath =
        (nlStatVarFilePath == null || nlStatVarFilePath.trim().isEmpty())
            ? DEFAULT_NL_STAT_VAR_FILE
            : nlStatVarFilePath.trim();
  }

  /** Validates that a SQL table or column identifier contains only safe alphanumeric characters. */
  public static String validateSqlIdentifier(String identifier, String fieldName) {
    if (identifier == null || !SAFE_IDENTIFIER_PATTERN.matcher(identifier).matches()) {
      throw new IllegalArgumentException(
          String.format("Invalid SQL identifier for %s: '%s'", fieldName, identifier));
    }
    return identifier;
  }

  /**
   * Loads {@link EmbeddingConfig} from a local file path, GCS URI ({@code gs://...}), or classpath
   * resource. Falls back to {@link #DEFAULT_EMBEDDING_SPECS} when {@code configPath} is null or
   * empty.
   */
  public static EmbeddingConfig load(
      String configPath, String overrideEmbeddingTable, String nlStatVarFilePath) {
    if (configPath == null || configPath.trim().isEmpty()) {
      LOGGER.info("No embeddingConfigPath specified; using default embedding specs.");
      return new EmbeddingConfig(
          overrideEmbeddingTable, DEFAULT_EMBEDDING_SPECS, nlStatVarFilePath);
    }

    String rawContent = readConfigContent(configPath.trim());
    return parseYaml(rawContent, overrideEmbeddingTable, nlStatVarFilePath);
  }

  /** Parses YAML or JSON content into an {@link EmbeddingConfig}. */
  @SuppressWarnings("unchecked")
  public static EmbeddingConfig parseYaml(
      String yamlContent, String overrideEmbeddingTable, String nlStatVarFilePath) {
    if (yamlContent == null || yamlContent.trim().isEmpty()) {
      return new EmbeddingConfig(
          overrideEmbeddingTable, DEFAULT_EMBEDDING_SPECS, nlStatVarFilePath);
    }

    Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
    Object parsed = yaml.load(yamlContent);
    if (parsed == null) {
      return new EmbeddingConfig(
          overrideEmbeddingTable, DEFAULT_EMBEDDING_SPECS, nlStatVarFilePath);
    }

    String resolvedTable = DEFAULT_EMBEDDING_TABLE;
    String resolvedNlFile = nlStatVarFilePath;
    List<Map<String, Object>> rawSpecs = new ArrayList<>();

    if (parsed instanceof List<?> list) {
      for (Object item : list) {
        if (item instanceof Map<?, ?> map) {
          rawSpecs.add((Map<String, Object>) map);
        }
      }
    } else if (parsed instanceof Map<?, ?> rootMap) {
      Map<String, Object> root = (Map<String, Object>) rootMap;
      if (root.containsKey("embedding_table") && root.get("embedding_table") != null) {
        resolvedTable = String.valueOf(root.get("embedding_table")).trim();
      }
      if ((resolvedNlFile == null || resolvedNlFile.trim().isEmpty())
          && root.get("nl_stat_var_file_path") != null) {
        resolvedNlFile = String.valueOf(root.get("nl_stat_var_file_path")).trim();
      }

      if (root.containsKey("calculations") && root.get("calculations") instanceof List<?> calcs) {
        for (Object calcObj : calcs) {
          if (calcObj instanceof Map<?, ?> calcMap) {
            Map<String, Object> calc = (Map<String, Object>) calcMap;
            String type = calc.get("type") != null ? String.valueOf(calc.get("type")) : "";
            if ("EMBEDDING_GENERATION".equalsIgnoreCase(type)) {
              if (calc.containsKey("embedding_table") && calc.get("embedding_table") != null) {
                resolvedTable = String.valueOf(calc.get("embedding_table")).trim();
              }
              Object embGenObj = calc.get("embedding_generation");
              if (embGenObj instanceof Map<?, ?> embGenMap) {
                Object specsObj = embGenMap.get("specs");
                if (specsObj instanceof List<?> specList) {
                  for (Object s : specList) {
                    if (s instanceof Map<?, ?> sMap) {
                      rawSpecs.add((Map<String, Object>) sMap);
                    }
                  }
                }
              } else if (calc.get("specs") instanceof List<?> specList) {
                for (Object s : specList) {
                  if (s instanceof Map<?, ?> sMap) {
                    rawSpecs.add((Map<String, Object>) sMap);
                  }
                }
              }
            }
          }
        }
      } else if (root.containsKey("embedding_generation")
          && root.get("embedding_generation") instanceof Map<?, ?> embGenMap) {
        Object specsObj = embGenMap.get("specs");
        if (specsObj instanceof List<?> specList) {
          for (Object s : specList) {
            if (s instanceof Map<?, ?> sMap) {
              rawSpecs.add((Map<String, Object>) sMap);
            }
          }
        }
      } else if (root.containsKey("specs") && root.get("specs") instanceof List<?> specList) {
        for (Object s : specList) {
          if (s instanceof Map<?, ?> sMap) {
            rawSpecs.add((Map<String, Object>) sMap);
          }
        }
      } else if (root.containsKey("embedding_label")) {
        rawSpecs.add(root);
      }
    }

    if (overrideEmbeddingTable != null
        && !overrideEmbeddingTable.trim().isEmpty()
        && !DEFAULT_EMBEDDING_TABLE.equals(overrideEmbeddingTable.trim())) {
      resolvedTable = overrideEmbeddingTable.trim();
    }

    List<EmbeddingSpec> specs = rawSpecs.stream().map(EmbeddingConfig::parseSpecMap).toList();
    return new EmbeddingConfig(resolvedTable, specs, resolvedNlFile);
  }

  @SuppressWarnings("unchecked")
  private static EmbeddingSpec parseSpecMap(Map<String, Object> map) {
    String embeddingLabel = getString(map, "embedding_label", null);
    String modelName = getString(map, "model_name", EmbeddingSpec.DEFAULT_MODEL_NAME);
    String modelEndpoint = getString(map, "model_endpoint", EmbeddingSpec.DEFAULT_MODEL_ENDPOINT);
    String taskType = getString(map, "task_type", EmbeddingSpec.DEFAULT_TASK_TYPE);
    String filterTypeStr =
        getString(
            map,
            "node_filter_type",
            getString(map, "node_filter", NodeFilterType.NO_FILTER.getConfigValue()));

    Object nodeTypesObj = map.get("node_types");
    Map<String, List<String>> nodeTypesMap = new LinkedHashMap<>();
    if (nodeTypesObj instanceof List<?> list) {
      for (Object item : list) {
        if (item != null) {
          nodeTypesMap.put(String.valueOf(item).trim(), List.of());
        }
      }
    } else if (nodeTypesObj instanceof Map<?, ?> ntMap) {
      for (Map.Entry<?, ?> entry : ntMap.entrySet()) {
        if (entry.getKey() != null) {
          String nodeType = String.valueOf(entry.getKey()).trim();
          List<String> predicates = new ArrayList<>();
          if (entry.getValue() instanceof List<?> predList) {
            for (Object p : predList) {
              if (p != null) {
                predicates.add(String.valueOf(p).trim());
              }
            }
          }
          nodeTypesMap.put(nodeType, predicates);
        }
      }
    } else {
      throw new IllegalArgumentException(
          "EmbeddingSpec 'node_types' must be a list or a map: " + map);
    }

    return new EmbeddingSpec(
        embeddingLabel,
        modelName,
        modelEndpoint,
        taskType,
        nodeTypesMap,
        NodeFilterType.fromString(filterTypeStr));
  }

  private static String getString(Map<String, Object> map, String key, String defaultVal) {
    Object val = map.get(key);
    if (val == null) {
      return defaultVal;
    }
    String str = String.valueOf(val).trim();
    return str.isEmpty() ? defaultVal : str;
  }

  private static String readConfigContent(String configPath) {
    try {
      if (configPath.startsWith("gs://")) {
        ResourceId resourceId = FileSystems.matchNewResource(configPath, false);
        try (InputStream is = Channels.newInputStream(FileSystems.open(resourceId))) {
          return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
      }

      Path localPath = Paths.get(configPath);
      if (Files.exists(localPath)) {
        return Files.readString(localPath, StandardCharsets.UTF_8);
      }

      try (InputStream is =
          EmbeddingConfig.class.getClassLoader().getResourceAsStream(configPath)) {
        if (is != null) {
          return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to read embedding configuration from: " + configPath, e);
    }
    throw new IllegalArgumentException("Embedding configuration file not found: " + configPath);
  }
}
