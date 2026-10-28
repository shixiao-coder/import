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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queries Cloud Spanner to filter and extract candidate nodes for a given {@link EmbeddingSpec},
 * mirroring the node filtering and GQL/SQL property extraction logic in aggregation-helper's {@code
 * EmbeddingGenerator}.
 */
public class ReadCandidateNodesFn extends DoFn<EmbeddingSpec, EmbeddingNodeCandidate> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadCandidateNodesFn.class);

  private static final String LOCK_TIMESTAMP_SQL =
      "SELECT MAX(AcquiredTimestamp) FROM IngestionLock";

  /**
   * Canonical list of place types always included for {@link NodeFilterType#ENTITY_TYPES}, matching
   * {@code _PLACE_TYPE_ORDER} in aggregation-helper.
   */
  public static final List<String> PLACE_TYPE_ORDER =
      List.of(
          "Place",
          "OceanicBasin",
          "Continent",
          "Country",
          "CensusRegion",
          "CensusDivision",
          "State",
          "AdministrativeArea1",
          "County",
          "AdministrativeArea2",
          "CensusCountyDivision",
          "EurostatNUTS1",
          "EurostatNUTS2",
          "CongressionalDistrict",
          "UDISEDistrict",
          "CensusCoreBasedStatisticalArea",
          "EurostatNUTS3",
          "SuperfundSite",
          "Glacier",
          "AdministrativeArea3",
          "AdministrativeArea4",
          "PublicUtility",
          "CollegeOrUniversity",
          "EpaParentCompany",
          "UDISEBlock",
          "AdministrativeArea5",
          "EpaReportingFacility",
          "SchoolDistrict",
          "CensusZipCodeTabulationArea",
          "PrivateSchool",
          "CensusTract",
          "City",
          "AirQualitySite",
          "PublicSchool",
          "Neighborhood",
          "CensusBlockGroup",
          "AdministrativeArea",
          "Village");

  private static final String OBSERVATION_ENTITY_TYPES_INDEXED_SQL =
      """
      WITH all_type_table AS (
          SELECT DISTINCT
              subject_id AS t
          FROM
              Edge@{FORCE_INDEX=InEdge}
          WHERE
              object_id = 'Class'
              AND predicate = 'typeOf'
      )
      SELECT
          c.t
      FROM
          all_type_table c
      INNER JOIN@{JOIN_METHOD=APPLY_JOIN}
          UNNEST(
              ARRAY(
                  SELECT
                      1
                  FROM
                      Edge@{FORCE_INDEX=InEdge} e
                  INNER JOIN@{JOIN_METHOD=APPLY_JOIN}
                      TimeSeries@{FORCE_INDEX=TimeSeriesByEntity1} ts
                      ON e.subject_id = ts.entity1
                  WHERE
                      e.object_id = c.t
                      AND e.predicate = 'typeOf'
                  LIMIT 1
              )
          )
      ORDER BY
          c.t
      """;

  private static final String OBSERVATION_ENTITY_TYPES_FALLBACK_SQL =
      """
      SELECT DISTINCT e.object_id AS t
      FROM Edge e
      INNER JOIN TimeSeries ts ON e.subject_id = ts.entity1
      WHERE e.predicate = 'typeOf'
        AND e.object_id IN (
          SELECT subject_id FROM Edge WHERE object_id = 'Class' AND predicate = 'typeOf'
        )
      ORDER BY t
      """;

  private final SpannerClient spannerClient;
  private final boolean ignoreLockTimestamp;
  private final List<String> nlStatVarDcids;
  private transient Spanner spanner;
  private transient DatabaseClient dbClient;

  public ReadCandidateNodesFn(
      SpannerClient spannerClient, boolean ignoreLockTimestamp, List<String> nlStatVarDcids) {
    this.spannerClient = spannerClient;
    this.ignoreLockTimestamp = ignoreLockTimestamp;
    this.nlStatVarDcids = nlStatVarDcids == null ? List.of() : List.copyOf(nlStatVarDcids);
  }

  @Setup
  public void setup() {
    this.spanner = spannerClient.createSpanner();
    this.dbClient = spannerClient.getDatabaseClient(spanner);
  }

  @Teardown
  public void teardown() {
    if (spanner != null) {
      spanner.close();
    }
  }

  @ProcessElement
  public void processElement(
      @Element EmbeddingSpec spec, OutputReceiver<EmbeddingNodeCandidate> receiver) {
    Counter candidateCounter =
        Metrics.counter(
            ReadCandidateNodesFn.class, "embedding_candidate_nodes:" + spec.embeddingLabel());

    try (ReadOnlyTransaction snapshot = dbClient.readOnlyTransaction()) {
      com.google.cloud.Timestamp latestLockTimestamp =
          ignoreLockTimestamp ? null : fetchLatestLockTimestamp(snapshot);
      List<EmbeddingNodeCandidate> candidates =
          queryCandidatesForSpec(snapshot, spec, latestLockTimestamp);

      LOGGER.info(
          "Found {} candidate node(s) for embedding_label '{}' (filter: {}, lockTimestamp: {}).",
          candidates.size(),
          spec.embeddingLabel(),
          spec.nodeFilterType(),
          latestLockTimestamp);

      for (EmbeddingNodeCandidate candidate : candidates) {
        candidateCounter.inc();
        receiver.output(candidate);
      }
    }
  }

  com.google.cloud.Timestamp fetchLatestLockTimestamp(ReadOnlyTransaction snapshot) {
    try (ResultSet rs = snapshot.executeQuery(Statement.of(LOCK_TIMESTAMP_SQL))) {
      if (rs.next() && !rs.isNull(0)) {
        return rs.getTimestamp(0);
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Could not read IngestionLock timestamp (defaulting to full scan): {}", e.getMessage());
    }
    return null;
  }

  List<String> fetchObservationEntityTypes(ReadOnlyTransaction snapshot) {
    Set<String> types = new TreeSet<>(PLACE_TYPE_ORDER);
    try (ResultSet rs = snapshot.executeQuery(Statement.of(OBSERVATION_ENTITY_TYPES_INDEXED_SQL))) {
      while (rs.next()) {
        if (!rs.isNull(0)) {
          types.add(rs.getString(0));
        }
      }
      return List.copyOf(types);
    } catch (Exception indexedEx) {
      LOGGER.info(
          "Indexed entity-type query not supported in current environment ({}), using standard SQL join.",
          indexedEx.getMessage());
    }

    try (ResultSet rs =
        snapshot.executeQuery(Statement.of(OBSERVATION_ENTITY_TYPES_FALLBACK_SQL))) {
      while (rs.next()) {
        if (!rs.isNull(0)) {
          types.add(rs.getString(0));
        }
      }
    }
    return List.copyOf(types);
  }

  List<EmbeddingNodeCandidate> queryCandidatesForSpec(
      ReadOnlyTransaction snapshot,
      EmbeddingSpec spec,
      com.google.cloud.Timestamp latestLockTimestamp) {
    List<String> allowedSubjectIds = null;
    if (spec.nodeFilterType() == NodeFilterType.NL_STATISTICAL_VARIABLE) {
      if (nlStatVarDcids.isEmpty()) {
        LOGGER.info(
            "NL StatVar DCID list is empty for spec '{}'; skipping.", spec.embeddingLabel());
        return List.of();
      }
      allowedSubjectIds = nlStatVarDcids;
    } else if (spec.nodeFilterType() == NodeFilterType.ENTITY_TYPES) {
      allowedSubjectIds = fetchObservationEntityTypes(snapshot);
      if (allowedSubjectIds.isEmpty()) {
        return List.of();
      }
    }

    if (!spec.hasPredicates()) {
      Statement stmt =
          buildSimpleNodesStatement(
              spannerClient.getNodeTableName(),
              spec.getNodeTypeNames(),
              allowedSubjectIds,
              latestLockTimestamp);
      List<EmbeddingNodeCandidate> results = new ArrayList<>();
      try (ResultSet rs = snapshot.executeQuery(stmt)) {
        while (rs.next()) {
          String subjectId = rs.getString("subject_id");
          String name = rs.isNull("name") ? "" : rs.getString("name");
          List<String> types = rs.isNull("node_types") ? List.of() : rs.getStringList("node_types");
          if (!name.isEmpty()) {
            results.add(new EmbeddingNodeCandidate(subjectId, name, types));
          }
        }
      }
      return results;
    }

    // When predicates are configured on one or more node types, query per node type and merge
    Map<String, EmbeddingNodeCandidate> mergedBySubjectId = new LinkedHashMap<>();
    for (Map.Entry<String, List<String>> entry : spec.nodeTypes().entrySet()) {
      String nodeType = entry.getKey();
      List<String> predicates = entry.getValue();
      if (predicates == null || predicates.isEmpty()) {
        Statement stmt =
            buildSimpleNodesStatement(
                spannerClient.getNodeTableName(),
                List.of(nodeType),
                allowedSubjectIds,
                latestLockTimestamp);
        try (ResultSet rs = snapshot.executeQuery(stmt)) {
          while (rs.next()) {
            String subjectId = rs.getString("subject_id");
            String name = rs.isNull("name") ? "" : rs.getString("name");
            List<String> types =
                rs.isNull("node_types") ? List.of() : rs.getStringList("node_types");
            if (!name.isEmpty()) {
              mergedBySubjectId.putIfAbsent(
                  subjectId, new EmbeddingNodeCandidate(subjectId, name, types));
            }
          }
        }
      } else {
        List<EmbeddingNodeCandidate> withProps =
            queryNodesWithPredicates(
                snapshot, nodeType, predicates, allowedSubjectIds, latestLockTimestamp);
        for (EmbeddingNodeCandidate cand : withProps) {
          EmbeddingNodeCandidate existing = mergedBySubjectId.get(cand.subjectId());
          if (existing == null) {
            mergedBySubjectId.put(cand.subjectId(), cand);
          } else {
            Map<String, String> combinedProps = new LinkedHashMap<>(existing.properties());
            combinedProps.putAll(cand.properties());
            mergedBySubjectId.put(
                cand.subjectId(),
                new EmbeddingNodeCandidate(
                    cand.subjectId(), cand.name(), cand.nodeTypes(), combinedProps));
          }
        }
      }
    }
    return List.copyOf(mergedBySubjectId.values());
  }

  private List<EmbeddingNodeCandidate> queryNodesWithPredicates(
      ReadOnlyTransaction snapshot,
      String nodeType,
      List<String> predicates,
      List<String> allowedSubjectIds,
      com.google.cloud.Timestamp latestLockTimestamp) {
    Statement stmt =
        buildNodesWithPredicatesStatement(
            spannerClient.getNodeTableName(),
            spannerClient.getEdgeTableName(),
            nodeType,
            predicates,
            allowedSubjectIds,
            latestLockTimestamp);

    Map<String, String> namesBySubject = new LinkedHashMap<>();
    Map<String, List<String>> typesBySubject = new LinkedHashMap<>();
    Map<String, Map<String, List<String>>> propValuesBySubject = new LinkedHashMap<>();
    Set<String> updatedSubjects = new LinkedHashSet<>();

    try (ResultSet rs = snapshot.executeQuery(stmt)) {
      while (rs.next()) {
        String subjectId = rs.getString("subject_id");
        String name = rs.isNull("name") ? "" : rs.getString("name");
        if (name.isEmpty()) {
          continue;
        }
        List<String> types = rs.isNull("node_types") ? List.of() : rs.getStringList("node_types");
        boolean nodeUpdated = rs.getBoolean("node_updated");
        boolean propUpdated = rs.getBoolean("prop_updated");
        if (nodeUpdated || propUpdated) {
          updatedSubjects.add(subjectId);
        }

        namesBySubject.putIfAbsent(subjectId, name);
        typesBySubject.putIfAbsent(subjectId, types);

        if (!rs.isNull("predicate") && !rs.isNull("object_value")) {
          String pred = rs.getString("predicate");
          String objVal = rs.getString("object_value");
          if (!objVal.isEmpty()) {
            propValuesBySubject
                .computeIfAbsent(subjectId, k -> new LinkedHashMap<>())
                .computeIfAbsent(pred, k -> new ArrayList<>())
                .add(objVal);
          }
        }
      }
    }

    List<EmbeddingNodeCandidate> candidates = new ArrayList<>();
    for (String subjectId : updatedSubjects) {
      String name = namesBySubject.get(subjectId);
      List<String> types = typesBySubject.getOrDefault(subjectId, List.of());
      Map<String, List<String>> rawProps =
          propValuesBySubject.getOrDefault(subjectId, Collections.emptyMap());
      Map<String, String> aggregatedProps = new LinkedHashMap<>();
      for (Map.Entry<String, List<String>> entry : rawProps.entrySet()) {
        String joined = String.join(". ", entry.getValue());
        if (!joined.isEmpty()) {
          aggregatedProps.put(entry.getKey(), joined);
        }
      }
      candidates.add(new EmbeddingNodeCandidate(subjectId, name, types, aggregatedProps));
    }
    return candidates;
  }

  /**
   * Builds a parameterized Spanner SQL statement querying the {@code Node} table for candidate
   * nodes when no edge predicate traversal is required.
   */
  public static Statement buildSimpleNodesStatement(
      String nodeTableName,
      List<String> nodeTypes,
      List<String> allowedSubjectIds,
      com.google.cloud.Timestamp latestLockTimestamp) {
    String safeNodeTable = EmbeddingConfig.validateSqlIdentifier(nodeTableName, "nodeTableName");
    String timestampCondition =
        latestLockTimestamp != null ? "last_update_timestamp > @timestamp" : "TRUE";
    String filterCondition =
        allowedSubjectIds != null ? "subject_id IN UNNEST(@allowed_subject_ids)" : "TRUE";

    String sql =
        String.format(
            """
            SELECT subject_id, name, types AS node_types
            FROM %s
            WHERE name IS NOT NULL
              AND name <> ''
              AND %s
              AND %s
              AND EXISTS (
                SELECT 1 FROM UNNEST(types) AS t WHERE t IN UNNEST(@node_types)
              )
            ORDER BY subject_id
            """,
            safeNodeTable, timestampCondition, filterCondition);

    Statement.Builder builder =
        Statement.newBuilder(sql).bind("node_types").to(Value.stringArray(nodeTypes));
    if (latestLockTimestamp != null) {
      builder.bind("timestamp").to(latestLockTimestamp);
    }
    if (allowedSubjectIds != null) {
      builder.bind("allowed_subject_ids").to(Value.stringArray(allowedSubjectIds));
    }
    return builder.build();
  }

  /**
   * Builds a parameterized Spanner SQL statement joining {@code Node}, {@code Edge}, and object
   * {@code Node}s for configured predicates (e.g. {@code description}).
   */
  public static Statement buildNodesWithPredicatesStatement(
      String nodeTableName,
      String edgeTableName,
      String nodeType,
      List<String> predicates,
      List<String> allowedSubjectIds,
      com.google.cloud.Timestamp latestLockTimestamp) {
    String safeNodeTable = EmbeddingConfig.validateSqlIdentifier(nodeTableName, "nodeTableName");
    String safeEdgeTable = EmbeddingConfig.validateSqlIdentifier(edgeTableName, "edgeTableName");
    String filterCondition =
        allowedSubjectIds != null ? "n.subject_id IN UNNEST(@allowed_subject_ids)" : "TRUE";
    String nodeUpdatedExpr =
        latestLockTimestamp != null ? "n.last_update_timestamp > @timestamp" : "TRUE";
    String propUpdatedExpr =
        latestLockTimestamp != null
            ? "IFNULL(o.last_update_timestamp > @timestamp, FALSE)"
            : "TRUE";

    String sql =
        String.format(
            """
            SELECT
              n.subject_id,
              n.name,
              n.types AS node_types,
              e.predicate AS predicate,
              o.value AS object_value,
              %s AS node_updated,
              %s AS prop_updated
            FROM %s n
            LEFT JOIN (
              %s e
              INNER JOIN %s o
                ON e.object_id = o.subject_id
               AND o.value IS NOT NULL
               AND o.value <> ''
            )
              ON n.subject_id = e.subject_id
             AND e.predicate IN UNNEST(@predicates)
            WHERE n.name IS NOT NULL
              AND n.name <> ''
              AND @node_type IN UNNEST(n.types)
              AND %s
            ORDER BY n.subject_id, e.predicate, o.subject_id
            """,
            nodeUpdatedExpr,
            propUpdatedExpr,
            safeNodeTable,
            safeEdgeTable,
            safeNodeTable,
            filterCondition);

    Statement.Builder builder =
        Statement.newBuilder(sql)
            .bind("node_type")
            .to(nodeType)
            .bind("predicates")
            .to(Value.stringArray(predicates));
    if (latestLockTimestamp != null) {
      builder.bind("timestamp").to(latestLockTimestamp);
    }
    if (allowedSubjectIds != null) {
      builder.bind("allowed_subject_ids").to(Value.stringArray(allowedSubjectIds));
    }
    return builder.build();
  }

  /**
   * Generates the Spanner GQL {@code Graph DCGraph} query string matching aggregation-helper's
   * {@code _generate_spanner_query}.
   */
  public static String buildGqlQuery(Map<String, List<String>> nodeTypes, String filterCondition) {
    String effectiveFilter =
        (filterCondition == null || filterCondition.trim().isEmpty())
            ? "TRUE"
            : filterCondition.trim();
    List<String> clauses = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : nodeTypes.entrySet()) {
      String safeNodeType = entry.getKey().replace("'", "\\'").replace("\"", "\\\"");
      String predicatesLiteral =
          entry.getValue().stream()
              .map(p -> "'" + p.replace("'", "\\'") + "'")
              .collect(Collectors.joining(", ", "[", "]"));
      String clause =
          String.format(
              """
              MATCH
              (n:Node WHERE "%s" IN UNNEST(n.types) AND %s AND IF(@timestamp IS NOT NULL, n.last_update_timestamp > @timestamp, TRUE))
              OPTIONAL MATCH
              (n)-[e: Edge
                  WHERE e.predicate IN UNNEST(%s)]->
              (o:Node
                  WHERE o.value IS NOT NULL
                  AND o.value <> "")
              WITH
                  n,
                  e.predicate AS pred,
                  STRING_AGG(o.value, ". ") AS values
              GROUP BY n, pred
              RETURN
              n.subject_id AS subject_id,
              n.types AS node_types,
              CASE
                  WHEN COUNT(pred) > 0 THEN
                  JSON_OBJECT(
                      "subject_id", n.subject_id,
                      "name", n.name,
                      "properties", JSON_OBJECT(
                      ARRAY_AGG(pred IGNORE NULLS),
                      ARRAY_AGG(TO_JSON(values) IGNORE NULLS)
                      )
                  )
                  ELSE
                  JSON_OBJECT(
                      "subject_id", n.subject_id,
                      "name", n.name
                  )
              END AS embedding_content
              GROUP BY n""",
              safeNodeType, effectiveFilter, predicatesLiteral);
      clauses.add(clause);
    }
    return "Graph DCGraph\n" + String.join("\nUNION ALL\n", clauses);
  }
}
