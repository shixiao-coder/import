package org.datacommons.ingestion.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.datacommons.ingestion.pipeline.embedding.DeleteExistingEmbeddingsFn;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingConfig;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingContent;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingNodeCandidate;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec;
import org.datacommons.ingestion.pipeline.embedding.GenerateEmbeddingContentFn;
import org.datacommons.ingestion.pipeline.embedding.GenerateEmbeddingsFn;
import org.datacommons.ingestion.pipeline.embedding.HttpVertexAiEmbeddingClient;
import org.datacommons.ingestion.pipeline.embedding.NlStatVarLoader;
import org.datacommons.ingestion.pipeline.embedding.ReadCandidateNodesFn;
import org.datacommons.ingestion.pipeline.embedding.VertexAiEmbeddingClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EmbeddingIngestionPipelineTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  public static final List<Mutation> CAPTURED_MUTATIONS =
      Collections.synchronizedList(new ArrayList<>());

  @Before
  public void setUp() {
    CAPTURED_MUTATIONS.clear();
  }

  @Test
  public void testParseYaml_masterFormatListNodeTypes() {
    String yaml =
        """
        version: '1.0'
        embedding_table: NodeEmbedding
        nl_stat_var_file_path: gs://datcom-nl-models/nl_stat_vars.csv
        calculations:
          - type: EMBEDDING_GENERATION
            embedding_generation:
              specs:
                - embedding_label: place-name-2026
                  model_name: gemini-embedding-001
                  model_endpoint: text-embedding-005
                  task_type: RETRIEVAL_QUERY
                  node_types:
                    - Country
                    - State
                  node_filter_type: NoFilter
                - embedding_label: statvar-2026
                  model_name: gemini-embedding-001
                  model_endpoint: text-embedding-005
                  task_type: RETRIEVAL_QUERY
                  node_types:
                    - StatisticalVariable
                  node_filter_type: NLStatisticalVariable
        """;

    EmbeddingConfig config = EmbeddingConfig.parseYaml(yaml, null, null);
    assertEquals("NodeEmbedding", config.embeddingTable());
    assertEquals("gs://datcom-nl-models/nl_stat_vars.csv", config.nlStatVarFilePath());
    assertEquals(2, config.specs().size());

    EmbeddingSpec placeSpec = config.specs().get(0);
    assertEquals("place-name-2026", placeSpec.embeddingLabel());
    assertEquals(EmbeddingSpec.NodeFilterType.NO_FILTER, placeSpec.nodeFilterType());
    assertEquals(List.of("Country", "State"), placeSpec.getNodeTypeNames());
    assertFalse(placeSpec.hasPredicates());

    EmbeddingSpec svSpec = config.specs().get(1);
    assertEquals("statvar-2026", svSpec.embeddingLabel());
    assertEquals(EmbeddingSpec.NodeFilterType.NL_STATISTICAL_VARIABLE, svSpec.nodeFilterType());
  }

  @Test
  public void testParseYaml_mapNodeTypesWithPredicatesAndEntityTypesFilter() {
    String yaml =
        """
        embedding_table: NodeEmbedding
        calculations:
          - type: EMBEDDING_GENERATION
            embedding_generation:
              specs:
                - embedding_label: entity-name-2026
                  model_name: gemini-embedding-001
                  model_endpoint: text-embedding-005
                  task_type: RETRIEVAL_QUERY
                  node_types:
                    Country: []
                    Topic:
                      - description
                  node_filter_type: EntityTypes
        """;

    EmbeddingConfig config = EmbeddingConfig.parseYaml(yaml, "CustomNodeEmbedding", null);
    assertEquals("CustomNodeEmbedding", config.embeddingTable());
    assertEquals(1, config.specs().size());

    EmbeddingSpec entitySpec = config.specs().get(0);
    assertEquals("entity-name-2026", entitySpec.embeddingLabel());
    assertEquals(EmbeddingSpec.NodeFilterType.ENTITY_TYPES, entitySpec.nodeFilterType());
    assertTrue(entitySpec.hasPredicates());
    assertEquals(List.of("description"), entitySpec.nodeTypes().get("Topic"));
  }

  @Test
  public void testValidateSqlIdentifier_rejectsInvalidTableName() {
    assertThrows(
        IllegalArgumentException.class,
        () -> EmbeddingConfig.validateSqlIdentifier("NodeEmbedding; DROP TABLE Node", "tableName"));
  }

  @Test
  public void testNlStatVarLoader_parsesSemicolonDcidsAndDeduplicates() {
    String csv =
        """
        dcid,sentence
        Count_Person,Total population
        Count_Person;Count_Household,Population and households
        Count_Person,Total population
        """;

    Map<String, List<String>> parsed =
        NlStatVarLoader.groupByDcid(NlStatVarLoader.parseCsvContent(csv));
    assertEquals(2, parsed.size());
    assertEquals(
        List.of("Total population", "Population and households"), parsed.get("Count_Person"));
    assertEquals(List.of("Population and households"), parsed.get("Count_Household"));
  }

  @Test
  public void testGenerateEmbeddingContentFn_noFilterAndEntityTypesProperties() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "entity-name-2026",
            "gemini-embedding-001",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of("Topic", List.of("description")),
            EmbeddingSpec.NodeFilterType.ENTITY_TYPES);

    Map<String, String> props = new LinkedHashMap<>();
    props.put("description", "Economic indicators. Economy of a region");
    EmbeddingNodeCandidate candidate =
        new EmbeddingNodeCandidate("dc/topic/Economy", "Economy", List.of("Topic"), props);

    List<EmbeddingContent> contents =
        GenerateEmbeddingContentFn.buildEmbeddingContents(candidate, spec, Map.of());
    assertEquals(1, contents.size());

    EmbeddingContent content = contents.get(0);
    assertEquals("dc/topic/Economy", content.subjectId());
    assertEquals("entity-name-2026", content.embeddingLabel());
    assertEquals(
        "{\"subject_id\":\"dc/topic/Economy\",\"name\":\"Economy\",\"properties\":{\"description\":\"Economic indicators. Economy of a region\"}}",
        content.embeddingContentJson());
    assertEquals(content.embeddingContentJson(), content.textToEmbed());
    assertEquals(
        GenerateEmbeddingContentFn.computeFarmFingerprintKey(content.embeddingContentJson()),
        content.embeddingContentKey());
  }

  @Test
  public void testGenerateEmbeddingContentFn_nlStatisticalVariable() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "statvar-2026",
            "gemini-embedding-001",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of("StatisticalVariable", List.of()),
            EmbeddingSpec.NodeFilterType.NL_STATISTICAL_VARIABLE);

    EmbeddingNodeCandidate candidate =
        new EmbeddingNodeCandidate(
            "Count_Person", "Count of Persons", List.of("StatisticalVariable"), Map.of());

    Map<String, List<String>> nlMap =
        Map.of("Count_Person", List.of("Count of Persons", "Total population", "Number of people"));

    List<EmbeddingContent> contents =
        GenerateEmbeddingContentFn.buildEmbeddingContents(candidate, spec, nlMap);
    assertEquals(3, contents.size());
    assertEquals("Count of Persons", contents.get(0).textToEmbed());
    assertEquals("Total population", contents.get(1).textToEmbed());
    assertEquals("Number of people", contents.get(2).textToEmbed());
  }

  @Test
  public void testSqlBuilders_parameterizedStatements() {
    Timestamp lockTs = Timestamp.parseTimestamp("2026-01-01T00:00:00Z");
    Statement nodeStmt =
        ReadCandidateNodesFn.buildSimpleNodesStatement(
            "Node", List.of("Country", "State"), null, lockTs);
    assertTrue(nodeStmt.getSql().contains("FROM Node"));
    assertTrue(nodeStmt.getSql().contains("last_update_timestamp > @timestamp"));

    Statement predStmt =
        ReadCandidateNodesFn.buildNodesWithPredicatesStatement(
            "Node", "Edge", "Topic", List.of("description"), null, lockTs);
    assertTrue(predStmt.getSql().contains("e.predicate IN UNNEST(@predicates)"));

    String gql =
        ReadCandidateNodesFn.buildGqlQuery(Map.of("Topic", List.of("description")), "TRUE");
    assertTrue(gql.contains("Graph DCGraph"));
    assertTrue(gql.contains("e.predicate IN UNNEST(['description'])"));

    String deleteDml = DeleteExistingEmbeddingsFn.buildDeleteDml("NodeEmbedding");
    assertEquals(
        "DELETE FROM NodeEmbedding WHERE embedding_label = @embedding_label AND subject_id IN UNNEST(@subject_ids)",
        deleteDml);
  }

  @Test
  public void testHttpVertexAiEmbeddingClient_requestAndResponseJson() {
    String reqJson =
        HttpVertexAiEmbeddingClient.buildRequestJson(
            List.of("California", "Texas"), "RETRIEVAL_QUERY");
    assertTrue(reqJson.contains("\"content\":\"California\""));
    assertTrue(reqJson.contains("\"task_type\":\"RETRIEVAL_QUERY\""));

    String respJson =
        """
        {
          "predictions": [
            {"embeddings": {"values": [0.1, 0.2, 0.3]}},
            {"embeddings": {"values": [0.4, 0.5, 0.6]}}
          ]
        }
        """;
    List<List<Double>> vectors = HttpVertexAiEmbeddingClient.parseResponseJson(respJson, 2);
    assertEquals(2, vectors.size());
    assertEquals(List.of(0.1, 0.2, 0.3), vectors.get(0));
    assertEquals(List.of(0.4, 0.5, 0.6), vectors.get(1));
  }

  @Test
  public void testEndToEndBeamTransforms_generatesSpannerNodeEmbeddingMutations() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "place-name-2026",
            "gemini-embedding-001",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of("State", List.of()),
            EmbeddingSpec.NodeFilterType.NO_FILTER);

    EmbeddingNodeCandidate candidate1 =
        new EmbeddingNodeCandidate("geoId/06", "California", List.of("State"), Map.of());
    EmbeddingNodeCandidate candidate2 =
        new EmbeddingNodeCandidate("geoId/48", "Texas", List.of("State"), Map.of());

    VertexAiEmbeddingClient fakeVertexClient =
        (texts, modelEndpoint, taskType) -> {
          List<List<Double>> out = new ArrayList<>(texts.size());
          for (int i = 0; i < texts.size(); i++) {
            out.add(List.of(0.25, 0.75));
          }
          return out;
        };

    pipeline
        .apply("SeedCandidates", Create.of(candidate1, candidate2))
        .apply("GenerateContent", ParDo.of(new GenerateEmbeddingContentFn(spec, Map.of())))
        .apply(
            "GenerateEmbeddings",
            ParDo.of(new GenerateEmbeddingsFn(fakeVertexClient, "NodeEmbedding", 10)))
        .apply(
            "CaptureMutations",
            ParDo.of(
                new DoFn<Mutation, Void>() {
                  @ProcessElement
                  public void processElement(@Element Mutation m) {
                    CAPTURED_MUTATIONS.add(m);
                  }
                }));

    pipeline.run().waitUntilFinish();

    assertEquals(2, CAPTURED_MUTATIONS.size());
    for (Mutation m : CAPTURED_MUTATIONS) {
      assertEquals("NodeEmbedding", m.getTable());
      assertEquals(Mutation.Op.INSERT_OR_UPDATE, m.getOperation());
      Map<String, Value> valueMap = m.asMap();
      assertEquals("place-name-2026", valueMap.get("embedding_label").getString());
      assertEquals(List.of(0.25, 0.75), valueMap.get("embeddings").getFloat64Array());
      assertEquals(List.of("State"), valueMap.get("node_types").getStringArray());
      assertNotNull(valueMap.get("embedding_content_key").getString());
      assertNotNull(valueMap.get("embedding_content").getJson());
    }
  }
}
