package org.datacommons.ingestion.pipeline.embedding;

import com.google.cloud.spanner.Mutation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.pipeline.EmbeddingPipelineOptions;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the Apache Beam DAG for reading modified Spanner graph nodes, deleting stale embeddings,
 * generating embedding content, calling Vertex AI for vector embeddings, and writing {@code
 * NodeEmbedding} mutations back to Spanner.
 */
public class SpannerEmbeddingPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerEmbeddingPipeline.class);

  public static Pipeline buildPipeline(
      EmbeddingPipelineOptions options, SpannerClient spannerClient) {
    EmbeddingConfig config =
        EmbeddingConfig.load(
            options.getEmbeddingConfigPath(),
            options.getEmbeddingTableName(),
            options.getNlStatVarFilePath());
    String vertexProjectId =
        options.getVertexAiProjectId() != null && !options.getVertexAiProjectId().isBlank()
            ? options.getVertexAiProjectId()
            : options.getProjectId();
    VertexAiEmbeddingClient vertexClient =
        new HttpVertexAiEmbeddingClient(vertexProjectId, options.getVertexAiLocation());
    return buildPipeline(options, spannerClient, config, vertexClient);
  }

  public static Pipeline buildPipeline(
      EmbeddingPipelineOptions options,
      SpannerClient spannerClient,
      EmbeddingConfig config,
      VertexAiEmbeddingClient vertexClient) {
    Pipeline pipeline = Pipeline.create(options);
    attachTransforms(pipeline, options, spannerClient, config, vertexClient);
    return pipeline;
  }

  public static void attachTransforms(
      Pipeline pipeline,
      EmbeddingPipelineOptions options,
      SpannerClient spannerClient,
      EmbeddingConfig config,
      VertexAiEmbeddingClient vertexClient) {
    Objects.requireNonNull(pipeline, "pipeline cannot be null");
    Objects.requireNonNull(options, "options cannot be null");
    Objects.requireNonNull(spannerClient, "spannerClient cannot be null");
    Objects.requireNonNull(config, "config cannot be null");
    Objects.requireNonNull(vertexClient, "vertexClient cannot be null");

    boolean needsNlStatVars =
        config.specs().stream()
            .anyMatch(
                s -> s.nodeFilterType() == EmbeddingSpec.NodeFilterType.NL_STATISTICAL_VARIABLE);
    Map<String, List<String>> nlStatVarSentences =
        needsNlStatVars
            ? NlStatVarLoader.groupByDcid(NlStatVarLoader.loadRecords(config.nlStatVarFilePath()))
            : Map.of();
    List<String> nlStatVarDcids = new ArrayList<>(nlStatVarSentences.keySet());

    String embeddingTable = config.embeddingTable();
    int batchSize = options.getEmbeddingBatchSize() > 0 ? options.getEmbeddingBatchSize() : 250;
    boolean skipDelete = options.getSkipDelete();
    boolean ignoreLockTimestamp = options.getIgnoreLockTimestamp();

    LOGGER.info(
        "Configuring EmbeddingIngestionPipeline with {} spec(s) targeting table '{}'",
        config.specs().size(),
        embeddingTable);

    for (EmbeddingSpec spec : config.specs()) {
      String label = spec.embeddingLabel();

      // 1. Read & filter candidate nodes from Spanner based on the EmbeddingSpec
      PCollection<EmbeddingNodeCandidate> candidates =
          pipeline
              .apply("SeedSpec-" + label, Create.of(spec))
              .apply(
                  "ReadCandidateNodes-" + label,
                  ParDo.of(
                      new ReadCandidateNodesFn(spannerClient, ignoreLockTimestamp, nlStatVarDcids)))
              .apply("ReshuffleCandidates-" + label, Reshuffle.viaRandomKey());

      // 2. Generate EmbeddingContent items for each candidate node
      PCollection<EmbeddingContent> contents =
          candidates
              .apply(
                  "GenerateEmbeddingContent-" + label,
                  ParDo.of(new GenerateEmbeddingContentFn(spec, nlStatVarSentences)))
              .apply("ReshuffleContents-" + label, Reshuffle.viaRandomKey());

      // 3. Call Vertex AI in batches to generate vector embeddings & NodeEmbedding mutations
      PCollection<Mutation> mutations =
          contents.apply(
              "GenerateEmbeddings-" + label,
              ParDo.of(new GenerateEmbeddingsFn(vertexClient, embeddingTable, batchSize)));

      // 4. Delete existing embeddings for modified nodes before writing new mutations
      PCollection<Mutation> mutationsReadyToWrite = mutations;
      if (!skipDelete) {
        PCollection<Void> deleteDoneSignal =
            candidates
                .apply(
                    "ExtractSubjectIds-" + label,
                    MapElements.into(TypeDescriptors.strings())
                        .via(EmbeddingNodeCandidate::subjectId))
                .apply("DistinctSubjectIds-" + label, Distinct.create())
                .apply("KeySubjectIds-" + label, WithKeys.of((Void) null))
                .apply("GroupSubjectIds-" + label, GroupByKey.create())
                .apply("SubjectIdBatches-" + label, Values.create())
                .apply(
                    "DeleteExistingEmbeddings-" + label,
                    ParDo.of(new DeleteExistingEmbeddingsFn(spannerClient, embeddingTable, label)));

        mutationsReadyToWrite =
            mutations.apply("WaitForDelete-" + label, Wait.on(deleteDoneSignal));
      }

      // 5. Write NodeEmbedding mutations to Spanner
      spannerClient.writeMutations(pipeline, "WriteNodeEmbeddings-" + label, mutationsReadyToWrite);
    }
  }
}
