package org.datacommons.ingestion.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.datacommons.ingestion.pipeline.embedding.SpannerEmbeddingPipeline;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow pipeline entrypoint for generating and ingesting Spanner {@code NodeEmbedding} records
 * based on a global embedding configuration and Vertex AI embedding models.
 */
public class EmbeddingIngestionPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingIngestionPipeline.class);

  public static SpannerClient createSpannerClient(EmbeddingPipelineOptions options) {
    return SpannerClient.builder()
        .gcpProjectId(options.getProjectId())
        .spannerInstanceId(options.getSpannerInstanceId())
        .spannerDatabaseId(options.getSpannerDatabaseId())
        .numShards(options.getNumShards())
        .emulatorHost(options.getEmulatorHost())
        .build();
  }

  public static void main(String[] args) {
    EmbeddingPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(EmbeddingPipelineOptions.class);

    SpannerClient spannerClient = createSpannerClient(options);
    LOGGER.info("Using Spanner client for embedding ingestion: {}", spannerClient);

    Pipeline pipeline = SpannerEmbeddingPipeline.buildPipeline(options, spannerClient);
    PipelineResult result = pipeline.run();
    LOGGER.info("EmbeddingIngestionPipeline submitted with state: {}", result.getState());
  }
}
