package org.datacommons.ingestion.pipeline.embedding;

import com.google.cloud.spanner.Mutation;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.datacommons.ingestion.spanner.model.NodeEmbeddingRecord;

/**
 * Batched Beam {@link DoFn} that buffers {@link EmbeddingContent} elements within a bundle, invokes
 * {@link VertexAiEmbeddingClient#predictEmbeddings(List, String, String)} in batches, and emits
 * Spanner {@link Mutation}s for the {@code NodeEmbedding} table.
 */
public class GenerateEmbeddingsFn extends DoFn<EmbeddingContent, Mutation> {
  private static final int DEFAULT_BATCH_SIZE = 250;

  private final VertexAiEmbeddingClient embeddingClient;
  private final String embeddingTableName;
  private final int batchSize;
  private final Counter embeddingsGeneratedCounter =
      Metrics.counter(GenerateEmbeddingsFn.class, "node_embeddings_generated");
  private final Counter vertexBatchCallsCounter =
      Metrics.counter(GenerateEmbeddingsFn.class, "vertex_ai_batch_calls");

  private transient List<EmbeddingContent> buffer;

  public GenerateEmbeddingsFn(
      VertexAiEmbeddingClient embeddingClient, String embeddingTableName, int batchSize) {
    this.embeddingClient =
        Objects.requireNonNull(embeddingClient, "embeddingClient cannot be null");
    this.embeddingTableName =
        EmbeddingConfig.validateSqlIdentifier(
            embeddingTableName != null && !embeddingTableName.isBlank()
                ? embeddingTableName
                : EmbeddingConfig.DEFAULT_EMBEDDING_TABLE,
            "embeddingTableName");
    this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
  }

  @StartBundle
  public void startBundle() {
    this.buffer = new ArrayList<>(batchSize);
  }

  @ProcessElement
  public void processElement(@Element EmbeddingContent content, OutputReceiver<Mutation> receiver) {
    if (content == null || content.textToEmbed() == null || content.textToEmbed().isBlank()) {
      return;
    }
    if (!buffer.isEmpty()) {
      EmbeddingContent first = buffer.get(0);
      if (!Objects.equals(first.modelEndpoint(), content.modelEndpoint())
          || !Objects.equals(first.taskType(), content.taskType())) {
        flushBuffer(receiver);
      }
    }
    buffer.add(content);
    if (buffer.size() >= batchSize) {
      flushBuffer(receiver);
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    if (buffer != null && !buffer.isEmpty()) {
      List<Mutation> mutations = generateMutationsForBatch(buffer);
      buffer.clear();
      org.joda.time.Instant timestamp = org.joda.time.Instant.now();
      for (Mutation mutation : mutations) {
        context.output(
            mutation, timestamp, org.apache.beam.sdk.transforms.windowing.GlobalWindow.INSTANCE);
      }
    }
  }

  private void flushBuffer(OutputReceiver<Mutation> receiver) {
    if (buffer.isEmpty()) {
      return;
    }
    List<Mutation> mutations = generateMutationsForBatch(buffer);
    buffer.clear();
    for (Mutation mutation : mutations) {
      receiver.output(mutation);
    }
  }

  private List<Mutation> generateMutationsForBatch(List<EmbeddingContent> batch) {
    String modelEndpoint = batch.get(0).modelEndpoint();
    String taskType = batch.get(0).taskType();
    List<String> texts = new ArrayList<>(batch.size());
    for (EmbeddingContent item : batch) {
      texts.add(item.textToEmbed());
    }

    vertexBatchCallsCounter.inc();
    List<List<Double>> vectors = embeddingClient.predictEmbeddings(texts, modelEndpoint, taskType);
    if (vectors.size() != batch.size()) {
      throw new IllegalStateException(
          String.format(
              "Vertex AI returned %d vectors for a batch of %d EmbeddingContent items",
              vectors.size(), batch.size()));
    }

    List<Mutation> mutations = new ArrayList<>(batch.size());
    for (int i = 0; i < batch.size(); i++) {
      NodeEmbeddingRecord record = batch.get(i).toNodeEmbeddingRecord(vectors.get(i));
      mutations.add(record.toMutation(embeddingTableName));
    }
    embeddingsGeneratedCounter.inc(mutations.size());
    return mutations;
  }
}
