package org.datacommons.ingestion.pipeline.embedding;

import java.io.Serializable;
import java.util.List;

/**
 * Serializable client interface for generating vector embeddings from text via Vertex AI Prediction
 * endpoints (e.g., {@code text-embedding-005}).
 */
public interface VertexAiEmbeddingClient extends Serializable {

  /**
   * Predicts vector embeddings for a batch of input text strings using the specified Vertex AI
   * model endpoint and task type.
   *
   * @param texts non-empty list of text strings to embed
   * @param modelEndpoint Vertex AI publisher model endpoint (e.g., {@code text-embedding-005})
   * @param taskType embedding task type (e.g., {@code RETRIEVAL_QUERY}, {@code RETRIEVAL_DOCUMENT})
   * @return list of float/double embedding vectors corresponding 1-to-1 with {@code texts}
   */
  List<List<Double>> predictEmbeddings(List<String> texts, String modelEndpoint, String taskType);
}
