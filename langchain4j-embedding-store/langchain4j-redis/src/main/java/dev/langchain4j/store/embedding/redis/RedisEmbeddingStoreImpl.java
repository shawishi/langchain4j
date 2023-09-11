package dev.langchain4j.store.embedding.redis;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.search.FTCreateParams;
import redis.clients.jedis.search.IndexDataType;
import redis.clients.jedis.search.schemafields.SchemaField;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Redis Embedding Store Implementation
 */
public class RedisEmbeddingStoreImpl implements EmbeddingStore<TextSegment> {

    private static final Logger log = LoggerFactory.getLogger(RedisEmbeddingStoreImpl.class);

    private final JedisPooled client;
    private final String indexName;
    private final Function<Embedding, Double> relevantScoreFunction;

    public RedisEmbeddingStoreImpl(String url,
                                   String indexName,
                                   SchemaField[] indexSchema,
                                   Function<Embedding, Double> relevantScoreFunction) {
        client = new JedisPooled(url);
        this.indexName = indexName;
        this.relevantScoreFunction = relevantScoreFunction;
        client.ftCreate(indexName, FTCreateParams.createParams()
                        .on(IndexDataType.JSON)
                        .addPrefix("bicycle:"),
                getIndexSchema(indexSchema));
    }

    @Override
    public String add(Embedding embedding) {
        return null;
    }

    @Override
    public void add(String id, Embedding embedding) {

    }

    @Override
    public String add(Embedding embedding, TextSegment textSegment) {
        return null;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        return null;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings, List<TextSegment> embedded) {
        return null;
    }

    @Override
    public List<EmbeddingMatch<TextSegment>> findRelevant(Embedding referenceEmbedding, int maxResults, double minScore) {
        return null;
    }

    private SchemaField[] getIndexSchema(SchemaField[] indexSchema) {
        return Optional.ofNullable(indexSchema).orElse(RedisSchema.DEFAULT_SCHEMA);
    }
}
