package dev.langchain4j.store.embedding.redis;

import com.google.gson.Gson;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.internal.ValidationUtils;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.search.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.stream.Collectors;

import static dev.langchain4j.internal.Utils.isCollectionEmpty;
import static dev.langchain4j.internal.Utils.randomUUID;
import static redis.clients.jedis.search.RediSearchUtil.ToByteArray;

/**
 * Redis Embedding Store Implementation
 */
public class RedisEmbeddingStoreImpl implements EmbeddingStore<TextSegment> {

    private static final Logger log = LoggerFactory.getLogger(RedisEmbeddingStoreImpl.class);
    private static final Gson GSON = new Gson();

    private final JedisPooled client;
    private final RedisSchema schema;

    public RedisEmbeddingStoreImpl(String url,
                                   RedisSchema schema) {
        client = new JedisPooled(url);
        this.schema = Optional.ofNullable(schema).orElse(RedisSchema.DEFAULT_SCHEMA);

        // create index
        if (!isIndexExist(this.schema.getIndexName())) {
            IndexDefinition indexDefinition = new IndexDefinition(IndexDefinition.Type.HASH);
            indexDefinition.setPrefixes(this.schema.getPrefix());
            String res = client.ftCreate(this.schema.getIndexName(), IndexOptions.defaultOptions()
                    .setDefinition(indexDefinition), this.schema.toSchema());
            if (!"OK".equals(res)) {
                throw new JedisDataException("create index error, msg=" + res);
            }
        }
    }

    @Override
    public String add(Embedding embedding) {
        String id = randomUUID();
        add(id, embedding);
        return id;
    }

    @Override
    public void add(String id, Embedding embedding) {
        addInternal(id, embedding, null);
    }

    @Override
    public String add(Embedding embedding, TextSegment textSegment) {
        String id = randomUUID();
        addInternal(id, embedding, textSegment);
        return id;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        List<String> ids = embeddings.stream()
                .map(ignored -> randomUUID())
                .collect(Collectors.toList());
        addAllInternal(ids, embeddings, null);
        return ids;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings, List<TextSegment> embedded) {
        List<String> ids = embeddings.stream()
                .map(ignored -> randomUUID())
                .collect(Collectors.toList());
        addAllInternal(ids, embeddings, embedded);
        return ids;
    }

    @Override
    public List<EmbeddingMatch<TextSegment>> findRelevant(Embedding referenceEmbedding, int maxResults, double minScore) {
        // Using KNN query on @vector field
        String queryTemplate = "*=>[ KNN %d @%s $BLOB AS vector_score ]";
        Query query = new Query(String.format(queryTemplate, maxResults, schema.getVectorFieldName()))
                .addParam("BLOB", ToByteArray(referenceEmbedding.vector()))
                .returnFields(schema.getIdFieldName(), schema.getVectorFieldName(), schema.getScalarFieldName(), "vector_score")
                .setSortBy("vector_score", false)
                .dialect(2);

        SearchResult result = client.ftSearch(schema.getIndexName(), query);
        List<Document> documents = result.getDocuments();

        return toEmbeddingMatch(documents);
    }

    private boolean isIndexExist(String indexName) {
        // jedis do not contain method like ftExists
        Set<String> indexSets = client.ftList();
        return indexSets.contains(indexName);
    }

    private void addInternal(String id, Embedding embedding, TextSegment embedded) {
        addAllInternal(Collections.singletonList(id), Collections.singletonList(embedding), embedded == null ? null : Collections.singletonList(embedded));
    }

    private void addAllInternal(List<String> ids, List<Embedding> embeddings, List<TextSegment> embedded) {
        if (isCollectionEmpty(ids) || isCollectionEmpty(embeddings)) {
            log.info("[do not add empty embeddings to elasticsearch]");
            return;
        }
        ValidationUtils.ensureTrue(ids.size() == embeddings.size(), "ids size is not equal to embeddings size");
        ValidationUtils.ensureTrue(embedded == null || embeddings.size() == embedded.size(), "embeddings size is not equal to embedded size");

        int size = ids.size();
        for (int i = 0; i < size; i++) {
            String id = ids.get(i);
            Embedding embedding = embeddings.get(i);
            TextSegment textSegment = embedded == null ? null : embedded.get(i);
            Map<byte[], byte[]> vectorField = new HashMap<>();
            vectorField.put(schema.getVectorFieldName().getBytes(), ToByteArray(embedding.vector()));
            String key = schema.getPrefix() + ":" + id;
            client.hset(key.getBytes(), vectorField);

            // see https://github.com/redis/jedis/issues/3339
            client.hsetnx(key, schema.getIdFieldName(), id);
            if (textSegment != null) {
                client.hsetnx(key, schema.getScalarFieldName(), textSegment.text());
                client.hsetnx(key.getBytes(), "metadata".getBytes(), mapToBytes(textSegment.metadata().asMap()));
            }
        }
    }

    private List<EmbeddingMatch<TextSegment>> toEmbeddingMatch(List<Document> documents) {
        log.info(GSON.toJson(documents));
        // TODO: extract result documents
        return new ArrayList<>();
    }

    private byte[] mapToBytes(Map<String, String> map) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try {
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(map);
            out.flush();
        } catch (IOException e) {
            log.error("[RedisEmbeddingStoreImpl] map to byte array error", e);
        }
        return byteOut.toByteArray();
    }
}
