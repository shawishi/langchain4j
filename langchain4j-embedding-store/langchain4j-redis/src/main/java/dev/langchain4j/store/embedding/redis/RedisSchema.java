package dev.langchain4j.store.embedding.redis;

import lombok.Builder;
import lombok.Data;
import redis.clients.jedis.search.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static dev.langchain4j.internal.ValidationUtils.ensureNotNull;

/**
 * Redis Schema Description
 */
@Data
public class RedisSchema {

    /**
     * Default Schema
     */
    public static final RedisSchema DEFAULT_SCHEMA = RedisSchema.builder()
            .indexName("test-index")
            .prefix("test")
            .idFieldName("id")
            .vectorFieldName("vector")
            .scalarFieldName("text")
            .metadataFieldName("metadata")
            .dimension(512)
            .build();
    public static final String SCORE_FIELD_NAME = "vector_score";
    private static final Schema.VectorField.VectorAlgo DEFAULT_VECTOR_ALGORITHM = Schema.VectorField.VectorAlgo.HNSW;
    private static final MetricType DEFAULT_METRIC_TYPE = MetricType.COSINE;
    private static final DataType DEFAULT_DATA_TYPE = DataType.FLOAT32;

    private final String indexName;
    private final String prefix;

    private final String idFieldName;
    private final String vectorFieldName;
    private final String scalarFieldName;
    private final String metadataFieldName;

    private final Schema.VectorField.VectorAlgo vectorAlgorithm;
    private final int dimension;
    private final MetricType metricType;
    private final DataType dataType;

    @Builder
    public RedisSchema(String indexName, String prefix,
                       String idFieldName, String vectorFieldName, String scalarFieldName, String metadataFieldName,
                       Schema.VectorField.VectorAlgo vectorAlgorithm, Integer dimension, MetricType metricType, DataType dataType) {
        indexName = ensureNotNull(indexName, "indexName");
        prefix = ensureNotNull(prefix, "prefix");
        idFieldName = ensureNotNull(idFieldName, "idFieldName");
        vectorFieldName = ensureNotNull(vectorFieldName, "vectorFieldName");
        scalarFieldName = ensureNotNull(scalarFieldName, "scalarFieldName");
        metadataFieldName = ensureNotNull(metadataFieldName, "metadataFieldName");
        dimension = ensureNotNull(dimension, "dimension");

        this.indexName = indexName;
        // if prefix is null, use indexName as default prefix
        this.prefix = prefix;

        this.idFieldName = idFieldName;
        this.vectorFieldName = vectorFieldName;
        this.scalarFieldName = scalarFieldName;
        this.metadataFieldName = metadataFieldName;

        this.vectorAlgorithm = vectorAlgorithm;
        this.dimension = dimension;
        this.metricType = metricType;
        this.dataType = dataType;
    }

    public Schema toSchema() {
        Map<String, Object> vectorAttrs = new HashMap<>();
        vectorAttrs.put("DIM", dimension);
        vectorAttrs.put("DISTANCE_METRIC", Optional.ofNullable(metricType).map(MetricType::getName).orElse(DEFAULT_METRIC_TYPE.getName()));
        vectorAttrs.put("TYPE", Optional.ofNullable(dataType).map(DataType::getName).orElse(DEFAULT_DATA_TYPE.getName()));
        vectorAttrs.put("INITIAL_CAP", 5);
        return new Schema()
                // actual do not need id field, because key contains id
                .addTextField(idFieldName, 1.0)
                .addTextField(scalarFieldName, 1.0)
                .addTextField("metadata", 1.0)
                .addVectorField(vectorFieldName, Optional.ofNullable(vectorAlgorithm).orElse(DEFAULT_VECTOR_ALGORITHM), vectorAttrs);
    }
}
