package dev.langchain4j.store.embedding.redis;

import lombok.Builder;
import lombok.Data;
import redis.clients.jedis.search.schemafields.NumericField;
import redis.clients.jedis.search.schemafields.SchemaField;
import redis.clients.jedis.search.schemafields.TextField;
import redis.clients.jedis.search.schemafields.VectorField;

import java.util.Optional;

import static dev.langchain4j.internal.ValidationUtils.ensureNotNull;

/**
 * Build Default Redis Schema
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
            .scalarFieldName("scalar")
            .dimension(1536)
            .build();
    private static final String FIELD_NAME_PREFIX = "$.";
    private static final VectorField.VectorAlgorithm DEFAULT_VECTOR_ALGORITHM = VectorField.VectorAlgorithm.HNSW;
    private static final MetricType DEFAULT_METRIC_TYPE = MetricType.COSINE;
    private static final DataType DEFAULT_DATA_TYPE = DataType.FLOAT64;

    private final String indexName;
    private final String prefix;

    private final String idFieldName;
    private final String vectorFieldName;
    private final String scalarFieldName;

    private final VectorField.VectorAlgorithm vectorAlgorithm;
    private final int dimension;
    private final MetricType metricType;
    private final DataType dataType;

    @Builder
    public RedisSchema(String indexName, String prefix,
                       String idFieldName, String vectorFieldName, String scalarFieldName,
                       VectorField.VectorAlgorithm vectorAlgorithm, Integer dimension, MetricType metricType, DataType dataType) {
        indexName = ensureNotNull(indexName, "indexName");
        idFieldName = ensureNotNull(idFieldName, "idFieldName");
        vectorFieldName = ensureNotNull(vectorFieldName, "vectorFieldName");
        scalarFieldName = ensureNotNull(scalarFieldName, "scalarFieldName");
        dimension = ensureNotNull(dimension, "dimension");

        this.indexName = indexName;
        this.prefix = prefix;

        this.idFieldName = idFieldName;
        this.vectorFieldName = vectorFieldName;
        this.scalarFieldName = scalarFieldName;

        this.vectorAlgorithm = vectorAlgorithm;
        this.dimension = dimension;
        this.metricType = metricType;
        this.dataType = dataType;
    }

    public SchemaField[] toSchemaField() {
        return new SchemaField[]{
                NumericField.of(FIELD_NAME_PREFIX + idFieldName).as(idFieldName),
                // embedded field
                TextField.of(FIELD_NAME_PREFIX + scalarFieldName).as(scalarFieldName),
                // embedding field
                VectorField.builder()
                        .fieldName(FIELD_NAME_PREFIX + vectorFieldName)
                        .algorithm(Optional.ofNullable(vectorAlgorithm).orElse(DEFAULT_VECTOR_ALGORITHM))
                        .addAttribute("DIM", dimension)
                        .addAttribute("DISTANCE_METRIC", Optional.ofNullable(metricType).map(MetricType::getName).orElse(DEFAULT_METRIC_TYPE.getName()))
                        // FLOAT32 or FLOAT64
                        .addAttribute("TYPE", Optional.ofNullable(dataType).map(DataType::getName).orElse(DEFAULT_DATA_TYPE.getName()))
                        .as(vectorFieldName)
                        .build()
        };
    }
}
