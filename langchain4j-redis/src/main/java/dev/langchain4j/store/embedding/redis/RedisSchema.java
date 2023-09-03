package dev.langchain4j.store.embedding.redis;

import redis.clients.jedis.search.schemafields.NumericField;
import redis.clients.jedis.search.schemafields.SchemaField;
import redis.clients.jedis.search.schemafields.TextField;
import redis.clients.jedis.search.schemafields.VectorField;

/**
 * Build Default Redis Schema
 */
public class RedisSchema {

    private static final SchemaField DEFAULT_VECTOR_SCHEMA = VectorField.builder()
            .fieldName("$.vector")
            .algorithm(VectorField.VectorAlgorithm.FLAT)
            .addAttribute("dims", 1536)
            .addAttribute("distance_metric", "CONSINE")
            .addAttribute("datatype", "FLOAT32")
            .as("vector")
            .build();

    public static final SchemaField[] DEFAULT_SCHEMA = {
            // id field
            NumericField.of("$.id").as("id"),
            // embedded field
            TextField.of("$.text").as("text"),
            // embedding field
            DEFAULT_VECTOR_SCHEMA,
    };
}
