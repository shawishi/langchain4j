package dev.langchain4j.agent.toolkit.spark.tool;

import dev.langchain4j.agent.toolkit.spark.SparkSQL;

public abstract class BaseSparkSQLTool {

    protected final SparkSQL db;
    protected final String name;
    protected final String description;

    protected BaseSparkSQLTool(SparkSQL db, String name, String description) {
        this.db = db;
        this.name = name;
        this.description = description;
    }
}
