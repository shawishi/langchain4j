package dev.langchain4j.agent.toolkit.spark.tool;

import dev.langchain4j.agent.toolkit.BaseTool;
import dev.langchain4j.agent.toolkit.spark.SparkSQL;

/**
 * Tool for querying a Spark SQL.
 */
public class QuerySparkSQLTool extends BaseSparkSQLTool implements BaseTool {

    private static final String NAME = "query_sql_db";
    private static final String DESCRIPTION = "Input to this tool is a detailed and correct SQL query, output is a result from the Spark SQL.\n" +
            "If the query is not correct, an error message will be returned.\n" +
            "If an error is returned, rewrite the query, check the query, and try again.";

    public QuerySparkSQLTool(SparkSQL db) {
        super(db, NAME, DESCRIPTION);
    }
}
