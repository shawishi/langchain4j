package dev.langchain4j.agent.toolkit.spark.tool;

import dev.langchain4j.agent.toolkit.BaseTool;
import dev.langchain4j.agent.toolkit.spark.SparkSQL;

/**
 * Tool for getting tables names.
 */
public class ListSparkSQLTool extends BaseSparkSQLTool implements BaseTool {

    private static final String NAME = "list_tables_sql_db";
    private static final String DESCRIPTION = "Input is an empty string, output is a comma separated list of tables in the Spark SQL.";

    public ListSparkSQLTool(SparkSQL db) {
        super(db, NAME, DESCRIPTION);
    }
}
