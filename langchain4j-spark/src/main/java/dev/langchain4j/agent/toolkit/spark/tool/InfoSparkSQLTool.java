package dev.langchain4j.agent.toolkit.spark.tool;

import dev.langchain4j.agent.toolkit.BaseTool;
import dev.langchain4j.agent.toolkit.spark.SparkSQL;

/**
 * Tool for getting metadata about a Spark SQL.
 */
public class InfoSparkSQLTool extends BaseSparkSQLTool implements BaseTool {

    private static final String NAME = "schema_sql_db";
    private static final String DESCRIPTION = "Input to this tool is a comma-separated list of tables, output is the schema and sample rows for those tables.\n" +
            "Be sure that the tables actually exist by calling list_tables_sql_db first!\n" +
            "\n" +
            "Example Input: \"table1, table2, tablezxl3\"\n";

    public InfoSparkSQLTool(SparkSQL db) {
        super(db, NAME, DESCRIPTION);
    }
}
