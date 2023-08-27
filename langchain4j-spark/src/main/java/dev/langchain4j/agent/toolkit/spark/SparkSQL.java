package dev.langchain4j.agent.toolkit.spark;

import com.google.common.collect.Sets;
import dev.langchain4j.agent.toolkit.spark.enums.FetchType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;

import java.util.*;
import java.util.stream.Collectors;

/**
 * SparkSQL is a utility class for interacting with Spark SQL.
 */
public class SparkSQL {

    private SparkSession spark;

    private final Set<String> ignoreTables;
    private final Set<String> includeTables;
    private Set<String> allTables;
    private Set<String> usableTables;

    private final int sampleRowsInTableInfo;

    public static SparkSQL fromUri(String uri) {
        SparkSession sparkSession = SparkSession.builder()
                .master(uri)
                .getOrCreate();
        return SparkSQL.builder()
                .sparkSession(sparkSession)
                .build();
    }

    public SparkSQL(Builder builder) {
        initSpark(builder.sparkSession, builder.catalog, builder.schema);
        this.ignoreTables = builder.ignoreTables;
        this.includeTables = builder.includeTables;
        this.sampleRowsInTableInfo = builder.sampleRowsInTableInfo;
        initTable();
    }

    public String run(String command, FetchType fetch) {
        Dataset<Row> df = spark.sql(command);
        if (FetchType.ONE.equals(fetch)) {
            df = df.limit(1);
        }
        return getDataframeResults(df).toString();
    }

    public String runNoThrow(String command, FetchType fetch) {
        try {
            return run(command, fetch);
        } catch (Exception e) {
            // Format the error message
            return "Error: " + e;
        }
    }

    public Set<String> getUsableTableNames() {
        // Get names of tables available.
        if (!includeTables.isEmpty()) {
            return includeTables;
        }

        // sorting the result can help LLM understanding it.
        return Sets.difference(allTables, ignoreTables).stream()
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public String getTableInfo(Set<String> tableNames) {
        Set<String> allTableNames = getUsableTableNames();
        if (tableNames != null) {
            Set<String> missingTables = Sets.difference(tableNames, allTableNames);
            if (!missingTables.isEmpty()) {
                throw new IllegalArgumentException("tableNames " + missingTables + " not found in database");
            }
            allTableNames = tableNames;
        }

        List<String> tables = new ArrayList<>();
        for (String tableName : allTableNames) {
            String tableInfo = getCreateTableStmt(tableName);
            if (sampleRowsInTableInfo > 0) {
                String sampleRows = getSampleSparkRows(tableName);
                tableInfo += "\n\n/*\n" + sampleRows + "\n*/";
            }
            tables.add(tableInfo);
        }
        return String.join("\n\n", tables);
    }

    public String getTableInfoNoThrow(Set<String> tableNames) {
        try {
            return getTableInfo(tableNames);
        } catch (Exception e) {
            return "Error: " + e;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private List<List<String>> getDataframeResults(Dataset<Row> df) {
        return df.collectAsList().stream()
                .map(this::convertRowAsList)
                .collect(Collectors.toList());
    }

    private List<String> convertRowAsList(Row row) {
        return Arrays.stream(((GenericRowWithSchema) row).values())
                .map(String::valueOf)
                .collect(Collectors.toList());
    }

    private void initSpark(SparkSession sparkSession, String catalog, String schema) {
        spark = Optional.ofNullable(sparkSession).orElse(SparkSession.builder().getOrCreate());
        // set catalog and schema
        if (catalog != null) {
            spark.catalog().setCurrentCatalog(catalog);
        }
        if (schema != null) {
            spark.catalog().setCurrentDatabase(schema);
        }
    }

    private void initTable() {
        allTables = getAllTables();
        if (!includeTables.isEmpty()) {
            Set<String> missingTables = Sets.difference(includeTables, allTables);
            if (!missingTables.isEmpty()) {
                throw new IllegalArgumentException("includeTables " + missingTables + " not found in database");
            }
        }
        if (!ignoreTables.isEmpty()) {
            Set<String> missingTables = Sets.difference(ignoreTables, allTables);
            if (!missingTables.isEmpty()) {
                throw new IllegalArgumentException("ignoreTables " + missingTables + " not found in database");
            }
        }
        usableTables = getUsableTableNames();
    }

    private Set<String> getAllTables() {
        List<Row> rows = spark.sql("SHOW TABLES").select("tableName").collectAsList();
        return rows.stream()
                .map(row -> row.getString(row.fieldIndex("tableName")))
                .collect(Collectors.toSet());
    }

    private String getCreateTableStmt(String table) {
        Row row = spark.sql(String.format("SHOW CREATE TABLE %s", table)).collectAsList().get(0);
        String statement = row.getString(row.fieldIndex("createtab_stmt"));

        // Ignore the data source provider and options to reduce the number of tokens.
        int usingClauseIndex = statement.indexOf("USING");
        return statement.substring(0, usingClauseIndex) + ";";
    }

    private String getSampleSparkRows(String table) {
        String query = String.format("SELECT * FROM %s LIMIT %d", table, sampleRowsInTableInfo);
        Dataset<Row> df = spark.sql(query);
        String columnsStr = Arrays.stream(df.schema().fields())
                .map(StructField::name)
                .collect(Collectors.joining("\t"));

        String sampleRowsStr;
        try {
            List<List<String>> sampleRows = getDataframeResults(df);
            sampleRowsStr = sampleRows.stream()
                    .map(row -> String.join("\t", row))
                    .collect(Collectors.joining("\n"));
        } catch (Exception e) {
            sampleRowsStr = "";
        }
        return String.format("%d rows from %s table:%n%s%n%s", sampleRowsInTableInfo, table, columnsStr, sampleRowsStr);
    }

    public static final class Builder {

        private SparkSession sparkSession;
        private String catalog;
        private String schema;

        private Set<String> ignoreTables = Sets.newHashSet();
        private Set<String> includeTables = Sets.newHashSet();

        private int sampleRowsInTableInfo = 3;

        public Builder sparkSession(SparkSession sparkSession) {
            this.sparkSession = sparkSession;
            return this;
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder ignoreTables(Set<String> ignoreTables) {
            this.ignoreTables = ignoreTables;
            return this;
        }

        public Builder includeTables(Set<String> includeTables) {
            this.includeTables = includeTables;
            return this;
        }

        public Builder sampleRowsInTableInfo(int sampleRowsInTableInfo) {
            this.sampleRowsInTableInfo = sampleRowsInTableInfo;
            return this;
        }

        public SparkSQL build() {
            return new SparkSQL(this);
        }
    }
}
