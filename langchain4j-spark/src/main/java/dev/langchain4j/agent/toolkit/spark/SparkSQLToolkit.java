package dev.langchain4j.agent.toolkit.spark;

import dev.langchain4j.agent.toolkit.BaseTool;
import dev.langchain4j.agent.toolkit.Toolkit;
import dev.langchain4j.agent.toolkit.spark.tool.InfoSparkSQLTool;
import dev.langchain4j.agent.toolkit.spark.tool.ListSparkSQLTool;
import dev.langchain4j.agent.toolkit.spark.tool.QueryCheckerTool;
import dev.langchain4j.agent.toolkit.spark.tool.QuerySparkSQLTool;
import dev.langchain4j.model.chat.ChatLanguageModel;

import java.util.Arrays;
import java.util.List;

public class SparkSQLToolkit implements Toolkit {

    private SparkSQL db;
    private ChatLanguageModel llm;

    @Override
    public List<BaseTool> getTools() {
        return Arrays.asList(
                new QuerySparkSQLTool(db),
                new QueryCheckerTool(),
                new InfoSparkSQLTool(db),
                new ListSparkSQLTool(db)
        );
    }
}
