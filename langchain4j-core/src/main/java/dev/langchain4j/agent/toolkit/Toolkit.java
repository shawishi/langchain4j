package dev.langchain4j.agent.toolkit;

import java.util.List;

public interface Toolkit {

    List<BaseTool> getTools();
}
