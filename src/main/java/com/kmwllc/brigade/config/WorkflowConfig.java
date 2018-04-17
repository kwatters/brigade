package com.kmwllc.brigade.config;

import java.util.List;

public interface WorkflowConfig<X extends StageConfig> extends Config<WorkflowConfig> {
    List<X> getStages();

    String getName();

    int getNumWorkerThreads();

    int getQueueLength();

    void addStage(X stage);
}
