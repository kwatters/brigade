package com.kmwllc.brigade.config2;

import com.kmwllc.brigade.config.StageConfig;

import java.util.List;

public interface WorkflowConfig2<X extends StageConfig2> extends Config2<WorkflowConfig2> {
    List<X> getStages();

    String getName();

    int getNumWorkerThreads();

    int getQueueLength();

    void addStage(X stage);
}
