package com.kmwllc.brigade.config2;

import com.kmwllc.brigade.config.Config;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * This represents the overall configuration for a brigade instance.  It includes
 * a list of workflow configs and a list of connector configs.
 *
 * @author kwatters
 */
public class BrigadeConfig2 {

    private List<WorkflowConfig2<?>> workflowConfigs;
    private List<ConnectorConfig2> connectorConfigs;

    public BrigadeConfig2() {
        workflowConfigs = new ArrayList<>();
        connectorConfigs = new ArrayList<>();
    }

    public void addWorkflowConfig(WorkflowConfig2 wc) {
        workflowConfigs.add(wc);
    }

    public void addConnectorConfig(ConnectorConfig2 cc) {
        connectorConfigs.add(cc);
    }

    public List<WorkflowConfig2<?>> getWorkflowConfigs() {
        return workflowConfigs;
    }

    public List<ConnectorConfig2> getConnectorConfigs() {
        return connectorConfigs;
    }

}
