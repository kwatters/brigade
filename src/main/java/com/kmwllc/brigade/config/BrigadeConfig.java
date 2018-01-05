package com.kmwllc.brigade.config;

import java.util.ArrayList;

/**
 * This represents the overall configuration for a brigade instance.  It includes
 * a list of workflow configs and a list of connector configs.
 *
 * @author kwatters
 */
public class BrigadeConfig extends Config {

    private ArrayList<WorkflowConfig> workflowConfigs;
    private ArrayList<ConnectorConfig> connectorConfigs;

    public BrigadeConfig() {
        workflowConfigs = new ArrayList<>();
        connectorConfigs = new ArrayList<>();
    }

    public void addWorkflowConfig(WorkflowConfig wc) {
        workflowConfigs.add(wc);
    }

    public void addConnectorConfig(ConnectorConfig cc) {
        connectorConfigs.add(cc);
    }

    public ArrayList<WorkflowConfig> getWorkflowConfigs() {
        return workflowConfigs;
    }

    public ArrayList<ConnectorConfig> getConnectorConfigs() {
        return connectorConfigs;
    }

}
