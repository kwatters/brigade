package com.kmwllc.brigade.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This represents the overall configuration for a brigade instance.  It includes
 * a list of workflow configs and a list of connector configs.
 *
 * @author kwatters
 */
public class BrigadeConfig {

    private List<WorkflowConfig<?>> workflowConfigs;
    private List<ConnectorConfig> connectorConfigs;
    private Map<String, String> props;

    public BrigadeConfig() {
        workflowConfigs = new ArrayList<>();
        connectorConfigs = new ArrayList<>();
        props = new HashMap<>();
    }

    public void addWorkflowConfig(WorkflowConfig wc) {
        workflowConfigs.add(wc);
    }

    public void addConnectorConfig(ConnectorConfig cc) {
        connectorConfigs.add(cc);
    }

    public List<WorkflowConfig<?>> getWorkflowConfigs() {
        return workflowConfigs;
    }

    public List<ConnectorConfig> getConnectorConfigs() {
        return connectorConfigs;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }
}
