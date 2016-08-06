package com.kmwllc.brigade.config;

import java.util.ArrayList;

public class BrigadeConfig extends Config {

  private ArrayList<WorkflowConfig> workflowConfigs;
  private ArrayList<ConnectorConfig> connectorConfigs;

  public BrigadeConfig() {
    super();
    workflowConfigs = new ArrayList<WorkflowConfig>();
    connectorConfigs = new ArrayList<ConnectorConfig>();
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
