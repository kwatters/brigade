package com.kmwllc.brigade.config;

import java.util.ArrayList;

public class BrigadeConfiguration extends Configuration {

	private ArrayList<WorkflowConfiguration> workflowConfigs;
	private ArrayList<ConnectorConfiguration> connectorConfigs;
	
	public BrigadeConfiguration() {
		super();
		// TODO Auto-generated constructor stub
		workflowConfigs = new ArrayList<WorkflowConfiguration>();
		connectorConfigs = new ArrayList<ConnectorConfiguration>();
	}

	public void addWorkflowConfig(WorkflowConfiguration wc) {
		workflowConfigs.add(wc);
	}
	
	public void addConnectorConfig(ConnectorConfiguration cc) {
		connectorConfigs.add(cc);
	}

	public ArrayList<WorkflowConfiguration> getWorkflowConfigs() {
		return workflowConfigs;
	}

	public ArrayList<ConnectorConfiguration> getConnectorConfigs() {
		return connectorConfigs;
	}
	
}
