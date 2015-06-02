package com.kmwllc.brigade.config;

public class ConnectorConfiguration extends Configuration {

	private String connectorName;
	private String workflow;
	private String connectorClass;

	public String getConnectorName() {
		return connectorName;
	}

	public void setConnectorName(String connectorName) {
		this.connectorName = connectorName;
	}

	public String getWorkflow() {
		return workflow;
	}

	public void setWorkflow(String workflow) {
		this.workflow = workflow;
	}

	public String getConnectorClass() {
		return connectorClass;
	}

	public void setConnectorClass(String connectorClass) {
		this.connectorClass = connectorClass;
	}
	
	
	
}
