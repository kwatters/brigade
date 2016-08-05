package com.kmwllc.brigade.interfaces;

import com.kmwllc.brigade.connector.ConnectorState;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.workflow.WorkflowServer;

public interface DocumentConnector {

  public void startCrawling();

  public ConnectorState getConnectorState();

  public void stopCrawling();

  public Document publishDocument(Document doc);

  public void setWorkflowName(String workflowName);

  void setWorkflowServer(WorkflowServer workflowServer);
  
}
