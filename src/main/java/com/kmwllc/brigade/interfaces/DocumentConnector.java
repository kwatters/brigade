package com.kmwllc.brigade.interfaces;

import com.kmwllc.brigade.connector.ConnectorState;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.workflow.WorkflowServer;
/**
 * A document connector needs to implement these methods 
 * 
 * @author kwatters
 *
 */
public interface DocumentConnector {

  public void startCrawling();

  public void stopCrawling();

  public ConnectorState getConnectorState();

  // TODO: this should be "feed()"
  public Document publishDocument(Document doc);

  // TODO: should add "flush()"
  // TODO: review if we need both of these
  public void setWorkflowName(String workflowName);

  void setWorkflowServer(WorkflowServer workflowServer);
  
}
