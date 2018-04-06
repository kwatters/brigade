package com.kmwllc.brigade.interfaces;

import com.kmwllc.brigade.connector.ConnectorState;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.workflow.WorkflowServer;
import com.kmwllc.brigade.workflow.WorkflowServer2;

/**
 * A document connector needs to implement these methods 
 * 
 * @author kwatters
 *
 */
public interface DocumentConnector2 {

  void startCrawling() throws Exception;

  void stopCrawling();

  ConnectorState getConnectorState();

  // TODO: this should be "feed()"
  Document publishDocument(Document doc);

  // TODO: should add "flush()"
  // TODO: review if we need both of these
  void setWorkflowName(String workflowName);

  void setWorkflowServer(WorkflowServer2 workflowServer);
  
}
