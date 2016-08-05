package com.kmwllc.brigade.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.interfaces.DocumentConnector;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.workflow.WorkflowMessage;
import com.kmwllc.brigade.workflow.WorkflowServer;

/**
 * 
 * AbstractConnector - base class for implementing a new document connector
 * service.
 * 
 */
public abstract class AbstractConnector implements DocumentConnector {

  public final static Logger log = LoggerFactory.getLogger(AbstractConnector.class.getCanonicalName());
  protected ConnectorState state = ConnectorState.STOPPED;
  private int batchSize = 1;
  // TODO: batching at the connector level? (microbatching?)
  // private List<Document> batch = Collections.synchronizedList(new ArrayList<Document>());
  private String docIdPrefix = "";
  // protected final String name;
  protected WorkflowServer workflowServer = WorkflowServer.getInstance();
  protected String workflowName;

  public AbstractConnector() {
    //this.name = name;
    // super(name);
    // no overruns!
    // this.getOutbox().setBlocking(true);
  }

  public abstract void setConfig(ConnectorConfig config);
  // public abstract void start() throws InterruptedException;
  public void start() throws InterruptedException {
    startCrawling();
  }
  
  public abstract void initialize();

//  public String getName() {
//    return name;
//  }

  public void feed(Document doc) {
    // System.out.println("Feeding document " + doc.getId());
    // TODO: add batching and change this to publishDocuments (as a list)
    // Batching for this sort of stuff is a very good thing.

    WorkflowMessage wm = new WorkflowMessage();
    wm.setDoc(doc);
    wm.setType("add");
    wm.setWorkflow(workflowName);
    // TODO: make this call async or a thread pool 
    // where we just put the message on a blocking queue.
    try {
      workflowServer.processMessage(wm);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      log.warn("Error happened while processing message.. interrupted!");
    }

    // TODO: consider if we want batching at this level or not.
    //    if (batchSize <= 1) {
    //      invoke("publishDocument", doc);
    //    } else {
    //      // handle the batch
    //      // TODO: make this synchronized and thread safe!
    //      batch.add(doc);
    //      if (batch.size() >= batchSize) {
    //        flush();
    //      }
    //    }
  }

  public void publishFlush() {
    // NoOp
    // Here for the framework to invoke it on the down stream services.
  };

  public void flush() {
    workflowServer.flush(workflowName);
  }

  public ConnectorState getState() {
    return state;
  }

  public void setState(ConnectorState state) {
    this.state = state;
  }

  public Document publishDocument(Document doc) {
    return doc;
  }

  public List<Document> publishDocuments(List<Document> batch) {
    return batch;
  }

  public ConnectorState getConnectorState() {
    return state;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public String getDocIdPrefix() {
    return docIdPrefix;
  }

  public void setDocIdPrefix(String docIdPrefix) {
    this.docIdPrefix = docIdPrefix;
  }

  @Override
  public void setWorkflowServer(WorkflowServer workflowServer) {
    // TODO Auto-generated method stub
   this.workflowServer = workflowServer; 
  }

  @Override
  public void setWorkflowName(String workflowName) {
    // TODO Auto-generated method stub
    this.workflowName = workflowName;
  }

  
}
