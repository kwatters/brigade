
package com.kmwllc.brigade.connector;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;

public class DocumentSequenceConnector extends AbstractConnector {

  public DocumentSequenceConnector() {
    super();
  }

  private int startId = 0;
  private int stopId = 10000;
  ConnectorConfig config;

  public void setConfig(ConnectorConfig config) {
    this.config = config;
  }

  @Override
  public void initialize() {
	  // TODO: workflow name should be a more generic concept ...
	  // perhaps replace with a topic id?
    workflowName = config.getProperty("workflowName");
    log.info("Initialize method for connector");
    startId = config.getIntegerParam("start", 1);		
    stopId = config.getIntegerParam("stop", 100000);
  }

  @Override
  public void startCrawling() {
    setState(ConnectorState.RUNNING);
    for (int i = startId; i <= stopId; i++) {
      Document d = new Document(Integer.toString(i));
      feed(d);
    }
    setState(ConnectorState.STOPPED);
  }

  @Override
  public void stopCrawling() {
    // no-op
  }


}
