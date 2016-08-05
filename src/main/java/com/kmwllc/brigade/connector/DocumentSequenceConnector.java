
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

    workflowName = config.getProperty("workflowName");
    log.info("Initialize method for connector");
    // TODO : handle integer properties
    String start = config.getProperty("start", "1");		
    String stop = config.getProperty("stop", "100000");

    startId = Integer.parseInt(start);
    stopId = Integer.parseInt(stop);

    // TODO: what do i want here?
    //setWorkflow(config.getWorkflow());

  }

  @Override
  public void start() throws InterruptedException {
    // TODO Auto-generated method stub
    setState(ConnectorState.RUNNING);
    for (int i = startId; i <= stopId; i++) {
      Document d = new Document(Integer.toString(i));
      feed(d);
    }
    setState(ConnectorState.STOPPED);
  }

  @Override
  public void startCrawling() {
    // TODO Auto-generated method stub

  }

  @Override
  public void stopCrawling() {
    // TODO Auto-generated method stub

  }


}
