package com.kmwllc.brigade.connector;

public class ConnectorRunner extends Thread {

  AbstractConnector connector = null;
  public ConnectorRunner(AbstractConnector abstractConnector) {
    // TODO Auto-generated constructor stub
    connector = abstractConnector;
  }
  @Override
  public void run() {
    // TODO Auto-generated method stub
    super.run();
    try {
      connector.start();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      // LOG what if the connector can't start, or is interrupted.
    }
  }
  @Override
  public synchronized void start() {
    super.start();
    // Start the thread
  }

}
