package com.kmwllc.brigade.connector;

import org.slf4j.Logger;

import com.kmwllc.brigade.logging.LoggerFactory;

public class ConnectorRunner extends Thread {

  public final static Logger log = LoggerFactory.getLogger(ConnectorRunner.class.getCanonicalName());

  AbstractConnector connector = null;
  public ConnectorRunner(AbstractConnector abstractConnector) {
    connector = abstractConnector;
  }
  @Override
  public void run() {
    super.run();
    try {
      connector.start();
    } catch (InterruptedException e) {
      log.warn("Connecctor start error {}", e);
      // LOG what if the connector can't start, or is interrupted.
    }
  }
  @Override
  public synchronized void start() {
    super.start();
    // Start the thread
  }

}
