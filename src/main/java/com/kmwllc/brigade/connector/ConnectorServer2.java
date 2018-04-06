package com.kmwllc.brigade.connector;

import com.kmwllc.brigade.config2.ConnectorConfig2;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.workflow.WorkflowServer;
import com.kmwllc.brigade.workflow.WorkflowServer2;
import org.slf4j.Logger;

import java.util.HashMap;

/**
 * A singleton instance in the JVM that manages the currently defined and loaded Connectors.
 * 
 * @author kwatters
 *
 */
public class ConnectorServer2 {

  public final static Logger log = LoggerFactory.getLogger(ConnectorServer2.class.getCanonicalName());
  private static ConnectorServer2 instance = null;

  private HashMap<String,AbstractConnector2> connectorMap;

  // singleton, the constructor is private.
  private ConnectorServer2() {
    connectorMap = new HashMap<>();
  }

  // This is a singleton also
  public static ConnectorServer2 getInstance() {
    if (instance ==null) {
      instance = new ConnectorServer2();
      return instance;
    } else {
      return instance;
    }
  }

  public AbstractConnector2 addConnector(ConnectorConfig2 config) throws ClassNotFoundException {
    String connectorClass = config.getConnectorClass().trim();
    log.info("Loading Connector :"  + config.getConnectorName() + " class=" + config.getConnectorClass());
    Class<?> sc = AbstractConnector2.class.getClassLoader().loadClass(connectorClass);
    try {
      AbstractConnector2 connectorInst = (AbstractConnector2) sc.newInstance();
      connectorInst.setWorkflowServer(WorkflowServer2.getInstance());
      connectorInst.setConfig(config);
      connectorInst.initialize();
      connectorMap.put(config.getConnectorName(), connectorInst);
      return connectorInst;
    } catch (InstantiationException | IllegalAccessException e) {
      log.warn("Error creating connector: {}", e);
      return null;
    }
  }

  public AbstractConnector2 getConnector(String connectorName) {
    if (connectorMap.containsKey(connectorName)) {
      return connectorMap.get(connectorName);
    } else {
      return null;
    }
  }

  public boolean hasConnector(String connectorName) {
    if (connectorMap.containsKey(connectorName)) {
      return true;
    } else {
      return false;
    }
  }

  public ConnectorState getConnectorState(String connectorName) {
    if (connectorMap.containsKey(connectorName)) {
      return connectorMap.get(connectorName).getState();
    } else {
      return null;
    }
  }

  public String[] listConnectors() {
    int size = connectorMap.keySet().size();
    String[] names = new String[size];
    connectorMap.keySet().toArray(names);
    return names;
  }

  public void startConnector(String connectorName) {
    if (!connectorMap.containsKey(connectorName)) {
      // TODO: unknown connector.  throw error?
      log.warn("Tried to start an unknown connector.");
      return;
    }
    if (getConnectorState(connectorName) != ConnectorState.RUNNING) {
      // we are clear to start because we're not currently running.
      // Create a new thread here a connector runner class?
      ConnectorRunner2 runner = new ConnectorRunner2(connectorMap.get(connectorName));
      runner.start();
      // we are running...
    } else {
      log.warn("Connector is already running.");
    }
  }
}
