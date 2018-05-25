package com.kmwllc.brigade.connector;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.workflow.WorkflowServer;
import org.slf4j.Logger;

import java.util.HashMap;

/**
 * A singleton instance in the JVM that manages the currently defined and loaded Connectors.
 * <br/><br/>
 * ConnectorServer fires a ConnectorStart event when a connector is registered.  The connector
 * delegates this event to whatever ConnectorEventListeners are registered with it.
 * 
 * @author kwatters
 *
 */
public class ConnectorServer {

  public final static Logger log = LoggerFactory.getLogger(ConnectorServer.class.getCanonicalName());
  private static ConnectorServer instance = null;

  private HashMap<String,AbstractConnector> connectorMap;

  // singleton, the constructor is private.
  private ConnectorServer() {
    connectorMap = new HashMap<>();
  }

  /**
   * Get the singleton instance of the connector server
   * @return THE ConnectorServer
   */
  public static ConnectorServer getInstance() {
    if (instance ==null) {
      instance = new ConnectorServer();
      return instance;
    } else {
      return instance;
    }
  }

  /**
   * Register a connector with the ConnectorServer.  This method uses reflection to instantiate the specific
   * class of Connector that is specified in the config.  It also associates the connector with the appropriate
   * pipeline and fires a ConnectorBegin event
   * @param config ConnectorConfig to build the Connector instance from
   * @return A populated Connector object
   * @throws ClassNotFoundException if the connector could not be built via reflection
   */
  public AbstractConnector addConnector(ConnectorConfig config) throws ClassNotFoundException {
    String connectorClass = config.getConnectorClass().trim();
    log.info("Loading Connector :"  + config.getConnectorName() + " class=" + config.getConnectorClass());
    Class<?> sc = AbstractConnector.class.getClassLoader().loadClass(connectorClass);
    try {
      AbstractConnector connectorInst = (AbstractConnector) sc.newInstance();
      connectorInst.setWorkflowServer(WorkflowServer.getInstance());
      connectorInst.setConfig(config);
      connectorInst.fireConnectorBegin(config);
      connectorInst.initialize();
      connectorMap.put(config.getConnectorName(), connectorInst);
      return connectorInst;
    } catch (InstantiationException | IllegalAccessException e) {
      log.warn("Error creating connector: {}", e);
      return null;
    }
  }

  /**
   * Get connector with specified name from registry of connectors or null if not found.
   * @param connectorName Name of connector
   * @return Connector or null if not found
   */
  public AbstractConnector getConnector(String connectorName) {
    if (connectorMap.containsKey(connectorName)) {
      return connectorMap.get(connectorName);
    } else {
      return null;
    }
  }

  /**
   * Answers whether a connector with the specified name is registered
   * @param connectorName Name of connector to find
   * @return whether we found the connector
   */
  public boolean hasConnector(String connectorName) {
    if (connectorMap.containsKey(connectorName)) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Get processing state for the connector with specified name.  Returns null if the connector
   * is not found
   * @param connectorName Name of connector
   * @return Processing state of connector or null if the connector was not found
   */
  public ConnectorState getConnectorState(String connectorName) {
    if (connectorMap.containsKey(connectorName)) {
      return connectorMap.get(connectorName).getState();
    } else {
      return null;
    }
  }

  /**
   * Get names of all connectors registered with this server.
   * @return Array containing names of connectors
   */
  public String[] listConnectors() {
    int size = connectorMap.keySet().size();
    String[] names = new String[size];
    connectorMap.keySet().toArray(names);
    return names;
  }

  /**
   * Start the connector with the specified name.  This method wraps the connector in a thread and
   * starts it.  A warning will be issues if the connector is already running.  This should not be invoked
   * directly by client code; it is automatically called during standard Brigade execution.
   * @param connectorName
   */
  public void startConnector(String connectorName) {
    if (!connectorMap.containsKey(connectorName)) {
      // TODO: unknown connector.  throw error?
      log.warn("Tried to start an unknown connector.");
      return;
    }
    if (getConnectorState(connectorName) != ConnectorState.RUNNING) {
      // we are clear to start because we're not currently running.
      // Create a new thread here a connector runner class?
      ConnectorRunner runner = new ConnectorRunner(connectorMap.get(connectorName));
      runner.start();
      // we are running...
    } else {
      log.warn("Connector is already running.");
    }
  }
}
