package com.kmwllc.brigade.connector;

import java.util.HashMap;

import com.kmwllc.brigade.config.ConnectorConfiguration;

public class ConnectorServer {

	private static ConnectorServer instance = null;
	
	private HashMap<String,AbstractConnector> connectorMap;
	
	// singleton, the constructor is private.
	private ConnectorServer() {
		connectorMap = new HashMap<String,AbstractConnector>();
	}
	
	// This is a singleton also
	public static ConnectorServer getInstance() {
		if (instance ==null) {
			instance = new ConnectorServer();
			return instance;
		} else {
			return instance;
		}
	}

	public void addConnector(ConnectorConfiguration config) throws ClassNotFoundException {
		String connectorClass = config.getConnectorClass();
		System.out.println("Loading Connector :"  + config.getConnectorName() + " class=" + config.getConnectorClass());
		Class<?> sc = AbstractConnector.class.getClassLoader().loadClass(connectorClass);
		try {
			AbstractConnector connectorInst = (AbstractConnector) sc.newInstance();
			connectorInst.initialize(config);
			connectorMap.put(config.getConnectorName(), connectorInst);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public AbstractConnector getConnector(String connectorName) {
		// TODO Auto-generated method stub
		if (connectorMap.containsKey(connectorName)) {
			return connectorMap.get(connectorName);
		} else {
			return null;
		}
	}
	
	public boolean hasConnector(String connectorName) {
		// TODO Auto-generated method stub
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
			System.out.println("Tried to start an unknown connector.");
			return;
		} 
		
		if (getConnectorState(connectorName) != ConnectorState.RUNNING) {
			// we are clear to start because we're not currently running.
			// Create a new thread here a connector runner class?
			ConnectorRunner runner = new ConnectorRunner(connectorMap.get(connectorName));
			runner.start();
			// we are running...
		} else {
			System.out.println("Connector is already running.");
		}
		
		
	}
}
