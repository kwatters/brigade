
package com.kmwllc.brigade.connector;
import com.kmwllc.brigade.config.ConnectorConfiguration;
import com.kmwllc.brigade.document.Document;

public class DocumentSequenceConnector extends AbstractConnector {

	private int startId = 0;
	private int stopId = 10000;
	
	@Override
	public void initialize(ConnectorConfiguration config) {
		// TODO Auto-generated method stub

		System.out.println("Initialize method for connector");
		// TODO : handle integer properties
		String start = config.getProperty("start", "1");		
		String stop = config.getProperty("stop", "100000");
		
		startId = Integer.parseInt(start);
		stopId = Integer.parseInt(stop);
		
		setWorkflow(config.getWorkflow());
		
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
	public void shutdown() {
		// TODO Auto-generated method stub
		flush();
		setState(ConnectorState.STOPPED);
		System.out.println("Connector finished.");
	}

}
