package com.kmwllc.brigade.connector;

import com.kmwllc.brigade.config.ConnectorConfiguration;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.workflow.WorkflowMessage;
import com.kmwllc.brigade.workflow.WorkflowServer;
import com.kmwllc.brigade.connector.ConnectorState;

public abstract class AbstractConnector {

	public WorkflowServer workflowServer;
	private String workflow;
	public abstract void initialize(ConnectorConfiguration config);
	public abstract void start() throws InterruptedException;
	public abstract void shutdown();

	private ConnectorState state = ConnectorState.STOPPED;
	
	public AbstractConnector() {
		super();
		workflowServer = WorkflowServer.getInstance();
	}

	public void feed(Document doc) throws InterruptedException {
		
		// System.out.println("Feeding document " + doc.getId());
		// This is the method that is used
		// TODO: Here want are going to dispatch the document down the workflow
		WorkflowMessage wm = new WorkflowMessage();
		wm.setDoc(doc);
		wm.setType("add");
		wm.setWorkflow(getWorkflow());
		// TODO: make this call async or a thread pool 
		// where we just put the message on a blocking queue.
		workflowServer.processMessage(wm);
	}
	
	public void flush() {
		
		// Ouch what do I do here?!
		workflowServer.flush(workflow);
		
	}
	
	public String getWorkflow() {
		return workflow;
	}
	public void setWorkflow(String workflow) {
		this.workflow = workflow;
	}
	public ConnectorState getState() {
		return state;
	}
	public void setState(ConnectorState state) {
		this.state = state;
	}

}
