package com.kmwllc.brigade.connector;

import java.util.Date;

import org.junit.Test;

import com.kmwllc.brigade.config.ConnectorConfiguration;
import com.kmwllc.brigade.config.StageConfiguration;
import com.kmwllc.brigade.config.WorkflowConfiguration;
import com.kmwllc.brigade.workflow.Workflow;
import com.kmwllc.brigade.workflow.WorkflowServer;

public class DocumentSequenceConnectorTest {

	public WorkflowConfiguration createWorkflowConfig() throws ClassNotFoundException {
		// Create a workflow config
		WorkflowConfiguration wC = new WorkflowConfiguration();
		wC.setName("ingest");
		
		StageConfiguration s1Conf = new StageConfiguration();
		s1Conf.setStageClass("com.kmwllc.brigade.stage.SetStaticFieldValue");
		s1Conf.setStageName("set title");
		s1Conf.setStringParam("fieldName", "title");
		s1Conf.setStringParam("value", "Hello World.");
		
		StageConfiguration s2Conf = new StageConfiguration();
		s2Conf.setStageClass("com.kmwllc.brigade.stage.SetStaticFieldValue");
		s2Conf.setStageName("set title");
		s2Conf.setStringParam("fieldName", "text");
		s2Conf.setStringParam("value", "Welcome to Brigade.");
		
		StageConfiguration s3Conf = new StageConfiguration();
		s3Conf.setStageName("Solr Sender");
		s3Conf.setStageClass("com.kmwllc.brigade.stage.SendToSolr");
		s3Conf.setStringParam("solrUrl", "http://localhost:8983/solr");
		s3Conf.setStringParam("idField", "id");

		wC.addStageConfig(s1Conf);
		wC.addStageConfig(s2Conf);
		wC.addStageConfig(s3Conf);
		// Create a workflow
		
		return wC;
	}

	@Test
	public void testConnector() throws ClassNotFoundException, InterruptedException {
		// We should initialize the workflow server..
		
		WorkflowConfiguration wC = createWorkflowConfig();
		
		WorkflowServer ws = WorkflowServer.getInstance();
		ws.addWorkflow(wC);
		
		ConnectorConfiguration config = new ConnectorConfiguration();
		config.setWorkflow("ingest");
		config.setStringParam("stop", "100000");
		// We need a way to create these from the platform or something?
		
		DocumentSequenceConnector conn = new DocumentSequenceConnector();
		conn.initialize(config);

		// 
		long start = new Date().getTime();
		conn.start();
		conn.shutdown();
		long delta = new Date().getTime() - start;
		System.out.println("Took " + delta + " ms.");
//		System.out.println("Ok.. now sleep,.. and start back up");
//		Thread.sleep(10000);
//		
//		ConnectorConfiguration config2 = new ConnectorConfiguration();
//		config2.setWorkflow("ingest");
//		config2.setStringParam("stop", "100000");
//		// We need a way to create these from the platform or something?
//		
//		DocumentSequenceConnector conn2 = new DocumentSequenceConnector();
//		conn2.initialize(config2);
//		
//		conn2.start();
//		// Shutdown flushes the workflows used.
//		conn2.shutdown();
//
		// System.out.println("Connector finished");
	}
}
