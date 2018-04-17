package com.kmwllc.brigade.connector;

import java.util.Date;
import java.util.HashMap;

import com.kmwllc.brigade.config.json.JsonConnectorConfig;
import com.kmwllc.brigade.config.json.JsonStageConfig;
import com.kmwllc.brigade.config.json.JsonWorkflowConfig;
import org.junit.Test;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.workflow.WorkflowServer;

public class DocumentSequenceConnectorTest {

	public WorkflowConfig createWorkflowConfig() throws ClassNotFoundException {
		// Create a workflow config
		WorkflowConfig wC = new JsonWorkflowConfig("testWorkflow", 10, 10);
//		wC.setName("ingest");
		
		StageConfig s1Conf = new JsonStageConfig("set title", "com.kmwllc.brigade.stage.SetStaticFieldValue");
		s1Conf.setStringParam("fieldName", "title");
		s1Conf.setStringParam("value", "Hello World.");
		
		StageConfig s2Conf = new JsonStageConfig("set title", "com.kmwllc.brigade.stage.SetStaticFieldValue");
		s2Conf.setStringParam("fieldName", "text");
		s2Conf.setStringParam("value", "Welcome to Brigade.");
		
		StageConfig s3Conf = new JsonStageConfig("Solr Sender", "com.kmwllc.brigade.stage.SendToSolr");
		s3Conf.setStringParam("solrUrl", "http://localhost:8983/solr");
		s3Conf.setStringParam("idField", "id");

		wC.addStage(s1Conf);
		wC.addStage(s2Conf);
		wC.addStage(s3Conf);
		// Create a workflow
		
		return wC;
	}

	//@Test
	public void testConnector() throws ClassNotFoundException, InterruptedException {
		// We should initialize the workflow server..
		
		WorkflowConfig wC = createWorkflowConfig();
		
		WorkflowServer ws = WorkflowServer.getInstance();
		try {
			ws.addWorkflow(wC, new HashMap<>());
		} catch (Exception e) {
			e.printStackTrace();
		}

		ConnectorConfig config = new JsonConnectorConfig("testconn1", DocumentSequenceConnector.class.getName());
		// config.setWorkflow("ingest");
		config.setStringParam("stop", "100000");
		// We need a way to create these from the platform or something?
		
		DocumentSequenceConnector conn = new DocumentSequenceConnector();
		conn.setConfig(config);
		conn.initialize();

		// 
		long start = new Date().getTime();
		conn.start();
		// conn.shutdown();
		long delta = new Date().getTime() - start;
		System.out.println("Took " + delta + " ms.");
//		System.out.println("Ok.. now sleep,.. and start back up");
//		Thread.sleep(10000);
//		
//		ConnectorConfiguration config = new ConnectorConfiguration();
//		config.setWorkflow("ingest");
//		config.setStringParam("stop", "100000");
//		// We need a way to create these from the platform or something?
//		
//		DocumentSequenceConnector conn2 = new DocumentSequenceConnector();
//		conn2.initialize(config);
//		
//		conn2.start();
//		// Shutdown flushes the workflows used.
//		conn2.shutdown();
//
		// System.out.println("Connector finished");
	}
}
