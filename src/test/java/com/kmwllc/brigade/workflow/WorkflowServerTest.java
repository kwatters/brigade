package com.kmwllc.brigade.workflow;

import org.junit.Test;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.document.Document;

public class WorkflowServerTest {

	//@Test
	public void testWorkflowServer() throws ClassNotFoundException, InterruptedException {
		
		WorkflowConfig wC = createWorkflow();
		WorkflowServer ws = WorkflowServer.getInstance();
		try {
			ws.addWorkflow(wC);
		} catch (Exception e) {
			e.printStackTrace();
		}

		Document d = createDocument();

		WorkflowMessage wm = new WorkflowMessage();
		wm.setType("add");
		wm.setWorkflow("ingest");
		wm.setDoc(d);
		
		ws.processMessage(wm);

		// TODO: something better..
		try {
			ws.flush(wm.getWorkflow());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private Document createDocument() {
		Document d = new Document("1");
		return d;
	}
	
	public WorkflowConfig createWorkflow() throws ClassNotFoundException {
		// Create a workflow config
		WorkflowConfig wC = new WorkflowConfig("testWorkflow");
		wC.setName("ingest");
		
		StageConfig s1Conf = new StageConfig();
		s1Conf.setStageClass("com.kmwllc.brigade.stage.SetStaticFieldValue");
		s1Conf.setStageName("set title");
		s1Conf.setStringParam("fieldName", "title");
		s1Conf.setStringParam("value", "Hello World.");

		StageConfig s2Conf = new StageConfig();
		s2Conf.setStageName("Solr Sender");
		s2Conf.setStageClass("com.kmwllc.brigade.stage.SendToSolr");
		s2Conf.setStringParam("solrUrl", "http://localhost:8983/solr");
		s2Conf.setStringParam("idField", "id");

		wC.addStage(s1Conf);
		wC.addStage(s2Conf);

		return wC;
	}
}
