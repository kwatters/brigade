package com.kmwllc.brigade.workflow;

import com.kmwllc.brigade.config.json.JsonStageConfig;
import com.kmwllc.brigade.config.json.JsonWorkflowConfig;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.document.Document;

import java.util.HashMap;

public class WorkflowServerTest {

	//@Test
	public void testWorkflowServer() throws ClassNotFoundException, InterruptedException {
		
		WorkflowConfig wC = createWorkflow();
		WorkflowServer ws = WorkflowServer.getInstance();
		try {
			ws.addWorkflow(wC, new HashMap<>());
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
		WorkflowConfig wC = new JsonWorkflowConfig("testWorkflow", 10, 100);
		//wC.setName("ingest");
		
		StageConfig s1Conf = new JsonStageConfig("set title", "com.kmwllc.brigade.stage.SetStaticFieldValue");
		s1Conf.setStringParam("fieldName", "title");
		s1Conf.setStringParam("value", "Hello World.");

		StageConfig s2Conf = new JsonStageConfig("Solr Sender", "com.kmwllc.brigade.stage.SendToSolr");
		s2Conf.setStringParam("solrUrl", "http://localhost:8983/solr");
		s2Conf.setStringParam("idField", "id");

		wC.addStageConfig(s1Conf);
		wC.addStageConfig(s2Conf);

		return wC;
	}
}
