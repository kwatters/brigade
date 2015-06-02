package com.kmwllc.brigade.workflow;

import org.junit.Test;

import com.kmwllc.brigade.config.StageConfiguration;
import com.kmwllc.brigade.config.WorkflowConfiguration;
import com.kmwllc.brigade.document.Document;

public class WorkflowServerTest {

	@Test
	public void testWorkflowServer() throws ClassNotFoundException, InterruptedException {
		
		WorkflowConfiguration wC = createWorkflow();
		WorkflowServer ws = WorkflowServer.getInstance();
		ws.addWorkflow(wC);

		Document d = createDocument();

		WorkflowMessage wm = new WorkflowMessage();
		wm.setType("add");
		wm.setWorkflow("ingest");
		wm.setDoc(d);
		
		ws.processMessage(wm);

		// TODO: something better..
		ws.flush(wm.getWorkflow());
		
	}

	private Document createDocument() {
		Document d = new Document("1");
		return d;
	}
	
	public WorkflowConfiguration createWorkflow() throws ClassNotFoundException {
		// Create a workflow config
		WorkflowConfiguration wC = new WorkflowConfiguration();
		wC.setName("ingest");
		
		StageConfiguration s1Conf = new StageConfiguration();
		s1Conf.setStageClass("com.kmwllc.brigade.stage.SetStaticFieldValue");
		s1Conf.setStageName("set title");
		s1Conf.setStringParam("fieldName", "title");
		s1Conf.setStringParam("value", "Hello World.");

		StageConfiguration s2Conf = new StageConfiguration();
		s2Conf.setStageName("Solr Sender");
		s2Conf.setStageClass("com.kmwllc.brigade.stage.SendToSolr");
		s2Conf.setStringParam("solrUrl", "http://localhost:8983/solr");
		s2Conf.setStringParam("idField", "id");

		wC.addStageConfig(s1Conf);
		wC.addStageConfig(s2Conf);

		return wC;
	}
}
