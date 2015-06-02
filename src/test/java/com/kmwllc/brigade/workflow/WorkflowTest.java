package com.kmwllc.brigade.workflow;

import org.junit.Test;

import com.kmwllc.brigade.config.StageConfiguration;
import com.kmwllc.brigade.config.WorkflowConfiguration;
import com.kmwllc.brigade.document.Document;

public class WorkflowTest {



	@Test
	public void testWorkflow() throws ClassNotFoundException, InterruptedException {

		// Create a workflow config
		WorkflowConfiguration wC = new WorkflowConfiguration();

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

		// Create a workflow
		Workflow w = new Workflow(wC);
		w.initialize();

		// Create a document to be processed
		Document d = new Document("1");
		try {
			w.processDocument(d);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		System.out.println("Processed...");

		while(true) {
			// Serve...
			Thread.sleep(100000);
		}
	}
}
