package com.kmwllc.brigade.workflow;

import com.kmwllc.brigade.config.json.JsonStageConfig;
import com.kmwllc.brigade.config.json.JsonWorkflowConfig;
import org.slf4j.Logger;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;

import java.util.HashMap;

public class WorkflowTest {

  public final static Logger log = LoggerFactory.getLogger(WorkflowTest.class.getCanonicalName());

  //@Test
	public void testWorkflow() throws ClassNotFoundException, InterruptedException {


		// Create a workflow config
		WorkflowConfig wC = new JsonWorkflowConfig("testWorkflow", 10, 100);

		StageConfig s1Conf = new JsonStageConfig("set title", "com.kmwllc.brigade.stage.SetStaticFieldValue");
		s1Conf.setStringParam("fieldName", "title");
		s1Conf.setStringParam("value", "Hello World.");

		StageConfig s2Conf = new JsonStageConfig("Solr Sender", "com.kmwllc.brigade.stage.SendToSolr");
		s2Conf.setStringParam("solrUrl", "http://localhost:8983/solr");
		s2Conf.setStringParam("idField", "id");

		wC.addStageConfig(s1Conf);
		wC.addStageConfig(s2Conf);

		// Create a workflow
		Workflow w = new Workflow(wC, new HashMap<>());
	  try {
		  w.initialize();
	  } catch (Exception e) {
		  e.printStackTrace();
	  }

	  // Create a document to be processed
		Document d = new Document("1");
		try {
			w.processDocument(d);
		} catch (InterruptedException e) {
		  log.warn("Interrupted exception: {}", e);
		}
		

		System.out.println("Processed...");

		while(true) {
			// Serve...
			Thread.sleep(100000);
		}
	}
}
