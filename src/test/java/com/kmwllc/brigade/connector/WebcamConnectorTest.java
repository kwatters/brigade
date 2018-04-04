package com.kmwllc.brigade.connector;

import org.junit.Ignore;
import org.junit.Test;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.workflow.WorkflowServer;


// this requires an external kafka server to be up and running.
@Ignore
public class WebcamConnectorTest {

  @Test
  public void testKafkaConnector() throws Exception {
    WorkflowConfig wC = new WorkflowConfig("testWorkflow");
    wC.setName("ingest");
    
    StageConfig s1Conf = new StageConfig();
    s1Conf.setStageClass("com.kmwllc.brigade.stage.SetStaticFieldValue");
    s1Conf.setStageName("set title");
    s1Conf.setStringParam("fieldName", "title");
    s1Conf.setStringParam("value", "Hello World.");
    
    // let's add a dl4j stage!
    StageConfig s2Conf = new StageConfig();
    s2Conf.setStageClass("com.kmwllc.brigade.stage.Deeplearning4j");
    
    wC.addStage(s1Conf);
    wC.addStage(s2Conf);
    
    WorkflowServer ws = WorkflowServer.getInstance();
    ws.addWorkflow(wC);
   

    // config the connector
    ConnectorConfig cfg = new ConnectorConfig("cam1", "com.kmwllc.brigade.connector.WebcamConnector");
    cfg.setIntegerParam("cameraIndex", 0);
    ConnectorServer.getInstance().addConnector(cfg);
    // TODO: push the workflow name into the connector config
    ConnectorServer.getInstance().getConnector("cam1").setWorkflowName("ingest");
    ConnectorServer.getInstance().getConnector("cam1").start();
    
    // let's see what happens.
    

  }
}
