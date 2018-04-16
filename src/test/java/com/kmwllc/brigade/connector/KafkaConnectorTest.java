package com.kmwllc.brigade.connector;

import java.util.Arrays;
import java.util.HashMap;

import com.kmwllc.brigade.config.json.JsonConnectorConfig;
import com.kmwllc.brigade.config.json.JsonStageConfig;
import com.kmwllc.brigade.config.json.JsonWorkflowConfig;
import org.junit.Ignore;
import org.junit.Test;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.workflow.WorkflowServer;


// this requires an external kafka server to be up and running.
@Ignore
public class KafkaConnectorTest {

  @Test
  public void testKafkaConnector() throws Exception {
    WorkflowConfig wC = new JsonWorkflowConfig("testWorkflow", 10, 100);
    //wC.setName("ingest");
    
    StageConfig s1Conf = new JsonStageConfig("set title", "com.kmwllc.brigade.stage.SetStaticFieldValue");
    s1Conf.setStringParam("fieldName", "title");
    s1Conf.setStringParam("value", "Hello World.");
    WorkflowServer ws = WorkflowServer.getInstance();
    ws.addWorkflow(wC, new HashMap<>());
   

    // config the connector
    ConnectorConfig cfg = new JsonConnectorConfig("kafka", "com.kmwllc.brigade.connector.KafkaConsumerConnector");
    cfg.setStringParam("bootstrapServers", "phobos:9092");
    cfg.setListParam("topics", Arrays.asList("test_topic"));
    ConnectorServer.getInstance().addConnector(cfg);
    // TODO: push the workflow name into the connector config
    ConnectorServer.getInstance().getConnector("kafka").setWorkflowName("ingest");
    ConnectorServer.getInstance().getConnector("kafka").start();
    
    // let's see what happens.
    

  }
}
