package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.BrigadeProperties;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.document.Document;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

import static com.kmwllc.brigade.utils.BrigadeUtils.runKeepingDocs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by matt on 3/22/17.
 */
public class CondExecTest {
  private InputStream getStream(String path) {
    return CondExecTest.class.getClassLoader().getResourceAsStream(path);
  }
  
  @Test
  public void testProps1() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec1.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/condExecConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/condExecWorkflow.json"), bp);
      List<Document> docs = runKeepingDocs(bp, cc, wc);
      assertEquals(2, docs.size());
      assertEquals("Blaze", docs.get(0).getField("name").get(0));
      assertEquals("Sonny", docs.get(1).getField("name").get(0));
  
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
  
  @Test
  public void testProps2() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/condExecConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/condExecWorkflow.json"), bp);
      List<Document> docs = runKeepingDocs(bp, cc, wc);
      assertEquals(2, docs.size());
      assertEquals("BLAZE", docs.get(0).getField("name").get(0));
      assertEquals("SONNY", docs.get(1).getField("name").get(0));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
  
  @Test
  public void testProps3() {
    System.setProperty("skip", "false");
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec1.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/condExecConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/condExecWorkflow2.json"), bp);
      List<Document> docs = runKeepingDocs(bp, cc, wc);
      assertEquals(2, docs.size());
      assertEquals("BLAZE", docs.get(0).getField("name").get(0));
      assertEquals("SONNY", docs.get(1).getField("name").get(0));
      System.clearProperty("skip");
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
  
  @Test
  public void testEnabled1() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec1.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/condExecConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/condExecWorkflow2.json"), bp);
      List<Document> docs = runKeepingDocs(bp, cc, wc);
      assertEquals(2, docs.size());
      assertEquals("BLAZE", docs.get(0).getField("name").get(0));
      assertEquals("SONNY", docs.get(1).getField("name").get(0));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
  
  @Test
  public void testEnabled2() {
    System.setProperty("imShouting", "false");
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec1.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/condExecConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/condExecWorkflow2.json"), bp);
      List<Document> docs = runKeepingDocs(bp, cc, wc);
      assertEquals(2, docs.size());
      assertEquals("Blaze", docs.get(0).getField("name").get(0));
      assertEquals("Sonny", docs.get(1).getField("name").get(0));
      System.clearProperty("imShouting");
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
  
  @Test
  public void testSkipIfField() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec1.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/condExecConnector2.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/condExecWorkflow2.json"), bp);
      List<Document> docs = runKeepingDocs(bp, cc, wc);
      assertEquals(2, docs.size());
      assertEquals("Blaze", docs.get(0).getField("name").get(0));
      assertEquals("SONNY", docs.get(1).getField("name").get(0));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
