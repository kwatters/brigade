package com.kmwllc.brigade;

import com.kmwllc.brigade.config.BrigadeProperties;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.StageExceptionMode;
import com.kmwllc.brigade.util.DocRetainer;
import com.kmwllc.brigade.utils.BrigadeRunner;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

import static com.kmwllc.brigade.stage.StageExceptionMode.NEXT_STAGE;
import static com.kmwllc.brigade.util.HasFieldWithValue.hasFieldWithValue;
import static org.junit.Assert.*;

public class StageExceptionTest {

  private InputStream getStream(String path) {
    return StageExceptionTest.class.getClassLoader().getResourceAsStream(path);
  }

  @Test
  public void testNextDocMode() {
    try {
      BrigadeProperties bp = new BrigadeProperties();
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/stageExceptionConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/stageExceptionWorkflow.json"), bp);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();

      DocRetainer docRetainer = (DocRetainer) cc.findConnectorListener("DocRetainer");
      List<Document> docs = docRetainer.getDocs();

      assertEquals(3, docs.size());
      assertThat(docs.get(0), hasFieldWithValue("name", "YNNOS"));
      assertThat(docs.get(1), hasFieldWithValue("name", "BLAZE"));
      assertThat(docs.get(2), hasFieldWithValue("name", "TTAM"));

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testNextStageMode() {
    try {
      BrigadeProperties bp = new BrigadeProperties();
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/stageExceptionConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/stageExceptionWorkflow.json"), bp);
      wc.setStageExceptionMode(NEXT_STAGE);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();

      DocRetainer docRetainer = (DocRetainer) cc.findConnectorListener("DocRetainer");
      List<Document> docs = docRetainer.getDocs();

      assertEquals(3, docs.size());
      assertThat(docs.get(0), hasFieldWithValue("name", "YNNOS"));
      assertThat(docs.get(1), hasFieldWithValue("name", "EZALB"));
      assertThat(docs.get(2), hasFieldWithValue("name", "TTAM"));

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testStopWorkflowMode() {
    ConnectorConfig cc = null;
    try {
      BrigadeProperties bp = new BrigadeProperties();
      cc = ConnectorConfig.fromStream(getStream("conf/stageExceptionConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/stageExceptionWorkflow2.json"), bp);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      //fail();
    }

    DocRetainer docRetainer = (DocRetainer) cc.findConnectorListener("DocRetainer");
    List<Document> docs = docRetainer.getDocs();

    assertEquals(2, docs.size());
    assertThat(docs.get(0), hasFieldWithValue("name", "YNNOS"));
    assertThat(docs.get(1), hasFieldWithValue("name", "BLAZE"));


  }
}
