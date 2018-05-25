package com.kmwllc.brigade;

import com.kmwllc.brigade.concurrency.DumpDocReader;
import com.kmwllc.brigade.config.BrigadeProperties;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.Repeat;
import com.kmwllc.brigade.stage.Stage;
import com.kmwllc.brigade.utils.BrigadeRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import static com.kmwllc.brigade.util.HasFieldWithValue.hasFieldWithValue;
import static org.junit.Assert.*;

public class DynamicConfigTest {

  private File testFile = new File("cond-exec-output.txt");

  private InputStream getStream(String path){
    return DynamicConfigTest.class.getClassLoader().getResourceAsStream(path);
  }

  @Before
  @After
  public void cleanupTestFile() {
    testFile.delete();
  }

  @Test
  public void testPlain() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/condExecConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/condExecWorkflow.json"), bp);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    List<Document> docs = new DumpDocReader().read(testFile);
    assertEquals(2, docs.size());
    assertThat(docs.get(0), hasFieldWithValue("name", "BLAZE"));
    assertThat(docs.get(1), hasFieldWithValue("name", "SONNY"));
  }

  @Test
  public void testAddStage() {
    try {
      Repeat repeatStage = new Repeat();
      repeatStage.setFieldName("name");
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/condExecConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/condExecWorkflow.json"), bp);
      // has to go before the DumpDoc stage
      wc.insertStage(repeatStage, wc.getStages().size() - 2);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    List<Document> docs = new DumpDocReader().read(testFile);
    assertEquals(2, docs.size());
    assertThat(docs.get(0), hasFieldWithValue("name", "BLAZE BLAZE"));
    assertThat(docs.get(1), hasFieldWithValue("name", "SONNY SONNY"));
  }

  @Test
  public void testRemoveStage() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/condExecConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/condExecWorkflow.json"), bp);
      wc.removeStage("maybeShout");
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    List<Document> docs = new DumpDocReader().read(testFile);
    assertEquals(2, docs.size());
    assertThat(docs.get(0), hasFieldWithValue("name", "Blaze"));
    assertThat(docs.get(1), hasFieldWithValue("name", "Sonny"));
  }
}
