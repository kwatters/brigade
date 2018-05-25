package com.kmwllc.brigade;

import com.kmwllc.brigade.concurrency.DumpDocReader;
import com.kmwllc.brigade.config.BrigadeProperties;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.utils.BrigadeRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import static com.kmwllc.brigade.util.HasFieldWithValue.hasFieldWithValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class FieldNameMapperTest {
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
  public void testSingleMapper() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/fieldMapConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/fieldMapWorkflow.json"), bp);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    List<Document> docs = new DumpDocReader().read(testFile);
    assertEquals(1, docs.size());
    assertThat(docs.get(0), hasFieldWithValue("first name", "Matt"));
    assertThat(docs.get(0), hasFieldWithValue("last name", "Holford"));
  }

  @Test
  public void testTwoMappers() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/fieldMapConnector2.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/fieldMapWorkflow.json"), bp);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    List<Document> docs = new DumpDocReader().read(testFile);
    assertEquals(1, docs.size());
    assertThat(docs.get(0), hasFieldWithValue("first_name", "Matt"));
    assertThat(docs.get(0), hasFieldWithValue("last_name", "Holford"));
  }

  @Test
  public void testAddFieldMapper() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/fieldMapConnector2.json"), bp);
      cc.addFieldNameMapper(f -> f.replaceAll("[a-zA-Z]", "X"));
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/fieldMapWorkflow.json"), bp);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    List<Document> docs = new DumpDocReader().read(testFile);
    assertEquals(1, docs.size());
    assertThat(docs.get(0), hasFieldWithValue("XXXXX_XXXX", "Matt"));
    assertThat(docs.get(0), hasFieldWithValue("XXXX_XXXX", "Holford"));
  }

  @Test
  public void testRemoveFieldMapper() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/fieldMapConnector2.json"), bp);
      cc.removeFieldNameMapper("LowercaseFieldNames");
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/fieldMapWorkflow.json"), bp);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    List<Document> docs = new DumpDocReader().read(testFile);
    assertEquals(1, docs.size());
    assertThat(docs.get(0), hasFieldWithValue("FIRST_NAME", "Matt"));
    assertThat(docs.get(0), hasFieldWithValue("LAST_NAME", "Holford"));
  }
}
