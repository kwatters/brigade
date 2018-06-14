package com.kmwllc.brigade;

import com.kmwllc.brigade.config.BrigadeProperties;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.document.Document;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

import static com.kmwllc.brigade.util.HasFieldWithValue.hasFieldWithValue;
import static com.kmwllc.brigade.utils.BrigadeUtils.runKeepingDocs;
import static org.junit.Assert.*;

public class FieldNameMapperTest {
  private InputStream getStream(String path){
    return FieldNameMapperTest.class.getClassLoader().getResourceAsStream(path);
  }

  @Test
  public void testSingleMapper() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/fieldMapConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/fieldMapWorkflow.json"), bp);
      
      List<Document> docs = runKeepingDocs(bp, cc, wc);
      assertEquals(1, docs.size());
      assertThat(docs.get(0), hasFieldWithValue("first name", "Matt"));
      assertThat(docs.get(0), hasFieldWithValue("last name", "Holford"));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    
  }

  @Test
  public void testTwoMappers() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/fieldMapConnector2.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/fieldMapWorkflow.json"), bp);
  
      List<Document> docs = runKeepingDocs(bp, cc, wc);
      assertEquals(1, docs.size());
      assertThat(docs.get(0), hasFieldWithValue("first_name", "Matt"));
      assertThat(docs.get(0), hasFieldWithValue("last_name", "Holford"));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testAddFieldMapper() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/fieldMapConnector2.json"), bp);
      cc.addFieldNameMapper(f -> f.replaceAll("[a-zA-Z]", "X"));
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/fieldMapWorkflow.json"), bp);
  
      List<Document> docs = runKeepingDocs(bp, cc, wc);
      assertEquals(1, docs.size());
      assertThat(docs.get(0), hasFieldWithValue("XXXXX_XXXX", "Matt"));
      assertThat(docs.get(0), hasFieldWithValue("XXXX_XXXX", "Holford"));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testRemoveFieldMapper() {
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/fieldMapConnector2.json"), bp);
      cc.removeFieldNameMapper("LowercaseFieldNames");
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/fieldMapWorkflow.json"), bp);
  
      List<Document> docs = runKeepingDocs(bp, cc, wc);
      assertEquals(1, docs.size());
      assertThat(docs.get(0), hasFieldWithValue("FIRST_NAME", "Matt"));
      assertThat(docs.get(0), hasFieldWithValue("LAST_NAME", "Holford"));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
