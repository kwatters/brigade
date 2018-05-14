package com.kmwllc.brigade;

import com.kmwllc.brigade.concurrency.DumpDocReader;
import com.kmwllc.brigade.config.BrigadeProperties;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.event.DefaultConnectorListener;
import com.kmwllc.brigade.stage.AbstractStage;
import com.kmwllc.brigade.stage.Stage;
import com.kmwllc.brigade.stage.StageFailure;
import com.kmwllc.brigade.utils.BrigadeRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.kmwllc.brigade.util.HasFieldWithValue.hasFieldWithValue;
import static org.junit.Assert.*;

public class ConnectorListenerTest {

  private InputStream getStream(String path) {
    return ConnectorListenerTest.class.getClassLoader().getResourceAsStream(path);
  }

  private File testFile = new File("cond-exec-output.txt");

  private Stage exceptionOnSonny = new AbstractStage() {
    @Override
    public void startStage(StageConfig config) {

    }

    @Override
    public void stopStage() {

    }

    @Override
    public void flush() {

    }

    @Override
    public List<Document> processDocument(Document doc) throws Exception {
      if (doc.getFirstValueAsString("author").equals("Sonny")) {
        throw new Exception("This name (Sonny) is not allowed");
      }
      return null;
    }
  };

  @Before
  @After
  public void cleanupTestFile() {
    testFile.delete();
  }

  @Test
  public void testConnectorEventListenerStatic() {
    System.clearProperty("name");
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/connectorListenerConnector.json"), bp);
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/connectorListenerWorkflow.json"), bp);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    List<Document> docs = new DumpDocReader().read(testFile);
    assertEquals(1, docs.size());
    assertThat(docs.get(0), hasFieldWithValue("name", "Matt"));
    assertNull(System.getProperty("name"));
  }

  @Test
  public void testConnectorEventListenerDynamic() {
    System.clearProperty("name");
    try {
      BrigadeProperties bp = BrigadeProperties.fromStream(getStream("conf/condExec2.properties"), true);
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/connectorListenerConnector2.json"), bp);
      cc.addConnectorListener(new DefaultConnectorListener() {
        @Override
        public void connectorBegin(ConnectorConfig cc) {
          System.setProperty("name", "Blaze");
        }

        @Override
        public void connectorEnd() {
          System.setProperty("name", "Matt");
        }
      });
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/connectorListenerWorkflow.json"), bp);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    List<Document> docs = new DumpDocReader().read(testFile);
    assertEquals(1, docs.size());
    assertThat(docs.get(0), hasFieldWithValue("name", "Blaze"));
    assertEquals("Matt", System.getProperty("name"));
  }

  @Test
  public void testOnDocument() {
    List<Document> docsInMem = new ArrayList<>();
    try {
      // empty properties
      BrigadeProperties bp = new BrigadeProperties();
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/csv-connector.json"), bp);
      cc.addConnectorListener(new DefaultConnectorListener() {
        @Override
        public void onDocument(Document doc) {
          docsInMem.add(doc);
        }
      });
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/empty-workflow.json"), bp);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    assertEquals(4, docsInMem.size());
    assertThat(docsInMem.get(3), hasFieldWithValue("author", "Blaze"));
    assertThat(docsInMem.get(1), hasFieldWithValue("title", "doc 1"));
  }

  @Test
  public void testDocumentFail() {
    List<Document> docsInMem = new ArrayList<>();
    List<String> errDocsInMem = new ArrayList<>();
    final StringBuilder exHolder = new StringBuilder();
    try {
      BrigadeProperties bp = new BrigadeProperties();
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/csv-connector.json"), bp);
      cc.addConnectorListener(new DefaultConnectorListener() {
        @Override
        public void onDocument(Document doc) {
          docsInMem.add(doc);
        }

        @Override
        public void docFail(String docId, List<StageFailure> failures) {
          errDocsInMem.add(docId);
          exHolder.append(failures.get(0).getException().getMessage());
        }
      });
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/empty-workflow.json"), bp);
      wc.appendStage(exceptionOnSonny);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    assertEquals(4, docsInMem.size());
    assertEquals(1, errDocsInMem.size());
    assertEquals(exHolder.toString(), "This name (Sonny) is not allowed");
    assertThat(docsInMem.get(1), hasFieldWithValue("author", "Matt"));
  }

  @Test
  public void testDocumentListener() {
    final AtomicInteger completeNo = new AtomicInteger();
    final AtomicInteger errNo = new AtomicInteger();
    final StringBuilder exHolder = new StringBuilder();
    try {
      BrigadeProperties bp = new BrigadeProperties();
      ConnectorConfig cc = ConnectorConfig.fromStream(getStream("conf/csv-connector.json"), bp);
      cc.addConnectorListener(new DefaultConnectorListener() {
        @Override
        public void docComplete(String docId) {
          completeNo.incrementAndGet();
        }

        @Override
        public void docFail(String docId, List<StageFailure> failures) {
          errNo.incrementAndGet();
          exHolder.append(failures.get(0).getException().getMessage());
        }
      });
      WorkflowConfig wc = WorkflowConfig.fromStream(getStream("conf/empty-workflow.json"), bp);
      wc.appendStage(exceptionOnSonny);
      BrigadeRunner br = new BrigadeRunner(bp, cc, wc);
      br.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    assertEquals(3, completeNo.get());
    assertEquals(1, errNo.get());
    assertEquals(exHolder.toString(), "This name (Sonny) is not allowed");
  }
}
