package com.kmwllc.brigade.stage;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class EmbeddedSolrTest {

  private CoreContainer container;
  private EmbeddedSolrServer server;

  @Before
  public void setUp() throws Exception {
    FileUtils.deleteDirectory(new File("src/test/resources/test-solr/testing/data"));

    Path solrHome = Paths.get("src/test/resources/test-solr");
    container = CoreContainer.createAndLoad(solrHome);
    // container = new CoreContainer(testSolrHome);
    container.load();

    server = new EmbeddedSolrServer(container, "testing");
  }

  @After
  public void tearDown() throws IOException {
    server.close();
  }

  @Test
  public void testEmbeddedSolr() throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();

    // ** Let's index a document into our embedded server

    SolrInputDocument newDoc = new SolrInputDocument();
    newDoc.addField("title", "Test Document 1");
    newDoc.addField("id", "doc-1");
    newDoc.addField("text", "Hello world!");
    server.add(newDoc);
    server.commit();

    // ** And now let's query for it

    params.set("q", "*:*");
    QueryResponse qResp = server.query(params);

    SolrDocumentList docList = qResp.getResults();
    assertEquals(1, docList.getNumFound());
    SolrDocument doc = docList.get(0);
    assertEquals("Test Document 1", doc.getFirstValue("title"));
  }
}
