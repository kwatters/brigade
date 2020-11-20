package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.util.BrigadeHelper;
import com.kmwllc.brigade.util.SolrHelper;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SendToSolrTest {

  @Rule
  public final BrigadeHelper brigadeHelper = new BrigadeHelper("conf/brigade.properties",
          "conf/csv-connector.json", "conf/solr-test-workflow.json");

  @Test
  public void test() {
//
    try {
      brigadeHelper.exec();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    
    Path solrHome = Paths.get("src/test/resources/test-solr");
    CoreContainer container = container = CoreContainer.createAndLoad(solrHome);

    container.load();
    EmbeddedSolrServer server = new EmbeddedSolrServer(container, "testing");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "meow");
    QueryResponse qr = null;
    try {
      qr = server.query(params);
    } catch (SolrServerException e) {
      e.printStackTrace();
      fail();
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
    SolrDocumentList results = qr.getResults();
    ;

    assertEquals(2, results.size());
    try {
      server.close();

      FileUtils.deleteDirectory(new File("src/test/resources/test-solr/testing/data"));
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }
}
