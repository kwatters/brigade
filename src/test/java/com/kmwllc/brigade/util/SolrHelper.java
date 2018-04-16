package com.kmwllc.brigade.util;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SolrHelper extends ExternalResource {
  private SolrClient server;
  private String solrDir;
  private String collection;
  private String dataDir;

  public SolrHelper(String solrDir, String collection, String dataDir) {
    this.solrDir = solrDir;
    this.collection = collection;
    this.dataDir = dataDir;
  }

  @Override
  protected void before() throws Throwable {
    FileUtils.deleteDirectory(new File(dataDir));
//    CoreContainer container = new CoreContainer(solrDir);
//    container.load();
//    server = new EmbeddedSolrServer(container, collection);
  }

  @Override
  protected void after() {
    try {
      server.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public SolrDocumentList query(String query) throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", query);
    QueryResponse qr = server.query(params);
    return qr.getResults();
  }
}
