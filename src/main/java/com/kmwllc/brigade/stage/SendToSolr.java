package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.connector.ConnectorServer;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This stage will convert an MRL document to a solr document. It then batches
 * those documents and sends the batches to solr. Upon a flush call any partial
 * batches will be flushed.
 * 
 * @author kwatters
 *
 */
public class SendToSolr extends AbstractStage {

  public final static Logger log = LoggerFactory.getLogger(SendToSolr.class.getCanonicalName());
  private String idField = "id";
  private String fieldsField = "fields";
  private boolean addFieldsField = true;
  private SolrClient solrServer = null;
  private String solrUrl = "http://localhost:8983/solr/collection1";
  private boolean issueCommit = true;
  private boolean versionLatest = false;

  private String versionSource;

  private int batchSize = 100;
  // private LinkedBlockingQueue<SolrInputDocument> batch = new
  // LinkedBlockingQueue<SolrInputDocument>();
  // Synchronized list. needed for thread safety.
  private List<SolrInputDocument> batch = Collections.synchronizedList(new ArrayList<SolrInputDocument>());

  // private String basicAuthUser = null;
  // private String basicAuthPass = null;

  // Batch size +/-

  @Override
  public void startStage(StageConfig config) {
    solrUrl = config.getProperty("solrUrl", solrUrl);
    issueCommit = config.getBoolParam("issueCommit", new Boolean(issueCommit));
    batchSize = Integer.valueOf(config.getIntegerParam("batchSize", batchSize));
    versionLatest = config.getBoolParam("versionLatest", new Boolean(versionLatest));
    versionSource = config.getProperty("versionSource");

    // basicAuthUser = config.getStringParam("basicAuthUser", basicAuthUser);
    // basicAuthPass = config.getStringParam("basicAuthPass", basicAuthPass);

    // Initialize a connection to the solr server on startup.
    if (solrServer == null) {
      // TODO: support an embeded solr instance
      log.info("Connecting to Solr at {}", solrUrl);
      // set credentials.

      // if (basicAuthUser != null) {
      // DefaultHttpClient httpClient = new DefaultHttpClient();
      // httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, new
      // UsernamePasswordCredentials(basicAuthUser, basicAuthPass));
      // create solr server with client.
      // solrServer = new HttpSolrServer( solrUrl , httpClient);
      // } else {
      solrServer = new HttpSolrClient(solrUrl);
      // }
    } else {
      log.info("Solr instance already created.");
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    SolrInputDocument solrDoc = new SolrInputDocument();

    // set the id field on the solr doc
    String docId = doc.getId();
    for (String fieldName : doc.getFields()) {
      if (fieldName == null) {
        // TODO: this shouldn't happen!
        log.warn("Null field name, this shouldn't happen! doc: {}",doc.getId());
        continue;
      }
      for (Object value : doc.getField(fieldName)) {
        if (value != null) {
          solrDoc.addField(fieldName, value);
        } else {
          // TODO: add something as a place holder if so configured.
        }
      }
      if (addFieldsField) {
        solrDoc.addField(fieldsField, fieldName);
      }
    }
    // prevent id field duplicate values.
    // remove the id field if it was set,
    solrDoc.removeField(idField);
    // make sure we add it back
    solrDoc.setField(idField, docId);
    // I guess we have the full document, we should send it
    // ArrayList<SolrInputDocument> solrDocs = new
    // ArrayList<SolrInputDocument>();

    try {
      synchronized (batch) {
        batch.add(solrDoc);
        if (batch.size() >= batchSize) {
          // System.out.println("Solr Server Flush Batch...");
          // you are blocking?
          try {
            solrServer.add(batch);
          } catch (HttpSolrClient.RemoteSolrException re) {
            log.warn("Swallow runtime exception: {}", re);
          }
          log.info("Sending Batch to Solr. Size: {}", batch.size());
          // System.out.println("Solr batch sent..");
          // batch.clear();
          batch = Collections.synchronizedList(new ArrayList<SolrInputDocument>());
        } else {
          // System.out.println("Batch Size " + batch.size());
        }
      }
    } catch (SolrServerException e) {
      log.warn("Solr Server Exception: {}", e);
    } catch (IOException e) {
      log.warn("IO Exception: {}", e);
    }
    // TODO: NO COMMITS HERE!
    // solrServer.commit();
    return null;

  }

  @Override
  public void stopStage() {
    // make sure to flush before we shutdown
    flush();
  }

  public synchronized void flush() {

    // Is this where I should flush the last batch?
    if (solrServer != null && batch.size() > 0) {
      try {
        log.info("flushing last batch. Size: {}", batch.size());
        solrServer.add(batch);
      } catch (SolrServerException e) {
        log.warn("Solr Exception flushing batch: {}", e);
        e.printStackTrace();
      } catch (IOException e) {
        log.warn("IO Exception flushing batch: {}", e);
      } finally {
        batch.clear();
      }
    }
    // TODO: should we commit on flush?
    try {
      if (issueCommit) {
        log.info("Committing solr");
        solrServer.commit();
      }
    } catch (SolrServerException e) {
      log.warn("Solr Exception Committing: {}", e);
    } catch (IOException e) {
      log.warn("IO Exception Committing: {}", e);
    }
    // TODO: call the super flush?
    // super.flush();

    if (versionLatest) {
      String versionSourceClause = "";
      if (versionSource != null) {
        versionSourceClause = String.format("source:%s AND ", versionSource);
      }
      // Grab the connector that sent this document and see what it's current job id is.
      String versionId = ConnectorServer.getInstance().getConnector(versionSource).getJobId();
      String notVersionid = String.format("%s!version_id:%s", versionSourceClause, versionId);
      try {
        log.info("Issuing a delete by query for {}", notVersionid);
        solrServer.deleteByQuery(notVersionid);
        solrServer.commit();
      } catch (SolrServerException e) {
        log.warn("Solr Server Exception while running version latest DBQ: {}", e);
      } catch (IOException e) {
        log.warn("IO Exception while runnign version latest DBQ: {}", e);
      }
      //flush();
    }
  }
}