package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.connector.ConnectorServer;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
  private boolean useEmbedded = false;
  private String embeddedConfDir;
  private String embeddedCollection;

  private String versionSource;

  private int batchSize = 100;
  // Synchronized list. needed for thread safety.
  private List<SolrInputDocument> batch = Collections.synchronizedList(new ArrayList<SolrInputDocument>());

  @Override
  public void startStage(StageConfig config) {
    solrUrl = config.getProperty("solrUrl", solrUrl);
    issueCommit = config.getBoolParam("issueCommit", new Boolean(issueCommit));
    batchSize = Integer.valueOf(config.getIntegerParam("batchSize", batchSize));
    versionLatest = config.getBoolParam("versionLatest", new Boolean(versionLatest));
    versionSource = config.getProperty("versionSource");
    useEmbedded = config.getBoolParam("useEmbedded", useEmbedded);
    embeddedConfDir = config.getStringParam("embeddedConfigDir");
    embeddedCollection = config.getStringParam("embeddedCollection");

    // Initialize a connection to the solr server on startup.
    if (solrServer == null) {
      // TODO: support an embeded solr instance
      log.info("Connecting to Solr at {}", solrUrl);
      if (useEmbedded) {
        solrServer = createEmbedded(embeddedConfDir, embeddedCollection);
      } else {
    	solrServer = new HttpSolrClient.Builder().withBaseSolrUrl(solrUrl).build();
      }
    } else {
      log.info("Solr instance already created.");
    }
  }

  private EmbeddedSolrServer createEmbedded(String confDir, String coll){
    Path solrHome = Paths.get(confDir);
    CoreContainer container = CoreContainer.createAndLoad(solrHome);
    container.load();
    return new EmbeddedSolrServer(container, coll);
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

    try {
      synchronized (batch) {
        batch.add(solrDoc);
        if (batch.size() >= batchSize) {
          try {
            solrServer.add(batch);
          } catch (HttpSolrClient.RemoteSolrException re) {
            log.warn("Swallow runtime exception: {}", re);
          }
          log.info("Sending Batch to Solr. Size: {}", batch.size());
          batch = Collections.synchronizedList(new ArrayList<SolrInputDocument>());
        }
      }
    } catch (SolrServerException e) {
      log.warn("Solr Server Exception: {}", e);
    } catch (IOException e) {
      log.warn("IO Exception: {}", e);
    }
    return null;

  }

  @Override
  public void stopStage() {
    // make sure to flush before we shutdown
    flush();
  }

  public synchronized void flush() {

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
    }


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
    }

    if (useEmbedded){
      try {
        solrServer.close();
      } catch (IOException e) {
        log.warn("IO Exception while closing embedded solr server");
      }
    }
  }
}