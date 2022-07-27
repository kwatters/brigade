package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This stage will convert an MRL document to a solr document. It then batches
 * those documents and sends the batches to solr. Upon a flush call any partial
 * batches will be flushed.
 * 
 * @author kwatters
 *
 */
public class SendToElastic extends AbstractStage {

  public final static Logger log = LoggerFactory.getLogger(SendToElastic.class);
  
  private String elasticHost = "localhost";
  private int elasticPort = 9300;
  private TransportClient elasticClient;
  private String indexName = "kmwgraph";
  private String indexType = "doc";
  
  @Override
  public void startStage(StageConfig config) {
    elasticHost = config.getProperty("elasticUrl", elasticHost);
    elasticPort = config.getIntegerParam("elasticPort", elasticPort);
    
    try {
      // TODO: support multiple hosts
      elasticClient = new PreBuiltTransportClient(Settings.EMPTY)
          .addTransportAddress(new TransportAddress(InetAddress.getByName(elasticHost), elasticPort));
    } catch (UnknownHostException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    indexName = config.getStringParam("indexName", indexName);
    indexType = config.getStringParam("indexType", indexType);
  }

  @Override
  public List<Document> processDocument(Document doc) {
    Map<String, ArrayList<Object>> docData = doc.getData();
    String docId = doc.getId();
    IndexResponse response = elasticClient.prepareIndex(indexName, indexType).setId(docId).setSource(docData).get();  
    return null;
  }

  @Override
  public void stopStage() {
    // make sure to flush before we shutdown
    flush();
    elasticClient.close();
  }

  public synchronized void flush() {
    // TODO: ? implement batching?  commits/etc?
  }
}