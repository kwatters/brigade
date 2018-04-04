package com.kmwllc.brigade.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;

public class KafkaConsumerConnector extends AbstractConnector {

  private String bootstrapServers = "localhost:9092";
  private String groupId = "test";
  private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
  private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
  private List<String> topics;
  private String docIdPrefix = "kafka_";
  private int pollInterval = 100;
  // TODO: how do templetize.. need to have generics here i think..
  private KafkaConsumer<String, String> consumer;
  // TODO: mark volitile?
  private boolean isCrawling = false;

  @Override
  public void setConfig(ConnectorConfig config) {
    bootstrapServers = config.getStringParam("bootstrapServers", bootstrapServers);
    groupId = config.getStringParam("groupId", groupId);
    keyDeserializer = config.getStringParam("keyDeserializer", keyDeserializer);
    valueDeserializer = config.getStringParam("valueDeserializer", valueDeserializer);
    topics = config.getListParam("topics");
    docIdPrefix = config.getStringParam("docIdPrefix", docIdPrefix);
  }



  @Override
  public void initialize() {
    // TODO: expose more properties. or let them just pass through to the brigade config.
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("group.id", groupId);
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", keyDeserializer);
    props.put("value.deserializer", valueDeserializer);
    consumer = new KafkaConsumer<>(props);
    consumer.subscribe(topics);
    while (isCrawling) {
      ConsumerRecords<String, String> records = consumer.poll(pollInterval);
      for (ConsumerRecord<String, String> record : records) {
        Document d = new Document(record.key());
        d.setField("data", record.value());
        d.setField("topic", record.topic());
        d.setField("offset",record.offset());
        d.setField("partition",record.partition());
        d.setField("timestamp",record.timestamp());
        feed(d);
      }
    }
  }

  @Override
  public void startCrawling() throws Exception {
    while (ConnectorState.RUNNING.equals(getConnectorState())) {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        Document d = new Document(record.key());
        d.setField("data", record.value());
        d.setField("topic", record.topic());
        d.setField("offset",record.offset());
        d.setField("partition",record.partition());
        d.setField("timestamp",record.timestamp());
        // TODO: maybe add headers?
        // record.headers()
        System.err.println("Feeding document : " + d.getId() + " Value: " + record.value());
        
        feed(d);
      }
    }
    log.info("Connector state {}",getState());
  }

  @Override
  public void stopCrawling() {
    // TODO: maybe add a checkpoint or something..  and some semaphore to let us know that we've actually stopped.
    setState(ConnectorState.STOPPED);
    // shutdown the consumer
    consumer.close();
    // send a flush once the consumer is closed
    try {
      flush();
    } catch (Exception e) {
      log.warn("Error flushing last batch. {}", e);
    }
  }

}
