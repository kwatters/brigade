package com.kmwllc.brigade.config.legacyXML;

import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.stage.Stage;
import com.kmwllc.brigade.stage.StageExceptionMode;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A connector configuration that at a minimum takes a name of the connector
 * and the implementing class for that connector.
 *
 * @author kwatters
 */
public class LegacyXMLWorkflowConfig implements WorkflowConfig<LegacyXMLStageConfig> {

  private String name;
  private int numWorkerThreads;
  private int queueLength;
  private Map<String, Object> config;
  private String stageExceptionModeClass;
  private StageExceptionMode stageExceptionMode;
  List<LegacyXMLStageConfig> stageConfigs;
  List<Stage> stages = new ArrayList<>();

  public LegacyXMLWorkflowConfig() {
    init();
  }

  public void init() {
    config = new HashMap<>();
    stages = new ArrayList<>();
  }

  public LegacyXMLWorkflowConfig(String name, String stageClass, int numWorkerThreads, int queueLength) {
    this();
    this.name = name;
    this.numWorkerThreads = numWorkerThreads;
    this.queueLength = queueLength;
  }

  @Override
  public Map<String, Object> getConfig() {
    return config;
  }

  @Override
  public void serialize(Writer w) throws ConfigException {
    (new XStream(new StaxDriver())).toXML(this, w);
  }

  @Override
  public WorkflowConfig deserialize(Reader r) throws ConfigException {
    Object o = (new XStream(new StaxDriver())).fromXML(r);
    LegacyXMLWorkflowConfig wc = (LegacyXMLWorkflowConfig) o;
    wc.init();
    return wc;
  }

  @Override
  public List<LegacyXMLStageConfig> getStageConfigs() {
    return stageConfigs;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getNumWorkerThreads() {
    return numWorkerThreads;
  }

  @Override
  public int getQueueLength() {
    return queueLength;
  }

  @Override
  public void addStageConfig(LegacyXMLStageConfig stage) {
    stageConfigs.add(stage);
  }

  @Override
  public String getStageExecutionModeClass() {
    return stageExceptionModeClass;
  }

  @Override
  public StageExceptionMode getStageExecutionMode() {
    return stageExceptionMode;
  }

  @Override
  public void setStageExceptionMode(StageExceptionMode stageExceptionMode) {
    this.stageExceptionMode = stageExceptionMode;
  }

  @Override
  public List<Stage> getStages() {
    return stages;
  }
}
