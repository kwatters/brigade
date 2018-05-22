package com.kmwllc.brigade.config.json;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.ConfigFactory;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.stage.Stage;
import com.kmwllc.brigade.stage.StageExceptionMode;

import java.io.*;
import java.util.*;

import static com.kmwllc.brigade.config.ConfigFactory.JSON;

/**
 * Implementation of WorkflowConfig for Json representation.  This uses Jackson for
 * serialization/deserialization.  This extends WorkflowConfig's generic parameter
 * to enforce a collections of stages that use the Json representation (JsonStageConfig).
 */
public class JsonWorkflowConfig implements WorkflowConfig<JsonStageConfig> {
  @JsonProperty("stages")
  private List<JsonStageConfig> stageConfigs;

  private String name;
  private int numWorkerThreads;
  private int queueLength;

  @JsonProperty("stageExecutionMode")
  private String stageExceptionModeClass;

  @JsonIgnore
  private StageExceptionMode stageExceptionMode;

  private Map<String, Object> config;

  @JsonIgnore
  private List<Stage> stages;

  @JsonIgnore
  private ObjectMapper om;

  public JsonWorkflowConfig() {
    config = new HashMap<>();
    stageConfigs = new ArrayList<>();
    stages = new ArrayList<>();
    try {
      om = ((JsonConfigFactory) ConfigFactory.instance(JSON)).getObjectMapper();
    } catch (ConfigException e) {
      e.printStackTrace();
    }
  }

  public JsonWorkflowConfig(String name, int numWorkerThreads, int queueLength) {
    this();
    this.name = name;
    this.numWorkerThreads = numWorkerThreads;
    this.queueLength = queueLength;
  }

  @Override
  public List<JsonStageConfig> getStageConfigs() {
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
  public void addStageConfig(JsonStageConfig stage) {
    stageConfigs.add(stage);
  }

  @Override
  public List<Stage> getStages() {
    return stages;
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
  @JsonAnyGetter
  public Map<String, Object> getConfig() {
    return config;
  }

  @JsonAnySetter
  public void put(String key, Object val) {
    config.put(key, val);
  }

  @Override
  public void serialize(Writer w) throws ConfigException {
    try {
      om.writerWithDefaultPrettyPrinter().writeValue(w, this);
    } catch (IOException e) {
      throw new ConfigException("Error serializing config", e);
    }
  }

  @Override
  public WorkflowConfig deserialize(Reader r) throws ConfigException {
    try {
      return om.readValue(r, JsonWorkflowConfig.class);
    } catch (IOException e) {
      throw new ConfigException("Error deserializing config", e);
    }
  }
}
