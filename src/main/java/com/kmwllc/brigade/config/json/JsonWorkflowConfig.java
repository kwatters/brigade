package com.kmwllc.brigade.config.json;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.ConfigFactory;
import com.kmwllc.brigade.config.WorkflowConfig;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.kmwllc.brigade.config.ConfigFactory.JSON;

public class JsonWorkflowConfig implements WorkflowConfig<JsonStageConfig> {
    private List<JsonStageConfig> stages;
    private String name;
    private int numWorkerThreads;
    private int queueLength;
    private Map<String, Object> config;

    @JsonIgnore
    private ObjectMapper om;

    public JsonWorkflowConfig() {
        config = new HashMap<>();
        stages = new ArrayList<>();
        try {
            om = ((JsonConfigFactory)ConfigFactory.instance(JSON)).getObjectMapper();
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
    public List<JsonStageConfig> getStages() {
        return stages;
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
    public void addStage(JsonStageConfig stage) {
        stages.add(stage);
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
