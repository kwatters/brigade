package com.kmwllc.brigade.config2.json;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config2.ConfigException;
import com.kmwllc.brigade.config2.ConfigFactory;
import com.kmwllc.brigade.config2.StageConfig2;
import com.kmwllc.brigade.config2.WorkflowConfig2;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.kmwllc.brigade.config2.ConfigFactory.JSON;

public class JsonWorkflowConfig2 implements WorkflowConfig2<JsonStageConfig2> {
    private List<JsonStageConfig2> stages;
    private String name;
    private int numWorkerThreads;
    private int queueLength;
    private Map<String, Object> config;

    @JsonIgnore
    private ObjectMapper om;

    public JsonWorkflowConfig2() {
        config = new HashMap<>();
        stages = new ArrayList<>();
        try {
            om = ((JsonConfigFactory)ConfigFactory.instance(JSON)).getObjectMapper();
        } catch (ConfigException e) {
            e.printStackTrace();
        }
    }

    public JsonWorkflowConfig2(String name, int numWorkerThreads, int queueLength) {
        this();
        this.name = name;
        this.numWorkerThreads = numWorkerThreads;
        this.queueLength = queueLength;
    }

    @Override
    public List<JsonStageConfig2> getStages() {
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
    public void addStage(JsonStageConfig2 stage) {
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
    public WorkflowConfig2 deserialize(Reader r) throws ConfigException {
        try {
            return om.readValue(r, JsonWorkflowConfig2.class);
        } catch (IOException e) {
            throw new ConfigException("Error deserializing config", e);
        }
    }
}
