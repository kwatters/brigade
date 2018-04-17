package com.kmwllc.brigade.config.json;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.ConfigFactory;
import com.kmwllc.brigade.config.StageConfig;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import static com.kmwllc.brigade.config.ConfigFactory.JSON;

public class JsonStageConfig implements StageConfig {

    @JsonProperty("name")
    private String stageName;

    @JsonProperty("type")
    private String stageClass;

    private Map<String, Object> config;

    @JsonIgnore
    private ObjectMapper om;

    public JsonStageConfig() {
        config = new HashMap<>();
        try {
            om = ((JsonConfigFactory) ConfigFactory.instance(JSON)).getObjectMapper();
        } catch (ConfigException e) {
            e.printStackTrace();
        }
    }

    public JsonStageConfig(String stageName, String stageClass) {
        this();
        this.stageClass = stageClass;
        this.stageName = stageName;
    }

    @Override
    public String getStageName() {
        return stageName;
    }

    @Override
    public String getStageClass() {
        return stageClass;
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
            om.writeValue(w, this);
        } catch (IOException e) {
            throw new ConfigException("Error serializing config", e);
        }
    }

    @Override
    public StageConfig deserialize(Reader r) throws ConfigException {
        try {
            return om.readValue(r, JsonStageConfig.class);
        } catch (IOException e) {
            throw new ConfigException("Error deserializing config", e);
        }
    }
}
