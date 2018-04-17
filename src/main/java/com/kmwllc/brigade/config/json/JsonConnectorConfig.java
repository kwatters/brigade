package com.kmwllc.brigade.config.json;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.ConfigFactory;
import com.kmwllc.brigade.config.ConnectorConfig;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import static com.kmwllc.brigade.config.ConfigFactory.JSON;

public class JsonConnectorConfig implements ConnectorConfig {

    @JsonProperty("name")
    private String connectorName;

    @JsonProperty("type")
    private String connectorClass;

    private Map<String, Object> config;

    @JsonIgnore
    private ObjectMapper om = new ObjectMapper();

    public JsonConnectorConfig() {
        config = new HashMap<>();
        try {
            om = ((JsonConfigFactory) ConfigFactory.instance(JSON)).getObjectMapper();
        } catch (ConfigException e) {
            e.printStackTrace();
        }
    }

    public JsonConnectorConfig(String connectorName, String connectorClass) {
        this();
        this.connectorName = connectorName;
        this.connectorClass = connectorClass;
    }

    @Override
    public String getConnectorName() {
        return connectorName;
    }

    @Override
    public String getConnectorClass() {
        return connectorClass;
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
    public ConnectorConfig deserialize(Reader r) throws ConfigException {
        try {
            return om.readValue(r, JsonConnectorConfig.class);
        } catch (IOException e) {
            throw new ConfigException("Error deserializing config", e);
        }
    }
}
