package com.kmwllc.brigade.config2.json;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.kmwllc.brigade.config2.ConfigException;
import com.kmwllc.brigade.config2.ConfigFactory;
import com.kmwllc.brigade.config2.ConnectorConfig2;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import static com.kmwllc.brigade.config2.ConfigFactory.JSON;

public class JsonConnectorConfig2 implements ConnectorConfig2 {

    @JsonProperty("id")
    private String connectorName;

    @JsonProperty("type")
    private String connectorClass;

    private Map<String, Object> config;

    @JsonIgnore
    private ObjectMapper om = new ObjectMapper();

    public JsonConnectorConfig2() {
        config = new HashMap<>();
        try {
            om = ((JsonConfigFactory) ConfigFactory.instance(JSON)).getObjectMapper();
        } catch (ConfigException e) {
            e.printStackTrace();
        }
    }

    public JsonConnectorConfig2(String connectorName, String connectorClass) {
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
    public ConnectorConfig2 deserialize(Reader r) throws ConfigException {
        try {
            return om.readValue(r, JsonConnectorConfig2.class);
        } catch (IOException e) {
            throw new ConfigException("Error deserializing config", e);
        }
    }
}
