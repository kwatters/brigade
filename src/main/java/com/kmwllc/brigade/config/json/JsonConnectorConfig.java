package com.kmwllc.brigade.config.json;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.ConfigFactory;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.event.ConnectorListener;
import com.kmwllc.brigade.utils.FieldNameMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.kmwllc.brigade.config.ConfigFactory.JSON;

public class JsonConnectorConfig implements ConnectorConfig {

    @JsonProperty("name")
    private String connectorName;

    @JsonProperty("type")
    private String connectorClass;

    private Map<String, Object> config;

    @JsonProperty("fieldNameMappers")
    private List<String> fieldNameMapperClasses;

    @JsonIgnore
    private List<FieldNameMapper> fieldNameMappers;

    @JsonProperty("connectorListeners")
    private List<String> connectorListenerClasses;

    @JsonIgnore
    private List<ConnectorListener> connectorListeners;

    @JsonIgnore
    private ObjectMapper om = new ObjectMapper();

    public JsonConnectorConfig() {
        config = new HashMap<>();
        fieldNameMapperClasses = new ArrayList<>();
        fieldNameMappers = new ArrayList<>();
        connectorListenerClasses = new ArrayList<>();
        connectorListeners = new ArrayList<>();
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
    public List<String> getFieldNameMapperClasses() {
        return fieldNameMapperClasses;
    }

    @Override
    public List<FieldNameMapper> getFieldNameMappers() {
        return fieldNameMappers;
    }

    @Override
    public List<String> getConnectorListenerClasses() {
        return connectorListenerClasses;
    }

    @Override
    public List<ConnectorListener> getConnectorListeners() {
        return connectorListeners;
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
