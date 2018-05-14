package com.kmwllc.brigade.config.legacyXML;

import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.json.JsonConnectorConfig;
import com.kmwllc.brigade.event.ConnectorListener;
import com.kmwllc.brigade.utils.FieldNameMapper;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

import java.io.*;
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
public class LegacyXMLConnectorConfig implements ConnectorConfig {

    private String connectorName;
    private String connectorClass;
    private Map<String, Object> config;
    private List<String> fieldNameMapperClasses = new ArrayList<>();
    private List<FieldNameMapper> fieldNameMappers = new ArrayList<>();
    private List<ConnectorListener> connectorListeners = new ArrayList<>();
    private List<String> connectorListenerClasses = new ArrayList<>();

    public LegacyXMLConnectorConfig() {
        init();
    }

    public void init() {
        if (config == null) {
            config = new HashMap<>();
        }
        if (fieldNameMappers == null) {
            fieldNameMappers = new ArrayList<>();
        }
        if (fieldNameMapperClasses == null) {
            fieldNameMapperClasses = new ArrayList<>();
        }
        if (connectorListeners == null) {
            connectorListeners = new ArrayList<>();
        }
        if (connectorListenerClasses == null) {
            connectorListenerClasses = new ArrayList<>();
        }
    }

    public LegacyXMLConnectorConfig(String connectorName, String connectorClass) {
        this();
        this.connectorName = connectorName;
        this.connectorClass = connectorClass;
    }

    public String getConnectorName() {
        return connectorName;
    }

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
    public Map<String, Object> getConfig() {
        return config;
    }

    @Override
    public void serialize(Writer w) throws ConfigException {
       (new XStream(new StaxDriver())).toXML(this, w);
    }

    @Override
    public ConnectorConfig deserialize(Reader r) throws ConfigException {
        Object o = (new XStream(new StaxDriver())).fromXML(r);
        LegacyXMLConnectorConfig cc = (LegacyXMLConnectorConfig) o;
        cc.init();
        return (ConnectorConfig) o;
    }
}
