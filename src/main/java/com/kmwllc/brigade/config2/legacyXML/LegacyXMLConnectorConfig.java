package com.kmwllc.brigade.config2.legacyXML;

import com.kmwllc.brigade.config2.ConfigException;
import com.kmwllc.brigade.config2.ConnectorConfig2;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

/**
 * A connector configuration that at a minimum takes a name of the connector
 * and the implementing class for that connector.
 *
 * @author kwatters
 */
public class LegacyXMLConnectorConfig implements ConnectorConfig2 {

    private String connectorName;
    private String connectorClass;
    private Map<String, Object> config;

    public LegacyXMLConnectorConfig() {
        config = new HashMap<>();
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
    public Map<String, Object> getConfig() {
        return config;
    }

    @Override
    public void serialize(Writer w) throws ConfigException {
       (new XStream(new StaxDriver())).toXML(this, w);
    }

    @Override
    public ConnectorConfig2 deserialize(Reader r) throws ConfigException {
        Object o = (new XStream(new StaxDriver())).fromXML(r);
        return (ConnectorConfig2) o;
    }
}
