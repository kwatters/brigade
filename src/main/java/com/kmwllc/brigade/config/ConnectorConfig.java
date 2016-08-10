package com.kmwllc.brigade.config;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

/**
 * A connector configuration that at a minimum takes a name of the connector
 * and the implementing class for that connector.
 * 
 * @author kwatters
 *
 */
public class ConnectorConfig extends Config {

  private final String connectorName;
  private final String connectorClass;
  
  public ConnectorConfig(String connectorName, String connectorClass) {
    this.connectorName = connectorName;
    this.connectorClass = connectorClass;
  }

  public String getConnectorName() {
    return connectorName;
  }

  public String getConnectorClass() {
    return connectorClass;
  }

  public static ConnectorConfig fromXML(String xml) {
    // TODO: move this to a utility to serialize/deserialize the config objects.
    // TODO: should override on the impl classes so they return a properly
    // cast config.
    Object o = (new XStream(new StaxDriver())).fromXML(xml);
    return (ConnectorConfig) o;
  }

}
