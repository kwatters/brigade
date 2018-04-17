package com.kmwllc.brigade.config.legacyXML;

import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.JsonHandlerConfig;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

public class LegacyXMLJsonHandlerConfig implements JsonHandlerConfig {
  private Map<String, Object> config;

  public LegacyXMLJsonHandlerConfig() {
    config = new HashMap<>();
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
  public JsonHandlerConfig deserialize(Reader r) throws ConfigException {
    Object o = (new XStream(new StaxDriver())).fromXML(r);
    return (JsonHandlerConfig) o;
  }
}
