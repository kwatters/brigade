package com.kmwllc.brigade.config.legacyXML;

import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.ConfigFactory;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;

import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public class LegacyXMLConfigFactory implements ConfigFactory {
  private static Map<String, String> legacyReplacements = new HashMap<>();

  static {
    legacyReplacements.put("com.kmwllc.brigade.config.ConnectorConfig",
            "com.kmwllc.brigade.config.legacyXML.LegacyXMLConnectorConfig");
    legacyReplacements.put("com.kmwllc.brigade.config.WorkflowConfig",
            "com.kmwllc.brigade.config.legacyXML.LegacyXMLWorkflowConfig");
    legacyReplacements.put("com.kmwllc.brigade.config.StageConfig",
            "com.kmwllc.brigade.config.legacyXML.LegacyXMLStageConfig");
  }

  @Override
  public WorkflowConfig deserializeWorkflow(Reader r) throws ConfigException {
    return new LegacyXMLWorkflowConfig().deserialize(r);
  }

  @Override
  public ConnectorConfig deserializeConnector(Reader r) throws ConfigException {
    return new LegacyXMLConnectorConfig().deserialize(r);
  }

  @Override
  public WorkflowConfig deserializeWorkflow(String s) throws ConfigException {
    for (Map.Entry<String, String> e : legacyReplacements.entrySet()) {
      s = s.replaceAll(e.getKey(), e.getValue());
    }
    return deserializeWorkflow(new StringReader(s));
  }

  @Override
  public ConnectorConfig deserializeConnector(String s) throws ConfigException {
    for (Map.Entry<String, String> e : legacyReplacements.entrySet()) {
      s = s.replaceAll(e.getKey(), e.getValue());
    }
    return deserializeConnector(new StringReader(s));
  }
}
