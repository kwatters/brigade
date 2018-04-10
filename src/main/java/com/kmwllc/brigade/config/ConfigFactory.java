package com.kmwllc.brigade.config;

import com.kmwllc.brigade.config.json.JsonConfigFactory;
import com.kmwllc.brigade.config.legacyXML.LegacyXMLConfigFactory;

import java.io.Reader;
import java.io.StringReader;

public interface ConfigFactory {
    String JSON = "json";
    String LEGACY_XML = "legacy_xml";

    static ConfigFactory instance(String type) throws ConfigException {
        switch (type) {
            case JSON:
                return new JsonConfigFactory();
            case LEGACY_XML:
                return new LegacyXMLConfigFactory();
            default:
                throw new ConfigException("Unknown ConfigFactory type");
        }
    }

    WorkflowConfig deserializeWorkflow(Reader r) throws ConfigException;
    ConnectorConfig deserializeConnector(Reader r) throws ConfigException;

    default WorkflowConfig deserializeWorkflow(String s) throws ConfigException {
        return deserializeWorkflow(new StringReader(s));
    }

    default ConnectorConfig deserializeConnector(String s) throws ConfigException {
        return deserializeConnector(new StringReader(s));
    }
}
