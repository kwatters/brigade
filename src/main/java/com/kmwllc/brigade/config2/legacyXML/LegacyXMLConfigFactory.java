package com.kmwllc.brigade.config2.legacyXML;

import com.kmwllc.brigade.config2.ConfigException;
import com.kmwllc.brigade.config2.ConfigFactory;
import com.kmwllc.brigade.config2.ConnectorConfig2;
import com.kmwllc.brigade.config2.WorkflowConfig2;

import java.io.Reader;

public class LegacyXMLConfigFactory implements ConfigFactory {
    @Override
    public WorkflowConfig2 deserializeWorkflow(Reader r) throws ConfigException {
        return new LegacyXMLWorkflowConfig().deserialize(r);
    }

    @Override
    public ConnectorConfig2 deserializeConnector(Reader r) throws ConfigException {
        return new LegacyXMLConnectorConfig().deserialize(r);
    }
}
