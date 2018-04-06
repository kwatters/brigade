package com.kmwllc.brigade.config2.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.brigade.config2.ConfigException;
import com.kmwllc.brigade.config2.ConfigFactory;
import com.kmwllc.brigade.config2.ConnectorConfig2;
import com.kmwllc.brigade.config2.WorkflowConfig2;

import java.io.Reader;

public class JsonConfigFactory implements ConfigFactory {
    @Override
    public WorkflowConfig2 deserializeWorkflow(Reader r) throws ConfigException {
        return new JsonWorkflowConfig2().deserialize(r);
    }

    @Override
    public ConnectorConfig2 deserializeConnector(Reader r) throws ConfigException {
        return new JsonConnectorConfig2().deserialize(r);
    }

    public ObjectMapper getObjectMapper() {
        ObjectMapper om = new ObjectMapper();

        om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        return om;
    }
}
