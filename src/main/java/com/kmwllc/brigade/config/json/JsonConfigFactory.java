package com.kmwllc.brigade.config.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.ConfigFactory;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;

import java.io.Reader;

public class JsonConfigFactory implements ConfigFactory {
    @Override
    public WorkflowConfig deserializeWorkflow(Reader r) throws ConfigException {
        return new JsonWorkflowConfig().deserialize(r);
    }

    @Override
    public ConnectorConfig deserializeConnector(Reader r) throws ConfigException {
        return new JsonConnectorConfig().deserialize(r);
    }

    public ObjectMapper getObjectMapper() {
        ObjectMapper om = new ObjectMapper();

        om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        return om;
    }
}
