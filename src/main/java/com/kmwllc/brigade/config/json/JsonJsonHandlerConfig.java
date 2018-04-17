package com.kmwllc.brigade.config.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.JsonHandlerConfig;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

public class JsonJsonHandlerConfig implements JsonHandlerConfig {
  private Map<String, Object> config;

  @JsonIgnore
  private ObjectMapper om;

  public JsonJsonHandlerConfig() {
    config = new HashMap<>();
  }

  @Override
  public Map<String, Object> getConfig() {
    return config;
  }

  @Override
  public void serialize(Writer w) throws ConfigException {
    try {
      om.writeValue(w, this);
    } catch (IOException e) {
      throw new ConfigException("Error serializing config", e);
    }
  }

  @Override
  public JsonHandlerConfig deserialize(Reader r) throws ConfigException {
    try {
      return om.readValue(r, JsonHandlerConfig.class);
    } catch (IOException e) {
      throw new ConfigException("Error deserializing config", e);
    }
  }
}
