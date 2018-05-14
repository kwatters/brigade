package com.kmwllc.brigade.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kmwllc.brigade.event.ConnectorListener;
import com.kmwllc.brigade.utils.FieldNameMapper;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.kmwllc.brigade.utils.BrigadeUtils.getConfigAsString;
import static com.kmwllc.brigade.utils.BrigadeUtils.getConfigFactory;

public interface ConnectorConfig extends Config<ConnectorConfig>, ConfigBuilder<ConnectorConfig> {
  String getConnectorName();

  String getConnectorClass();

  @JsonProperty("fieldNameMappers")
  List<String> getFieldNameMapperClasses();

  @JsonIgnore
  List<FieldNameMapper> getFieldNameMappers();

  @JsonProperty("connectorListeners")
  List<String> getConnectorListenerClasses();

  @JsonIgnore
  List<ConnectorListener> getConnectorListeners();

  class EmptyConnectorConfig implements ConnectorConfig {
    @Override
    public String getConnectorName() {
      return null;
    }

    @Override
    public String getConnectorClass() {
      return null;
    }

    @Override
    public List<String> getFieldNameMapperClasses() {
      return null;
    }

    @Override
    public List<FieldNameMapper> getFieldNameMappers() {
      return null;
    }

    @Override
    public List<String> getConnectorListenerClasses() {
      return null;
    }

    @Override
    public List<ConnectorListener> getConnectorListeners() {
      return null;
    }

    @Override
    public Map<String, Object> getConfig() {
      return null;
    }

    @Override
    public void serialize(Writer w) throws ConfigException {

    }

    @Override
    public ConnectorConfig deserialize(Reader r) throws ConfigException {
      return null;
    }
  }

  default ConnectorConfig buildInternal(InputStream in, Optional<BrigadeProperties> properties)
          throws ConfigException {
    Optional<String> connectorString = getConfigAsString(in, properties);

    if (connectorString.isPresent()) {
      String cs = connectorString.get();
      ConfigFactory configFactory = getConfigFactory(cs);
      ConnectorConfig cc = configFactory.deserializeConnector(cs);

      for (String fieldMapperClassName : cc.getFieldNameMapperClasses()) {
        try {
          Class<?> fieldMapperClass =
                  ConnectorConfig.class.getClassLoader().loadClass(fieldMapperClassName);
          FieldNameMapper fnm = (FieldNameMapper) fieldMapperClass.getDeclaredConstructor().newInstance();
          cc.getFieldNameMappers().add(fnm);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException
                | InvocationTargetException e) {
          throw new ConfigException("Could not instantiate field name mapper");
        }
      }

      for (String connectorListenerClassName : cc.getConnectorListenerClasses()) {
        try {
          Class<?> connectorListenerClass = ConnectorConfig.class.getClassLoader().loadClass(connectorListenerClassName);
          ConnectorListener l = (ConnectorListener) connectorListenerClass.getDeclaredConstructor().newInstance();
          cc.getConnectorListeners().add(l);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException
                | InvocationTargetException e) {
          throw new ConfigException("Could not instantiate connector listener");
        }
      }

      return cc;
    } else {
      throw new ConfigException("Could not get connector serialization");
    }
  }

  default void addConnectorListener(ConnectorListener l) {
    getConnectorListeners().add(l);
  }

  default void removeConnectorListener(String name) {
    ConnectorListener cl = findConnectorListener(name);
    if (cl != null) {
      getConnectorListeners().remove(cl);
    }
  }

  default ConnectorListener findConnectorListener(String name) {
    return getConnectorListeners().stream().filter(l -> l.getName().equals(name)).findFirst().orElse(null);
  }

  default ConnectorListener getConnectorListener(String name) {
    return findConnectorListener(name);
  }

  default void addFieldNameMapper(FieldNameMapper fnm) {
    getFieldNameMappers().add(fnm);
  }

  default void removeFieldNameMapper(String name) {
    FieldNameMapper fnm = findFieldNameMapper(name);
    if (fnm != null) {
      getFieldNameMappers().remove(fnm);
    }
  }

  default FieldNameMapper findFieldNameMapper(String name) {
    return getFieldNameMappers().stream().filter(f -> f.getName().equals(name)).findFirst().orElse(null);
  }

  @Override
  default ConnectorConfig buildFromFile(String fileName, Optional<BrigadeProperties> properties)
          throws ConfigException, FileNotFoundException {
    return buildInternal(new FileInputStream(fileName), properties);
  }

  @Override
  default ConnectorConfig buildFromStream(InputStream in, Optional<BrigadeProperties> properties)
          throws ConfigException {
    return buildInternal(in, properties);
  }

  static ConnectorConfig fromFile(String fileName)
          throws FileNotFoundException, ConfigException {
    return new EmptyConnectorConfig().buildFromFile(fileName, Optional.empty());
  }

  static ConnectorConfig fromFile(String fileName, BrigadeProperties bp)
          throws FileNotFoundException, ConfigException {
    return new EmptyConnectorConfig().buildFromFile(fileName, Optional.of(bp));
  }

  static ConnectorConfig fromStream(InputStream in) throws ConfigException {
    return new EmptyConnectorConfig().buildFromStream(in, Optional.empty());
  }

  static ConnectorConfig fromStream(InputStream in, BrigadeProperties bp) throws ConfigException {
    return new EmptyConnectorConfig().buildFromStream(in, Optional.of(bp));
  }
}
