package com.kmwllc.brigade.config;

import com.kmwllc.brigade.event.ConnectorListener;
import com.kmwllc.brigade.utils.FieldNameMapper;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.kmwllc.brigade.utils.BrigadeUtils.getConfigAsString;
import static com.kmwllc.brigade.utils.BrigadeUtils.getConfigFactory;

/**
 * Interface for Connector configurations.  Also has static methods to build that can be called on
 * the interface: e.g. ConnectorConfig.fromFile().  The actual building is handled by the ConfigFactory
 * which builds config objects based on detecting the format from the file or stream.<br/><br/>
 * ConnectorConfig supports having fieldNameMappers and connectorListeners added and removed dynamically.
 */
public interface ConnectorConfig extends Config<ConnectorConfig>, ConfigBuilder<ConnectorConfig> {
  /**
   * Get name of connector
   * @return Name of connector
   */
  String getConnectorName();

  /**
   * Get fully-qualified class name of the connector
   * @return Fully-qualified class name of the connecetor
   */
  String getConnectorClass();

  /**
   * Get fully-qualified class names for the FieldMapers
   * @return fully-qualified class names for the FieldMapers
   */
  List<String> getFieldNameMapperClasses();

  /**
   * Get instances of FieldMappers used by this connector.
   * @return instances of FieldMappers used by this connector.
   */
  List<FieldNameMapper> getFieldNameMappers();

  /**
   * Get fully-qualified class names for the ConnectorListener classes
   * @return fully-qualified class names for the ConnectorListener classes
   */
  List<String> getConnectorListenerClasses();

  /**
   * Get instances of ConnectorListeners used by this connector.
   * @return instances of ConnectorListeners used by this connector.
   */
  List<ConnectorListener> getConnectorListeners();

  // this inline class is needed to support the polymorphic build methods while allowing
  // the ability to call them through static method on the base class
  // We may wish to refactor to an abstract BaseConnectorConfig that is extended by implementors
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

  /**
   * Should be called internally only.  Move to private method once we get to Java 9.
   * Deserializes connector config from the referenced stream using appropriate ConfigFactory.
   * Uses reflection to instantiate any fieldMappers and/or connectorListeners specified in
   * the config.
   * @param in Stream containing connector config
   * @param properties If present, use properties to expand variables in connector config
   * @return Populated ConnectorConfig object
   * @throws ConfigException If could not be instantiated
   */
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

  /**
   * Add a ConnectorListener
   * @param l ConnectorListener instance
   */
  default void addConnectorListener(ConnectorListener l) {
    getConnectorListeners().add(l);
  }

  /**
   * Remove ConnectorListener having specified name
   * @param name Name of ConnectorListener to remove
   */
  default void removeConnectorListener(String name) {
    ConnectorListener cl = findConnectorListener(name);
    if (cl != null) {
      getConnectorListeners().remove(cl);
    }
  }

  /**
   * Find connector with specified name.  This is intended as an internal convenience method.  When we move
   * to Java 9, we can make this a private method.  Folks should call getConnectorListener() (which uses this method
   * internally) instead.
   * @param name Name of the ConnectorListener
   * @return ConnectorListener corresponding to name
   */
  default ConnectorListener findConnectorListener(String name) {
    return getConnectorListeners().stream().filter(l -> l.getName().equals(name)).findFirst().orElse(null);
  }

  /**
   * Get connector with specified name.  People should use this method instead of findConnectorListener() which
   * this method calls behind the scenes.
   * @param name Name of the ConnectorListener
   * @return ConnectorListener corresponding to name
   */
  default ConnectorListener getConnectorListener(String name) {
    return findConnectorListener(name);
  }

  /**
   * Add a FieldNameMapper instance programmatically.
   * @param fnm FieldNameMapper instance
   */
  default void addFieldNameMapper(FieldNameMapper fnm) {
    getFieldNameMappers().add(fnm);
  }

  /**
   * Remove FieldNameMapper having specified name
   * @param name Name of the FieldNameMapper to remove
   */
  default void removeFieldNameMapper(String name) {
    FieldNameMapper fnm = findFieldNameMapper(name);
    if (fnm != null) {
      getFieldNameMappers().remove(fnm);
    }
  }

  /**
   * Find FieldNameMapper with specified name.  This is intended as an internal method only.  When this is moved
   * to Java 9, we should make it a private method.
   * @param name Name of the FieldNameMapper to find
   * @return FieldNameMapper instance
   */
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

  /**
   * Build a ConnectorConfig from the specified file.  Variable expansion will not occur
   * @param fileName Path to file
   * @return Populated ConnectorConfig
   * @throws FileNotFoundException if the file could not be opened
   * @throws ConfigException if there was an exception while instantiating the ConnectorConfig
   */
  static ConnectorConfig fromFile(String fileName)
          throws FileNotFoundException, ConfigException {
    return new EmptyConnectorConfig().buildFromFile(fileName, Optional.empty());
  }

  /**
   * Build a ConnectorConfig from the specified file.  Perform variable expansion against the specified
   * BrigadeProperties instance
   * @param fileName Path to file
   * @param bp BrigadeProperties instance to validate against
   * @return Populated ConnectorConfig
   * @throws FileNotFoundException if the file could not be opened
   * @throws ConfigException if there was an exception while instantiating the ConnectorConfig
   */
  static ConnectorConfig fromFile(String fileName, BrigadeProperties bp)
          throws FileNotFoundException, ConfigException {
    return new EmptyConnectorConfig().buildFromFile(fileName, Optional.of(bp));
  }

  /**
   * Build a ConnectorConfig from the specified stream.  No variable expansion will be performed
   * @param in Stream to build from
   * @return A populated ConnectorConfig
   * @throws ConfigException if there was an exception while instantiating the ConnectorConfig
   */
  static ConnectorConfig fromStream(InputStream in) throws ConfigException {
    return new EmptyConnectorConfig().buildFromStream(in, Optional.empty());
  }

  /**
   * Build a ConnectorConfig from the specified stream.  Perform variable expansion against the specified
   * BrigadeProperties instance
   * @param in Stream to build from
   * @param bp BrigadeProperties instance against which to perform variable expansion
   * @return A populated ConnectorConfig
   * @throws ConfigException if there was an exception while instantiating the ConnectorConfig
   */
  static ConnectorConfig fromStream(InputStream in, BrigadeProperties bp) throws ConfigException {
    return new EmptyConnectorConfig().buildFromStream(in, Optional.of(bp));
  }
}
