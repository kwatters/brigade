package com.kmwllc.brigade.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Interface to support polymorphism in building config objects.  Currently you can build either from a physical
 * file in a filesystem or from a classpath resource.  Both methods optionally support using a BrigadeProperties
 * instance to expand variables in the configuration.
 *
 * @param <T> The type of configuration object to build
 */
public interface ConfigBuilder<T> {
  /**
   * Build a configuration object from the given physical file.
   * @param fileName Path to the file
   * @param properties Properties to use to expand variables within the configuration
   * @return A configuration object
   * @throws IOException if there was an error reading the file
   * @throws ConfigException if there was an error instantiating the configuration object
   * @throws ClassNotFoundException if there was a reflection problem
   */
  T buildFromFile(String fileName, Optional<BrigadeProperties> properties) throws IOException, ConfigException,
          ClassNotFoundException;

  /**
   * Build a configuration object from the given physical file.
   * @param in Stream to build from
   * @param properties Properties to use to expand variables within the configuration
   * @return A configuration object
   * @throws IOException if there was an error reading the stream
   * @throws ConfigException if there was an error instantiating the configuration object
   * @throws ClassNotFoundException if there was a reflection problem
   */
  T buildFromStream(InputStream in, Optional<BrigadeProperties> properties) throws ConfigException, IOException,
          ClassNotFoundException;
}
