package com.kmwllc.brigade.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public interface ConfigBuilder<T> {
  T buildFromFile(String fileName, Optional<BrigadeProperties> properties) throws IOException, ConfigException, ClassNotFoundException;
  T buildFromStream(InputStream in, Optional<BrigadeProperties> properties) throws ConfigException, IOException, ClassNotFoundException;
}
