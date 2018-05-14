package com.kmwllc.brigade.config;

import com.kmwllc.brigade.utils.BrigadeUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BrigadeProperties extends HashMap<String, String> implements ConfigBuilder<BrigadeProperties>,
        Config<BrigadeProperties> {

  @Override
  public BrigadeProperties buildFromFile(String fileName, Optional<BrigadeProperties> props)
          throws IOException, ConfigException {
    return buildInternal(new FileInputStream(fileName), props);
  }

  @Override
  public BrigadeProperties buildFromStream(InputStream in, Optional<BrigadeProperties> props)
          throws IOException, ConfigException {
    return buildInternal(in, props);
  }

  private BrigadeProperties buildInternal(InputStream in, Optional<BrigadeProperties> props)
          throws IOException, ConfigException {
    BrigadeProperties output = deserialize(new InputStreamReader(in));
    if (props.isPresent()) {
      // Bootstrap properties file
      StringWriter sw = new StringWriter();
      output.serialize(sw);
      String propString = sw.toString();
      StrSubstitutor sub = new StrSubstitutor(props.get());
      propString = sub.replace(propString);
      output = deserialize(new StringReader(propString));
    }
    return output;
  }

  public static BrigadeProperties fromStream(InputStream in, BrigadeProperties bp)
          throws IOException, ConfigException {
    return new BrigadeProperties().buildFromStream(in, Optional.of(bp));
  }

  public static BrigadeProperties fromStream(InputStream in) throws IOException, ConfigException {
    return new BrigadeProperties().buildFromStream(in, Optional.empty());
  }

  public static BrigadeProperties fromFile(String fileName, BrigadeProperties bp)
          throws IOException, ConfigException {
    return new BrigadeProperties().buildFromFile(fileName, Optional.of(bp));
  }

  public static BrigadeProperties fromFile(String fileName) throws IOException, ConfigException {
    return new BrigadeProperties().buildFromFile(fileName, Optional.empty());
  }

  public static BrigadeProperties fromFile(String fileName, boolean bootstrap) throws IOException, ConfigException {
    BrigadeProperties bp = new BrigadeProperties().buildInternal(new FileInputStream(fileName), Optional.empty());
    if (!bootstrap) {
      return bp;
    }
    return new BrigadeProperties().buildInternal(new FileInputStream(fileName), Optional.of(bp));
  }

  public static BrigadeProperties fromStream(InputStream stream, boolean bootstrap)
          throws IOException, ConfigException {
    BrigadeProperties init = new BrigadeProperties().buildFromStream(stream, Optional.empty());
    if (bootstrap) {
      StringWriter sw = new StringWriter();
      init.serialize(sw);
      return new BrigadeProperties().buildFromStream(IOUtils.toInputStream(sw.toString(), "UTF-8"), Optional.of(init));
    } else {
      return init;
    }
  }

  @Override
  public Map<String, Object> getConfig() {
    return null;
  }

  @Override
  public void serialize(Writer w) throws ConfigException {
    for (Entry<String, String> e : entrySet()) {
      try {
        w.write(String.format("%s=%s\n", e.getKey(), e.getValue()));
      } catch (IOException ex) {
        ex.printStackTrace();
        throw new ConfigException("Could not serialize brigade properties");
      }
    }
  }

  @Override
  public BrigadeProperties deserialize(Reader r) throws ConfigException {
    try {
      return BrigadeUtils.loadPropertiesAsMap(r);
    } catch (IOException e) {
      e.printStackTrace();
      throw new ConfigException("Could not read brigade properties");
    }
  }

}
