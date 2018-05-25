package com.kmwllc.brigade.config;

import com.kmwllc.brigade.utils.BrigadeUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * BrigadeProperties is a wrapper around a String->String Map that also supports the polymorphic
 * configuration build capabilities (fromFile, fromStream).  BrigadeProperties has the further
 * capability of "bootstrapping" a set of properties where a values are derived from a previously
 * set value.  For example given the following set of properties:<br/><ul>
 * <li>filePath = /home/matt</li>
 * <li>downloadFolder = ${filePath}/downloads</li></ul><br/>
 * The second entry can be resolved to<ul>
 * <li>downloadFolder = /home/matt/downloads</li></ul><br/>
 * Once built, properties may be modified in code up until the point they are passed to a BrigadeRunner
 * and BrigadeRunner.exec() is called.  The properties may be passed to ConnectorConfig and WorkflowConfig
 * and used there to resolve parameters.  Properties in BrigadeProperties may be overridden by setting
 * a Java system property of the same name (either by calling System.setProperty() or as a -D flag when
 * invoking Brigade from the command line).
 */
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

  /**
   * Build a BrigadeProperties object from the referenced stream.  Bootstrap variable expansion using passed-in
   * instance of BrigadeProperties
   *
   * @param in Stream containing properties
   * @param bp Instance to bootstrap against
   * @return BrigadeProperties object
   * @throws IOException if error reading stream
   * @throws ConfigException if error in instantiating config
   */
  public static BrigadeProperties fromStream(InputStream in, BrigadeProperties bp)
          throws IOException, ConfigException {
    return new BrigadeProperties().buildFromStream(in, Optional.of(bp));
  }

  /**
   * Build a BrigadeProperties object from the referenced stream without bootstrapping.
   * @param in Stream containing properties
   * @return BrigadeProperties object
   * @throws IOException if error reading stream
   * @throws ConfigException if error in instantiating config
   */
  public static BrigadeProperties fromStream(InputStream in) throws IOException, ConfigException {
    return new BrigadeProperties().buildFromStream(in, Optional.empty());
  }

  /**
   * Build a BrigadeProperties object from the reference file, using referenced properties instance to
   * resolve variables.
   * @param fileName Path to the file
   * @param bp Instance to expand variables against
   * @return BrigadeProperties object
   * @throws IOException if error reading file
   * @throws ConfigException if error in instantiating config
   */
  public static BrigadeProperties fromFile(String fileName, BrigadeProperties bp)
          throws IOException, ConfigException {
    return new BrigadeProperties().buildFromFile(fileName, Optional.of(bp));
  }

  /**
   * Build a BrigadeProperties object from the referenced file without bootstrapping.
   * @param fileName Path to the file
   * @return BrigadeProperties object
   * @throws IOException if error reading file
   * @throws ConfigException if error in instantiating config
   */
  public static BrigadeProperties fromFile(String fileName) throws IOException, ConfigException {
    return new BrigadeProperties().buildFromFile(fileName, Optional.empty());
  }

  /**
   * Build a BrigadeProperties object from the referenced file.  If bootstrap=true, use the file itself to resolve
   * variable names.  This is done internally by processing the properties file twice.
   * @param fileName Path to the file
   * @param bootstrap Whether to use the file for variable expansion
   * @return BrigadeProperties object
   * @throws IOException if error reading file
   * @throws ConfigException if error in instantiating config
   */
  public static BrigadeProperties fromFile(String fileName, boolean bootstrap) throws IOException, ConfigException {
    BrigadeProperties bp = new BrigadeProperties().buildInternal(new FileInputStream(fileName), Optional.empty());
    if (!bootstrap) {
      return bp;
    }
    return new BrigadeProperties().buildInternal(new FileInputStream(fileName), Optional.of(bp));
  }

  /**
   * Build a BrigadeProperties object from the referenced stream.  If bootstrap=true, use the stream itself
   * to resolve variable names.  This is done internally by processing/serializing the stream and then using
   * the serialization to expand variables.
   * @param stream Stream containing properties
   * @param bootstrap Whether to use the stream for variable expansion
   * @return BrigadeProperties object
   * @throws IOException if error reading stream
   * @throws ConfigException if error instantiating config
   */
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
