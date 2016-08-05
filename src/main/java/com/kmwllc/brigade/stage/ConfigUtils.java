package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

public class ConfigUtils {

  public static String toXML(Configuration c) {
    String xml = null;
    xml = initXStream().toXML(c);
    return xml;
  }

  public static XStream initXStream() {
    XStream xstream = new XStream(new StaxDriver());
    xstream.alias("stage", StageConfig.class);
    xstream.alias("workflow", WorkflowConfig.class);
    return xstream;
  }

}
