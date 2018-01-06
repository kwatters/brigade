package com.kmwllc.brigade.config;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

/**
 * Created by matt on 3/28/17.
 */
public class JsonHandlerConfig extends Config {
    public static JsonHandlerConfig fromXML(String xml) {
        // TODO: move this to a utility to serialize/deserialize the config objects.
        // TODO: should override on the impl classes so they return a properly
        // cast config.
        Object o = (new XStream(new StaxDriver())).fromXML(xml);
        return (JsonHandlerConfig) o;
    }
}
