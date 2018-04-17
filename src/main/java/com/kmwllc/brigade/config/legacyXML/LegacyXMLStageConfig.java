package com.kmwllc.brigade.config.legacyXML;

import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.StageConfig;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

/**
 * A connector configuration that at a minimum takes a name of the connector
 * and the implementing class for that connector.
 *
 * @author kwatters
 */
public class LegacyXMLStageConfig implements StageConfig {

    private String stageName;
    private String stageClass;
    private Map<String, Object> config;

    public LegacyXMLStageConfig() {
        config = new HashMap<>();
    }

    public LegacyXMLStageConfig(String stageName, String stageClass) {
        this();
        this.stageClass = stageClass;
        this.stageName = stageName;
    }

    @Override
    public String getStageName() {
        return stageName;
    }

    @Override
    public String getStageClass() {
        return stageClass;
    }

    @Override
    public Map<String, Object> getConfig() {
        return config;
    }

    @Override
    public void serialize(Writer w) throws ConfigException {
       (new XStream(new StaxDriver())).toXML(this, w);
    }

    @Override
    public StageConfig deserialize(Reader r) throws ConfigException {
        Object o = (new XStream(new StaxDriver())).fromXML(r);
        return (StageConfig) o;
    }
}
