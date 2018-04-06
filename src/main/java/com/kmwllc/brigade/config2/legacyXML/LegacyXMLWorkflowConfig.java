package com.kmwllc.brigade.config2.legacyXML;

import com.kmwllc.brigade.config2.ConfigException;
import com.kmwllc.brigade.config2.StageConfig2;
import com.kmwllc.brigade.config2.WorkflowConfig2;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A connector configuration that at a minimum takes a name of the connector
 * and the implementing class for that connector.
 *
 * @author kwatters
 */
public class LegacyXMLWorkflowConfig implements WorkflowConfig2<LegacyXMLStageConfig> {

    private String name;
    private int numWorkerThreads;
    private int queueLength;
    private Map<String, Object> config;
    List<LegacyXMLStageConfig> stages;

    public LegacyXMLWorkflowConfig() {
        config = new HashMap<>();
    }

    public LegacyXMLWorkflowConfig(String name, String stageClass, int numWorkerThreads, int queueLength) {
        this();
        this.name = name;
        this.numWorkerThreads = numWorkerThreads;
        this.queueLength = queueLength;
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
    public WorkflowConfig2 deserialize(Reader r) throws ConfigException {
        Object o = (new XStream(new StaxDriver())).fromXML(r);
        return (WorkflowConfig2) o;
    }

    @Override
    public List<LegacyXMLStageConfig> getStages() {
        return stages;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getNumWorkerThreads() {
        return numWorkerThreads;
    }

    @Override
    public int getQueueLength() {
        return queueLength;
    }

    @Override
    public void addStage(LegacyXMLStageConfig stage) {
        stages.add(stage);
    }
}
