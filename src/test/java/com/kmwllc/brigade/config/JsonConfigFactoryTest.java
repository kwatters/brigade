package com.kmwllc.brigade.config;

import com.kmwllc.brigade.config.json.JsonConnectorConfig;
import com.kmwllc.brigade.config.json.JsonStageConfig;
import com.kmwllc.brigade.config.json.JsonWorkflowConfig;
import org.junit.Test;

import java.io.StringReader;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JsonConfigFactoryTest {

    @Test
    public void testConnectorConfig() {
        ConfigFactory cf = null;
        try {
            cf = ConfigFactory.instance(ConfigFactory.JSON);
        } catch (ConfigException e) {
            e.printStackTrace();
            fail();
        }

        JsonConnectorConfig conn = new JsonConnectorConfig("testconnector", "com.xyz.TestConnector");
        conn.getConfig().put("prop1", "val1");

        StringWriter sw = new StringWriter();
        try {
            conn.serialize(sw);
        } catch (ConfigException e) {
            e.printStackTrace();
            fail();
        }
        String expected ="{\n" +
                "  \"name\" : \"testconnector\",\n" +
                "  \"type\" : \"com.xyz.TestConnector\",\n" +
                "  \"prop1\" : \"val1\"\n" +
                "}";
        assertEquals(expected, sw.toString());

        String input = "{\n" +
                "  name : \"testconnector\",\n" +
                "  type : \"com.xyz.TestConnector\",\n" +
                "  prop2 : \"val2\",\n" +
                "  prop1 : \"val1\"\n" +
                "}";
        try {
            ConnectorConfig cc = cf.deserializeConnector(new StringReader(input));
            assertEquals("testconnector", cc.getConnectorName());
            assertEquals("com.xyz.TestConnector", cc.getConnectorClass());
            assertEquals("val2", cc.getStringParam("prop2"));
            assertEquals("val1", cc.getStringParam("prop1"));
        } catch (ConfigException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testWorkflowConfig() {
        ConfigFactory cf = null;
        try {
            cf = ConfigFactory.instance(ConfigFactory.JSON);
        } catch (ConfigException e) {
            e.printStackTrace();
            fail();
        }
        JsonWorkflowConfig wf = new JsonWorkflowConfig("testWF", 10, 100);
        JsonStageConfig stage1 = new JsonStageConfig("Stage1", "com.xyz.Stage1");
        stage1.getConfig().put("s1p1", "val1");
        JsonStageConfig stage2 = new JsonStageConfig("Stage2", "com.xyz.Stage2");
        stage2.getConfig().put("s2p1", "val3");
        wf.addStageConfig(stage1);
        wf.addStageConfig(stage2);

        StringWriter sw = new StringWriter();
        try {
            wf.serialize(sw);
        } catch (ConfigException e) {
            e.printStackTrace();
            fail();
        }
        String expected = "{\n" +
                "  \"stages\" : [ {\n" +
                "    \"name\" : \"Stage1\",\n" +
                "    \"type\" : \"com.xyz.Stage1\",\n" +
                "    \"s1p1\" : \"val1\"\n" +
                "  }, {\n" +
                "    \"name\" : \"Stage2\",\n" +
                "    \"type\" : \"com.xyz.Stage2\",\n" +
                "    \"s2p1\" : \"val3\"\n" +
                "  } ],\n" +
                "  \"name\" : \"testWF\",\n" +
                "  \"numWorkerThreads\" : 10,\n" +
                "  \"queueLength\" : 100\n" +
                "}";

        String input = "{\n" +
                "  stages : [ {\n" +
                "    name : \"Stage1\",\n" +
                "    type : \"com.xyz.Stage1\",\n" +
                "    s1p2 : \"val2\",\n" +
                "    s1p1 : \"val1\"\n" +
                "  }, {\n" +
                "    name : \"Stage2\",\n" +
                "    type : \"com.xyz.Stage2\",\n" +
                "    s2p1 : \"val3\"\n" +
                "  } ],\n" +
                "  name : \"testWF\",\n" +
                "  numWorkerThreads : 10,\n" +
                "  queueLength : 100\n" +
                "}";

        try {
            WorkflowConfig wc = cf.deserializeWorkflow(new StringReader(input));
            assertEquals("testWF", wc.getName());
            assertEquals(2, wc.getStageConfigs().size());
            StageConfig s1 = (StageConfig) wc.getStageConfigs().get(0);
            assertEquals("val2", s1.getStringParam("s1p2"));
            assertEquals("val1", s1.getStringParam("s1p1"));
            assertEquals("Stage1", s1.getStageName());
            assertEquals("com.xyz.Stage1", s1.getStageClass());
            StageConfig s2 = (StageConfig) wc.getStageConfigs().get(1);
            assertEquals("val3", s2.getStringParam("s2p1"));
            assertEquals("Stage2", s2.getStageName());
            assertEquals("com.xyz.Stage2", s2.getStageClass());
        } catch (ConfigException e) {
            e.printStackTrace();
            fail();
        }
    }
}
