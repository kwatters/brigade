package com.kmwllc.brigade;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.DictionaryLookup;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by matt on 4/17/17.
 */
public class DictionaryResolverTest {

    @Test
    public void test1() {
        List<String> inputs = new ArrayList<>();
        inputs.add("a");
        inputs.add("c");
        inputs.add("b");

        StageConfig stageConfig = getStageConfig();

        Document testDoc = getDocument(inputs, stageConfig);

        ArrayList<Object> output = testDoc.getField("output");
        assertEquals(6, output.size());
        assertEquals("z", output.get(0));
        assertEquals("y", output.get(1));
        assertEquals("w", output.get(2));
        assertEquals("v", output.get(3));
        assertEquals("u", output.get(4));
        assertEquals("x", output.get(5));
    }

    @Test
    public void test2() {
        List<String> inputs1 = new ArrayList<>();
        inputs1.add("a");
        inputs1.add("b");
        List<String> inputs2 = new ArrayList<>();
        inputs2.add("c");
        List<String> inputs3 = new ArrayList<>();
        inputs3.add("b");
        inputs3.add("c");
        DictionaryLookup dl = new DictionaryLookup();

        StageConfig stageConfig = new StageConfig("test", "test");
        String dictFile = "conf/test-dict.csv";
        Map<String, String> ioMap = new HashMap<>();
        ioMap.put("input1", "output1");
        ioMap.put("input2", "output2");
        ioMap.put("input3", "output3");
        stageConfig.setStringParam("dictionaryFile", dictFile);
        stageConfig.setMapParam("ioMap", ioMap);
        dl.startStage(stageConfig);
        Document doc = new Document("123");
        for (String i : inputs1) {
            doc.addToField("input1", i);
        }
        for (String i : inputs2) {
            doc.addToField("input2", i);
        }
        for (String i : inputs3) {
            doc.addToField("input3", i);
        }
        dl.processDocument(doc);
        assertEquals(3, doc.getField("output1").size());
        assertEquals(true, doc.getField("output1").contains("z"));
        assertEquals(3, doc.getField("output2").size());
        assertEquals(true, doc.getField("output2").contains("w"));
        assertEquals(4, doc.getField("output3").size());
        assertEquals(true, doc.getField("output3").contains("x"));
    }


    private Document getDocument(List<String> inputs, StageConfig stageConfig) {
        DictionaryLookup lookup = new DictionaryLookup();
        lookup.startStage(stageConfig);
        Document testDoc = new Document("abc");
        for (String i : inputs) {
            testDoc.addToField("input", i);
        }
        lookup.processDocument(testDoc);
        return testDoc;
    }

    private StageConfig getStageConfig() {
        StageConfig stageConfig = new StageConfig("test", "test");
        String dictFile = "conf/test-dict.csv";
        Map<String, String> ioMap = new HashMap<>();
        ioMap.put("input", "output");
        stageConfig.setStringParam("dictionaryFile", dictFile);
        stageConfig.setMapParam("ioMap", ioMap);
        return stageConfig;
    }
}
