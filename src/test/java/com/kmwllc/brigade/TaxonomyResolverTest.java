package com.kmwllc.brigade;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.json.JsonStageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.TaxonomyResolver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by matt on 3/31/17.
 */
public class TaxonomyResolverTest {

    @Test
    public void testBasic() {
        List<String> inputs = new ArrayList<>();
        inputs.add("|t=c3||");
        inputs.add("|t=b2|p=a1|");
        inputs.add("|t=c3|p=b2|");
        inputs.add("|t=b2||");
        inputs.add("|t=a1||");

        StageConfig stageConfig = getStageConfig();

        Document testDoc = getDocument(inputs, stageConfig);

        ArrayList<Object> output = testDoc.getField("output");
        assertEquals(3, output.size());
        assertEquals(true, output.contains("a1"));
        assertEquals(true, output.contains("a1/b2"));
        assertEquals(true, output.contains("a1/b2/c3"));
    }

    @Test
    public void testForking() {
        List<String> inputs = new ArrayList<>();
        inputs.add("|t=c3||");
        inputs.add("|t=d3||");
        inputs.add("|t=d3|p=b2");
        inputs.add("|t=b2|p=a1|");
        inputs.add("|t=c3|p=b2|");
        inputs.add("|t=b2||");
        inputs.add("|t=a1||");

        StageConfig stageConfig = getStageConfig();

        Document testDoc = getDocument(inputs, stageConfig);

        ArrayList<Object> output = testDoc.getField("output");
        assertEquals(4, output.size());
        assertEquals(true, output.contains("a1"));
        assertEquals(true, output.contains("a1/b2"));
        assertEquals(true, output.contains("a1/b2/c3"));
        assertEquals(true, output.contains("a1/b2/d3"));
    }

    @Test
    public void testMultiRoot() {
        List<String> inputs = new ArrayList<>();
        inputs.add("|t=c3||");
        inputs.add("|t=z1||");
        inputs.add("|t=y2||");
        inputs.add("|t=y2|p=z1");
        inputs.add("|t=b2|p=a1|");
        inputs.add("|t=c3|p=b2|");
        inputs.add("|t=b2||");
        inputs.add("|t=a1||");

        StageConfig stageConfig = getStageConfig();

        Document testDoc = getDocument(inputs, stageConfig);

        ArrayList<Object> output = testDoc.getField("output");
        assertEquals(5, output.size());
        assertEquals(true, output.contains("a1"));
        assertEquals(true, output.contains("a1/b2"));
        assertEquals(true, output.contains("a1/b2/c3"));
        assertEquals(true, output.contains("z1"));
        assertEquals(true, output.contains("z1/y2"));
    }

    private Document getDocument(List<String> inputs, StageConfig stageConfig) {
        TaxonomyResolver tr = new TaxonomyResolver();
        tr.startStage(stageConfig);
        Document testDoc = new Document("abc");
        for (String i : inputs) {
            testDoc.addToField("input", i);
        }
        tr.processDocument(testDoc);
        return testDoc;
    }

    private StageConfig getStageConfig() {
        StageConfig stageConfig = new JsonStageConfig("test", "test");
        stageConfig.setStringParam("inputField", "input");
        stageConfig.setStringParam("outputField", "output");
        stageConfig.setStringParam("delimiter", "\\|");
        stageConfig.setStringParam("termPrefix", "t=");
        stageConfig.setStringParam("parentPrefix", "p=");
        return stageConfig;
    }
}
