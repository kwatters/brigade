package com.kmwllc.brigade;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.RegexExtractor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by matt on 3/31/17.
 */
public class TPRegexTest {

    @Test
    public void testTPCapture1() {
        String input = "|t=abc|p=xyz|";

        StageConfig stageConfig = new StageConfig("test", "test");
        stageConfig.setStringParam("inputField", "text");
        stageConfig.setStringParam("outputField", "text");
        stageConfig.setStringParam("regex", "\\|t=(.*?)\\|+(?:p=(.*?)\\|)?");
        stageConfig.setBoolParam("multiFieldCapture", true);
        List<String> keepGroups = new ArrayList<>();
        keepGroups.add("1");
        keepGroups.add("2");
        stageConfig.setListParam("keepGroups", keepGroups);

        RegexExtractor st = new RegexExtractor();
        st.startStage(stageConfig);
        Document testDoc = new Document("abc");
        testDoc.setField("text", input);
        st.processDocument(testDoc);

        assertEquals(2, testDoc.getField("text").size());
        String output1 = (String) testDoc.getField("text").get(0);
        assertEquals("abc", output1);
        String output2 = (String) testDoc.getField("text").get(1);
        assertEquals("xyz", output2);
    }

    @Test
    public void testTPCapture2() {
        String input = "|t=abc||";

        StageConfig stageConfig = new StageConfig("test", "test");
        stageConfig.setStringParam("inputField", "text");
        stageConfig.setStringParam("outputField", "text");
        stageConfig.setStringParam("regex", "\\|t=(.*?)\\|+(?:p=(.*?)\\|)?");
        stageConfig.setBoolParam("multiFieldCapture", true);
        List<String> keepGroups = new ArrayList<>();
        keepGroups.add("1");
        keepGroups.add("2");
        stageConfig.setListParam("keepGroups", keepGroups);
        RegexExtractor st = new RegexExtractor();
        st.startStage(stageConfig);
        Document testDoc = new Document("abc");
        testDoc.setField("text", input);
        st.processDocument(testDoc);

        assertEquals(1, testDoc.getField("text").size());
        String output1 = (String) testDoc.getField("text").get(0);
        assertEquals("abc", output1);
    }
}
