package com.kmwllc.brigade;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.json.JsonStageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.CopyFirstValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by matt on 3/31/17.
 */
public class CopyFirstValueTest {

    @Test
    public void test() {
        List<String> input1 = new ArrayList<>();
        input1.add("v1");
        input1.add("v2");

        List<Long> input2 = new ArrayList<>();
        input2.add(123L);
        input2.add(234L);
        input2.add(345L);

        String input3 = "abc";

        StageConfig stageConfig = new JsonStageConfig("test", "test");
        Map<String, String> ioMap = new HashMap<>();
        ioMap.put("input1", "output1");
        ioMap.put("input2", "output2");
        ioMap.put("input3", "output3");
        stageConfig.setMapParam("ioMap", ioMap);

        CopyFirstValue st = new CopyFirstValue();
        st.startStage(stageConfig);
        Document testDoc = new Document("abc");
        for (String i1 : input1) {
            testDoc.addToField("input1", i1);
        }
        for (Long i2 : input2) {
            testDoc.addToField("input2", i2);
        }
        testDoc.addToField("input3", input3);
        st.processDocument(testDoc);

        assertEquals(1, testDoc.getField("output1").size());
        assertEquals("v1", testDoc.getField("output1").get(0));
        assertEquals(1, testDoc.getField("output2").size());
        assertEquals(123L, testDoc.getField("output2").get(0));
        assertEquals(1, testDoc.getField("output3").size());
        assertEquals("abc", testDoc.getField("output3").get(0));

    }
}
