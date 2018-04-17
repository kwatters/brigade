package com.kmwllc.brigade;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.json.JsonStageConfig;
import com.kmwllc.brigade.document.Document;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by matt on 3/31/17.
 */
public class StripTagsTest {

    @Test
    public void testStripTags() {
        String input = "<html>stuff <fudge:f class=\"abc\">blah</fudge:f>    <p>more\\nstuff</p> &lt;span&gt;yet more stuff" +
                "\\r\\n &lt;/span&gt; <ol><li>term1</li><li>term2</li><li>term3</li></ol></html>";
        String expectedOutput = "stuff blah more stuff yet more stuff term1 term2 term3";

        Map<String, String> ioMap = new HashMap<>();
        ioMap.put("input", "output");
        StageConfig stageConfig = new JsonStageConfig("test", "test");
        stageConfig.setMapParam("ioMap", ioMap);
        com.kmwllc.brigade.stage.StripTags fte = new com.kmwllc.brigade.stage.StripTags();
        fte.startStage(stageConfig);
        Document testDoc = new Document("abc");
        testDoc.setField("input", input);
        fte.processDocument(testDoc);

        String output = (String) testDoc.getField("output").get(0);
        assertEquals(expectedOutput, output);
    }

    @Test
    public void testStripTagsInplace() {
        String input = "<html>stuff <fudge:f class=\"abc\">blah</fudge:f>    <p>more\\nstuff</p> &lt;span&gt;yet more stuff" +
                "\\r\\n &lt;/span&gt; </html>";
        String expectedOutput = "stuff blah more stuff yet more stuff";

        Map<String, String> ioMap = new HashMap<>();
        ioMap.put("input", "");
        StageConfig stageConfig = new JsonStageConfig("test", "test");
        stageConfig.setMapParam("ioMap", ioMap);
        com.kmwllc.brigade.stage.StripTags fte = new com.kmwllc.brigade.stage.StripTags();
        fte.startStage(stageConfig);
        Document testDoc = new Document("abc");
        testDoc.setField("input", input);
        fte.processDocument(testDoc);

        String output = (String) testDoc.getField("input").get(0);
        assertEquals(expectedOutput, output);
    }
}
