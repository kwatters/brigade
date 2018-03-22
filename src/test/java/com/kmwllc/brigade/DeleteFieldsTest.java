package com.kmwllc.brigade;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.DeleteFields;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by matt on 4/17/17.
 */
public class DeleteFieldsTest {

    @Test
    public void test() {
        StageConfig config = new StageConfig("test", "test");
        List<String> toDelete = new ArrayList<>();
        toDelete.add("b");
        toDelete.add("c");
        config.setListParam("fields", toDelete);
        DeleteFields df = new DeleteFields();
        df.startStage(config);

        Document doc = new Document("123");
        doc.addToField("a", "xyz");
        doc.addToField("b", "efg");
        doc.addToField("c", "qrs");
        df.processDocument(doc);

        assertEquals("xyz", doc.getField("a").get(0));
        assertEquals(null, doc.getField("b"));
        assertEquals(null, doc.getField("c"));
    }
}
