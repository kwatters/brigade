package com.kmwllc.brigade;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.DictionaryEE;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by matt on 4/18/17.
 */
public class DictionaryEETest {

    @Test
    public void testTrie() {
        Document d = getDocument("Dmitri  did not go to work on Martin Luther King Day, Guy", "Trie");

        assertEquals(3, d.getField("output").size());
        assertEquals(true, d.getField("output").contains("cat"));
        assertEquals(true, d.getField("output").contains("American"));
        assertEquals(true, d.getField("output").contains("Canadian"));
    }

    @Test
    public void testFST() {
        Document d = getDocument("Dmitri  did not go to work on Martin Luther King Day, Guy", "Trie");

        assertEquals(3, d.getField("output").size());
        assertEquals(true, d.getField("output").contains("cat"));
        assertEquals(true, d.getField("output").contains("American"));
        assertEquals(true, d.getField("output").contains("Canadian"));
    }


    private Document getDocument(String input, String mgr) {
        StageConfig sc = new StageConfig("test", "test");
        sc.setStringParam("inputField", "input");
        sc.setStringParam("outputField", "output");
        sc.setStringParam("dictionaryFile", "test-dict.csv");
        sc.setStringParam("dictionaryManager", mgr);
        DictionaryEE st = new DictionaryEE();
        st.startStage(sc);

        Document d = new Document("123");
        if (input != null) {
            d.addToField("input", input);
        }

        try {
            st.processDocument(d);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        return d;
    }
}
