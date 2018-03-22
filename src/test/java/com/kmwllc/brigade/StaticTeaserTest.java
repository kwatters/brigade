package com.kmwllc.brigade;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.StaticTeaser;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by matt on 4/18/17.
 */
public class StaticTeaserTest {

    @Test
    public void test1() {
        Document d = getDocument("attributes");

        assertEquals("attributes", d.getField("teaser").get(0));
    }

    @Test
    public void test2() {
        Document d = getDocument("confident cat");

        assertEquals("confident", d.getField("teaser").get(0));
    }

    @Test
    public void test3() {
        Document d = getDocument("confidently");

        assertEquals("Teaser not available.", d.getField("teaser").get(0));
    }

    @Test
    public void test4() {
        Document d = getDocument("cat cat goose");

        assertEquals("cat cat", d.getField("teaser").get(0));
    }

    @Test
    public void test5() {
        Document d = getDocument(null);

        assertEquals("Teaser not available.", d.getField("teaser").get(0));
    }

    @Test
    public void test6() {
        Document d = getDocument("short");
        assertEquals("short", d.getField("teaser").get(0));
    }


    private Document getDocument(String input) {
        StageConfig sc = new StageConfig("test", "test");
        List<String> inputs = new ArrayList<>();
        inputs.add("body");
        sc.setListParam("inputFields", inputs);
        sc.setStringParam("outputField", "teaser");
        sc.setIntegerParam("length", 10);
        StaticTeaser st = new StaticTeaser();
        st.startStage(sc);

        Document d = new Document("123");
        if (input != null) {
            d.addToField("body", input);
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
