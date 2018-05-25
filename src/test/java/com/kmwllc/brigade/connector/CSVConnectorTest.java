package com.kmwllc.brigade.connector;

import com.kmwllc.brigade.concurrency.DumpDocReader;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.util.BrigadeHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by matt on 3/22/17.
 */
public class CSVConnectorTest {

    @Rule
    public final BrigadeHelper brigadeHelper = new BrigadeHelper("conf/brigade.properties",
            "conf/csv-connector.xml", "conf/vanilla-workflow.xml");
    private File testFile = new File("csv-test-output.txt");

    @After
    public void cleanup() {
        testFile.delete();
    }

    @Test
    public void testCSV() {
        try {
            brigadeHelper.exec();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        List<Document> docs = new DumpDocReader().read(testFile);
        assertEquals(4, docs.size());
        assertEquals("Matt", docs.get(1).getField("author").get(0));
        assertEquals("Meow Meow", docs.get(3).getField("text").get(0));
    }
}
