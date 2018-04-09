package com.kmwllc.brigade.connector;

import com.kmwllc.brigade.concurrency.DumpDocReader;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.util.BrigadeHelper;
import com.kmwllc.brigade.util.BrigadeHelper2;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static com.kmwllc.brigade.config2.ConfigFactory.JSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by matt on 3/22/17.
 */
public class CSVConnectorTest2 {

    @Rule
    public final BrigadeHelper2 brigadeHelper = new BrigadeHelper2("conf/brigade.properties",
            "conf/csv-connector2.json", "conf/vanilla-workflow2.json");

    @Test
    public void testCSV() {
        File testFile = new File("csv-test-output.txt");
        testFile.delete();
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
        testFile.delete();
    }
}
