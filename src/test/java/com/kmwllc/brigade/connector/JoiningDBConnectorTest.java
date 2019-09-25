package com.kmwllc.brigade.connector;

import com.kmwllc.brigade.concurrency.DumpDocReader;
import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.legacyXML.LegacyXMLConnectorConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.util.BrigadeHelper;
import com.kmwllc.brigade.util.DBHelper;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test out the joining database connector by running a primary sql
 * and a list of otherSql statements.
 */
public class JoiningDBConnectorTest {

    @Rule
    public final DBHelper dbHelper = new DBHelper("org.h2.Driver", "jdbc:h2:mem:test", "",
            "", "db-test-start.sql", "db-test-end.sql");

    @Rule
    public final BrigadeHelper brigadeHelper = new BrigadeHelper("conf/brigade.properties",
            "conf/joining-db-connector.xml", "conf/vanilla-collapse-workflow.xml");
    
    private File testFile = new File("csv-test-output.txt");

    @After
    public void cleanup() {
        testFile.delete();
    }

    @Test
    public void testDB() {

        try {
            brigadeHelper.exec();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        List<Document> docs = new DumpDocReader().read(testFile);
        // assertEquals(3, docs.size());
        
        assertEquals("Matt", docs.get(0).getField("name").get(0));
        assertEquals("Cat", docs.get(2).getField("type").get(0));
        assertEquals("weight, hair_color", docs.get(0).getField("meta_name").get(0));
		//        for (Document doc : docs) {
		//        	System.out.println("----------------------");
		//    		System.out.println(doc);
		//    		if (doc.hasChildren())
		//    			for (Document d : doc.getChildrenDocs()) {
		//    				System.out.println(d);
		//    			}
		//        }
    }
}
