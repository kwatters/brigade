package com.kmwllc.brigade;

import com.kmwllc.brigade.util.BrigadeHelper;
import org.junit.Rule;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BrigadeThreadingTest {
    @Rule
    public final BrigadeHelper brigadeHelper = new BrigadeHelper("conf/brigade.properties",
            "conf/conc-test-connector.xml", "conf/conc-test-workflow.xml");

    private final int expectedDocCount = 1000;

    @Test
    public void testThreading() {
        File testFile = new File("conc-test-output.txt");
        testFile.delete();
        try {
            brigadeHelper.exec();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        int docCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(testFile))) {
            String s;
            Map<String, String> curr = null;
            while ((s = br.readLine()) != null) {
                if (s.length() > 0) {
                    if (s.startsWith("///")) {
                        docCount++;
                        if (curr != null) {
                            if (!checkCurr(curr)) {
                                fail();
                            }
                        }
                        curr = new HashMap<>();
                    } else {
                        String[] split = s.split(":");
                        curr.put(split[0], split[1]);
                    }
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertEquals(expectedDocCount, docCount);
        testFile.delete();
    }

    private boolean checkCurr(Map<String, String> map) {
        String id = map.get("id");
        for (int i = 1; i <= 5; i++) {
            String key = "f" + i;
            if (!map.containsKey(key) || !map.get(key).equals("zyx" + id)) {
                return false;
            }
        }
        return true;
    }

}
