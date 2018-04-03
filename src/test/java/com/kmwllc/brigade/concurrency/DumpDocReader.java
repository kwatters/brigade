package com.kmwllc.brigade.concurrency;

import com.kmwllc.brigade.document.Document;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DumpDocReader {

    public List<Document> read(File file) {
        List<Document> docs = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String s;
            Map<String, String> curr = null;
            while ((s = br.readLine()) != null) {
                if (s.length() > 0) {
                    if (s.startsWith("///")) {
                        if (curr != null) {
                            Document doc = convertMap(curr);
                            docs.add(doc);
                        }
                        curr = new HashMap<>();
                    } else {
                        String[] split = s.split(":");
                        curr.put(split[0], split[1]);
                    }
                }
            }

            Document doc = convertMap(curr);
            docs.add(doc);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return docs;
    }

    private Document convertMap(Map<String, String> map) {
        String id = map.get("id");
        Document doc = new Document(id);
        for (Map.Entry<String, String> e : map.entrySet()) {
            doc.setField(e.getKey(), e.getValue());
        }
        return doc;
    }
}
