package com.kmwllc.brigade.concurrency;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.AbstractStage;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class DumpDoc extends AbstractStage {
    private Writer out;
    private static final Object FILE_ACCESS_LOCK = new Object();

    @Override
    public void startStage(StageConfig config) {
        String outputFile = config.getStringParam("output");
        try {
            out = new BufferedWriter(new FileWriter(outputFile, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Document> processDocument(Document doc) throws Exception {
        synchronized (FILE_ACCESS_LOCK) {
            List<String> fieldList = new ArrayList<>(doc.getFields());
            Collections.sort(fieldList);
            out.write("///\n");
            out.write(String.format("id:%s\n", doc.getId()));
            for (String field : fieldList) {
                out.write(field + ":");
                Iterator<Object> vIter = doc.getField(field).iterator();
                while (vIter.hasNext()) {
                    out.write(vIter.next().toString());
                    if (vIter.hasNext()) {
                        out.write(",");
                    }
                }
                out.write("\n");
                out.flush();
            }
        }
        return null;
    }

    @Override
    public void stopStage() {
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() {

    }
}
