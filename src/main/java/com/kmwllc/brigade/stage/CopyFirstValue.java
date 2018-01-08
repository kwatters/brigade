package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.List;
import java.util.Map;

/**
 * Created by matt on 4/6/17.
 */
public class CopyFirstValue extends AbstractStage {

    private Map<String, String> ioMap;

    @Override
    public void startStage(StageConfig config) {
        ioMap = config.getMapParam("ioMap");
    }

    @Override
    public List<Document> processDocument(Document doc) {
        for (Map.Entry<String, String> entry : ioMap.entrySet()) {
            String input = entry.getKey();
            String output = entry.getValue();

            if (!doc.hasField(input)) {
                continue;
            }

            Object fv = doc.getField(input).get(0);
            doc.setField(output, fv);
        }
        return null;
    }

    @Override
    public void stopStage() {

    }

    @Override
    public void flush() {

    }
}
