package com.kmwllc.brigade.concurrency;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.AbstractStage;

import java.util.List;
import java.util.Random;

public class SleepingFieldReverser extends AbstractStage {
    private String fieldName;
    private int sleepTime;
    private Random random = new Random();

    @Override
    public void startStage(StageConfig config) {
        fieldName = config.getStringParam("field");
        sleepTime = config.getIntegerParam("sleepTime", 100);
    }

    @Override
    public List<Document> processDocument(Document doc) throws Exception {
        String id = doc.getId();
        int sleepAmt = random.nextInt(sleepTime);
        Thread.sleep(sleepAmt);
        String val = doc.getField(fieldName).get(0).toString();
        String rval = new StringBuilder(val).reverse().toString();
        doc.removeField(fieldName);
        doc.setField(fieldName, rval + id);
        return null;
    }

    @Override
    public void stopStage() {

    }

    @Override
    public void flush() {

    }
}
