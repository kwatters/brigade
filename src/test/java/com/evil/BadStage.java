package com.evil;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.AbstractStage;

import java.util.List;

/**
 * Created by matt on 4/13/17.
 */
public class BadStage extends AbstractStage {

    @Override
    public void startStage(StageConfig config) {

    }

    @Override
    public List<Document> processDocument(Document doc) throws Exception {
        throw new Exception("BAD");
    }

    @Override
    public void stopStage() {

    }

    @Override
    public void flush() {

    }
}
