package com.kmwllc.brigade.stage;

import java.util.List;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

public class CountFieldValues extends AbstractStage {

  private String inputField = null;
  private String outputField = null;
  
  @Override
  public void startStage(StageConfig config) {
    inputField = config.getProperty("inputField");
    outputField = config.getProperty("outputField");
  }

  @Override
  public List<Document> processDocument(Document doc) {
   
    if (doc.hasField(inputField)) {
      doc.setField(outputField, doc.getField(inputField).size());
    } else {
      doc.setField(outputField, 0);
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
