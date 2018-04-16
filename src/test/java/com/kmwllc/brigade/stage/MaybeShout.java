package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.List;

public class MaybeShout extends AbstractStage {
  private String fieldName;

  @Override
  public void startStage(StageConfig config) {
    fieldName = config.getStringParam("field");
  }

  @Override
  public List<Document> processDocument(Document doc) throws Exception {
    if (getProps().get("skip").equalsIgnoreCase("true")) {
      return null;
    } else {
      String orig = doc.getField(fieldName).get(0).toString();
      doc.removeField(fieldName);
      doc.setField(fieldName, orig.toUpperCase());
      return null;
    }
  }

  @Override
  public void stopStage() {

  }

  @Override
  public void flush() {

  }
}
