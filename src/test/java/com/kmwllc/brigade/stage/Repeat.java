package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.List;

public class Repeat extends AbstractStage {
  private String fieldName;

  @Override
  public void startStage(StageConfig config) {
    fieldName = config.getStringParam("field");
  }

  @Override
  public List<Document> processDocument(Document doc) throws Exception {
    String orig = doc.getField(fieldName).get(0).toString();
    doc.removeField(fieldName);
    doc.setField(fieldName, String.format("%s %s", orig, orig));
    return null;
  }

  @Override
  public void stopStage() {

  }

  @Override
  public void flush() {

  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }
}
