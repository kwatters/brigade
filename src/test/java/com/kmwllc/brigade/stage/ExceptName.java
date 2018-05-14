package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.List;

public class ExceptName extends AbstractStage {
  private String fieldName;
  private String except;

  @Override
  public void startStage(StageConfig config) {
    fieldName = config.getStringParam("field");
    except = config.getStringParam("except");
  }

  @Override
  public List<Document> processDocument(Document doc) throws Exception {
    String orig = doc.getField(fieldName).get(0).toString();
    if (orig.equalsIgnoreCase(except)) {
      throw new Exception(String.format("Field %s can't be set to %s", fieldName, except));
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
