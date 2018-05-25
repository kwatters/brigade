package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.List;

public class NameFromSysProp extends AbstractStage {

  @Override
  public void startStage(StageConfig config) {

  }

  @Override
  public List<Document> processDocument(Document doc) throws Exception {
    String sysName = System.getProperty("name");
    if (sysName != null) {
      doc.setField("name", sysName);
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
