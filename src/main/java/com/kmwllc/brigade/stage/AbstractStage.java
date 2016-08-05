package com.kmwllc.brigade.stage;

import java.util.List;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

public abstract class AbstractStage {
  // Process only when output field doesn't exist in the document
  // Stages that support this should check and handle it in their
  // processDocument()
  protected boolean processOnlyNull = false;

  public abstract void startStage(StageConfig config);

  public abstract List<Document> processDocument(Document doc);

  public abstract void stopStage();

  public abstract void flush();
}
