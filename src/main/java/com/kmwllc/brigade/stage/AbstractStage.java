package com.kmwllc.brigade.stage;

import java.util.List;

import org.slf4j.Logger;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;

public abstract class AbstractStage {
  
  // TODO: make sure the subclasses get the right logger..
  public final static Logger log = LoggerFactory.getLogger(AbstractStage.class.getCanonicalName());
  // Process only when output field doesn't exist in the document
  // Stages that support this should check and handle it in their
  // processDocument()
  protected boolean processOnlyNull = false;

  public abstract void startStage(StageConfig config);

  public abstract List<Document> processDocument(Document doc);

  public abstract void stopStage();

  public abstract void flush();
}
