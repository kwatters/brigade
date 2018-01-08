package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.List;

/**
 * This stage will copy field values from field A to field B.
 * 
 * @author kwatters
 *
 */
public class CopyFieldToId extends AbstractStage {
  private String source = "fielda";

  @Override
  public void startStage(StageConfig config) {
    if (config != null) {
      source = config.getProperty("source", source);
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    if (doc.hasField(source)) {
      for (Object o : doc.getField(source)) {
        doc.setId(o.toString());
      }
    } else {
      // noop this doc doesn't have the field. ignore it.
    }
    return null;
  }

  @Override
  public void stopStage() {
    // no-op for this stage
  }

  @Override
  public void flush() {
    // no-op for this stage
  }

}
