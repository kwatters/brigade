package com.kmwllc.brigade.stage;

import java.util.List;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

/**
 * This stage will copy field values from field A to field B.
 * 
 * @author kwatters
 *
 */
public class CopyField extends AbstractStage {
  private String source = "fielda";
  private String dest = "fieldb";

  @Override
  public void startStage(StageConfig config) {
    if (config != null) {
      source = config.getProperty("source", source);
      dest = config.getProperty("dest", dest);
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    if (doc.hasField(source)) {
      for (Object o : doc.getField(source)) {
        // TODO: Clone these objects?
        doc.addToField(dest, o);
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
