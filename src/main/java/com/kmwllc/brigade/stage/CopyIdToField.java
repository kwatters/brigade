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
public class CopyIdToField extends AbstractStage {
  
  private String fieldName = "node_id";

  @Override
  public void startStage(StageConfig config) {
    if (config != null) {
      fieldName = config.getProperty("fieldName", fieldName);
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
	  doc.addToField(fieldName, doc.getId());
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
