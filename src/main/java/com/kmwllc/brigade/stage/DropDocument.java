package com.kmwllc.brigade.stage;

import java.util.List;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.document.ProcessingStatus;

/**
 * DropDocument - if the document contains a particular field value, drop this
 * document from the workflow. input field input value
 * 
 * @author kwatters
 *
 */
public class DropDocument extends AbstractStage {

  private String field;
  private String value;

  @Override
  public void startStage(StageConfig config) {
    if (config != null) {
      field = config.getProperty("field", null);
      value = config.getProperty("value", null);
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    if (doc.hasField(field)) {
      for (Object o : doc.getField(field)) {
        if (o.equals(value)) {
          doc.setStatus(ProcessingStatus.DROP);
          break;
        }
      }
    }
    return null;
  }

  @Override
  public void stopStage() {
    // no-op
  }

  @Override
  public void flush() {
    // no-op for this stage
  }

}
