package com.kmwllc.brigade.stage;

import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

/**
 * This stage will join together a list of values into a single string value
 * with a separator.
 * 
 * @author kwatters
 *
 */
public class JoinFieldValues extends AbstractStage {

  private String inputField;
  private String outputField;
  private String joinString;

  @Override
  public void startStage(StageConfig config) {
    if (config != null) {
      inputField = config.getProperty("inputField");
      outputField = config.getProperty("outputField");
      joinString = config.getProperty("joinString");
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    if (doc.hasField(inputField)) {
      String joinedValues = StringUtils.join(doc.getField(inputField), joinString);
      doc.setField(outputField, joinedValues);
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
