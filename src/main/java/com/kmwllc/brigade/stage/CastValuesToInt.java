package com.kmwllc.brigade.stage;

import java.util.ArrayList;
import java.util.List;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

/**
 * This stage will iterate the values of the inputField and attempt to cast them
 * to an integer. The values will be stored in the outputField. Values in the
 * output field will be overwritten.
 * 
 * @author kwatters
 *
 */
public class CastValuesToInt extends AbstractStage {

  private String inputField = null;
  private String outputField = null;

  @Override
  public void startStage(StageConfig config) {
    if (config != null) {
      inputField = config.getProperty("inputField");
      outputField = config.getProperty("outputField");
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    if (!doc.hasField(inputField)) {
      // doc doesn't have this field... just return
      return null;
    }
    // throw away malformed values.
    ArrayList<Integer> ints = new ArrayList<Integer>();
    for (Object val : doc.getField(inputField)) {
      if (val != null) {
        try {
          int i = Integer.valueOf(val.toString().replaceAll(",", ""));
          ints.add(i);
        } catch (NumberFormatException e) {
          e.printStackTrace();
        }
      }
    }
    doc.removeField(outputField);
    for (Integer i : ints) {
      doc.addToField(outputField, i);
    }
    return null;
  }

  @Override
  public void stopStage() {
    // NOOP
  }

  @Override
  public void flush() {
    // NOOP

  }

}
