package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.List;

/**
 * This will set a field on a document with a value
 * 
 * @author kwatters
 *
 */
public class SetStaticFieldValue extends AbstractStage {

  private String fieldName = null;
  private String value = null;
  private List<String> values = null;

  @Override
  public void startStage(StageConfig config) {

    if (config != null) {
      fieldName = config.getStringParam("fieldName");
      value = config.getStringParam("value");
      values = config.getListParam("values");
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    if (values != null) {
      for (String value : values) {
        doc.addToField(fieldName, value);
      }
    } else {
      //doc.addToField(fieldName, value);
      doc.setField(fieldName, value);
    }

    return null;
  }

  @Override
  public void stopStage() {
    // no-op for this stage
  }

  @Override
  public void flush() {
    // Only required if this stage does any batching. NO-OP here.
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

}
