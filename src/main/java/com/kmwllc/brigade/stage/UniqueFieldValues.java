package com.kmwllc.brigade.stage;

import java.util.HashSet;
import java.util.List;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

/**
 * This stage will remove all duplicate values for a given field on a document.
 * 
 * @author kwatters
 *
 */
public class UniqueFieldValues extends AbstractStage {

  private String fieldName;

  @Override
  public void startStage(StageConfig config) {
    // NoOp
    fieldName = config.getProperty("fieldName");
  }

  @Override
  public List<Document> processDocument(Document doc) {

    HashSet<Object> unique = new HashSet<Object>();
    for (Object o : doc.getField(fieldName)) {
      unique.add(o);
    }
    doc.removeField(fieldName);
    for (Object o : unique) {
      doc.addToField(fieldName, o);
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

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

}
