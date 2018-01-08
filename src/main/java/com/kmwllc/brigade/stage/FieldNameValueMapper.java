package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.List;

/**
 * This stage will take a field that contains a list of field names and a field that contains a list of values
 * it will iterate those two lists and convert them into fields with values on the documents rather.
 * 
 * @author kwatters
 *
 */

public class FieldNameValueMapper extends AbstractStage {

  private String nameField;
  private String valueField;
  
  private boolean cleanFieldName = true;
  private String fieldNamePrefix = null;

  
  @Override
  public void startStage(StageConfig config) {
    nameField = config.getProperty("nameField");
    valueField = config.getProperty("valueField");
    fieldNamePrefix = config.getProperty("fieldNamePrefix");
    cleanFieldName = config.getBoolParam("cleanFieldName", cleanFieldName);
  }

  @Override
  public List<Document> processDocument(Document doc) throws Exception {

    if (doc.hasField(nameField) && doc.hasField(valueField)) {
      int size = doc.getField(nameField).size();
      int valSize = doc.getField(valueField).size();
      log.warn("Field name {} and value list {} not equal length : {}", size, valSize, doc.getId());
      for (int i = 0 ; i < size; i++) {
        Object fieldName = doc.getField(nameField).get(i);
        Object fieldValue = doc.getField(valueField).get(i);
        String cleanFieldName = cleanFieldName(fieldName);
        if (cleanFieldName != null  && fieldValue != null) {
          // log.info("Orig {} Clean Name: {}" , fieldName, cleanFieldName);
          if (fieldValue.toString().length() > 0) {
            doc.addToField(cleanFieldName, fieldValue);
          }
        }
      }

    }
    return null;
  }

  private String cleanFieldName(Object fieldName) {
    // TODO use a common utils function for field name normalization!
    
    if (fieldName == null) {
      log.error("Null field name!!");

      return null;
    }
    String clean = (String)fieldName;
    clean = clean.trim().toLowerCase();
    clean = clean.replaceAll(" ",  "_");
    clean = clean.replaceAll("\\.", "_");
    
    if (fieldNamePrefix != null) {
      clean = fieldNamePrefix + clean;
    }
    return clean;
  }

  @Override
  public void stopStage() {
    // TODO Auto-generated method stub

  }

  @Override
  public void flush() {
    // TODO Auto-generated method stub

  }

}
