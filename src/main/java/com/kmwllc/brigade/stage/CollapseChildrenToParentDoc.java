package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollapseChildrenToParentDoc extends AbstractStage {

  private boolean removeChildren = true;
  private Map<String, String> fieldMap = null;
  private String childFieldNamePrefix = ""; 
  
  @Override
  public void startStage(StageConfig config) {
    removeChildren = config.getBoolParam("removeChildren", removeChildren);
    fieldMap = config.getMapParam("fieldMap");
    if (fieldMap == null) {
      fieldMap = new HashMap<String,String>();
    }
    
  }

  @Override
  public List<Document> processDocument(Document doc) throws Exception {
    // TODO Auto-generated method stub
    if (doc.hasChildren()) {
      
      for (Document child : doc.getChildrenDocs()) {
        for (String field : child.getFields()) {
          String parentFieldName = field;
          // System.out.println("Field Name: " + parentFieldName);
          if (fieldMap.containsKey(field)) {
            parentFieldName = fieldMap.get(field);
          }
          ArrayList<Object> vals = child.getField(field);
          for (Object v : vals) {
//            if (v != null & v.toString().length() >0) {
              doc.addToField(parentFieldName, v);
//            }
          }
        }
      }
      
      if (removeChildren) {
        doc.removeChildrenDocs();
      }
      
    }
    
    return null;
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
