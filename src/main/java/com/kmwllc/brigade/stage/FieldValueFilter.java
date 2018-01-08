package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class FieldValueFilter extends AbstractStage {

  public final static Logger log = LoggerFactory.getLogger(FieldValueFilter.class.getCanonicalName());
  
  private String fieldName = null;
  
  private List<String> prefixFilterList = new ArrayList<String>();
  
  
  @Override
  public void startStage(StageConfig config) {
    fieldName = config.getStringParam("fieldName");
    prefixFilterList = config.getListParam("prefixFilterList");
  }

  @Override
  public List<Document> processDocument(Document doc) throws Exception {
    // 
    if (!doc.hasField(fieldName)) {
      return null;
    }
    ArrayList<Object> filteredVals = new ArrayList<Object>();
    
    for (Object o : doc.getField(fieldName)) {
      boolean isOk = true;

      if (o == null) continue;
      String strO = o.toString().toLowerCase().trim();
      for (String prefix : prefixFilterList) {
        // TODO: more effecient case insensitive handling.. this is pretty bad.
        if (strO.startsWith(prefix.toLowerCase().trim())) {
          log.warn("Doc : {} filtering field value {}", doc.getId(), o);
          isOk = false;
          continue;
        }
      }
      if (isOk) {
        filteredVals.add(o);
      }
    }
    // 
    doc.removeField(fieldName);
    for (Object o : filteredVals) {
      doc.addToField(fieldName, o);
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
