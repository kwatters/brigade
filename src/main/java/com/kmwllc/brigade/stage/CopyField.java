package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.List;

/**
 * This stage will copy field values from field A to field B.
 * 
 * @author kwatters
 *
 * edited by cclemente on 7/17/2017
 */
public class CopyField extends AbstractStage {
  private String source = "fielda";
  private String dest = "fieldb";
  private boolean onlyIfDestEmpty = false;
  
  // if true this will remove the existing values on the destination field
  private boolean overwrite = false;
  // if this is set, the document must match the value in this field to do the copy.
  private String conditionalField = null;
  private String conditionalValue = null;

  @Override
  public void startStage(StageConfig config) {
    if (config != null) {
      source = config.getProperty("source", source);
      dest = config.getProperty("dest", dest);
      onlyIfDestEmpty = config.getBoolParam("onlyIfDestEmpty", onlyIfDestEmpty);
      overwrite = config.getBoolParam("overwrite", overwrite);
      conditionalField = config.getProperty("conditionalField", conditionalField);
      conditionalValue = config.getProperty("conditionalValue", conditionalValue);
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    if (conditionalField != null) {
      boolean found = false;
      // TODO: add a helper method on the doc for hasFieldWithValue(f,v);...
      // test if the document contains the conditional value in the field. if not skip this step
      if (doc.hasField(conditionalField)) {
        for (Object v : doc.getField(conditionalField)) {
          if (v.toString().equals(conditionalValue)) {
            found = true;
            break;
          }
        }
      } else {
        // didn't have the conditional field set .. skip this stage
        return null;
      }
      // we didn't 
      if (!found) {
        return null;
      }
    }
    
    if (doc.hasField(source)) {

      // check destination empty only flag
      if (onlyIfDestEmpty) {
        if (doc.hasField(dest) && (doc.getField(dest).size() > 0)) {
          return null; // if populated, skip document
        }
      }

      
      if (overwrite) {
        // remove the dest field
        doc.removeField(dest);
      }
      
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
