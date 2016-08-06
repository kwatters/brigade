package com.kmwllc.brigade.stage;

import java.util.List;
import java.util.Set;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;



/**
 * This stage will rename fields on a document to lowercase them, and replace
 * punctuation with underscores This is useful to make the field names search
 * engine(solr) friendly.
 *
 * @author kwatters
 *
 */
public class NormalizeFieldNames extends AbstractStage {

  @Override
  public void startStage(StageConfig config) {
    // none.. yet.
  }

  @Override
  public List<Document> processDocument(Document doc) {

    Set<String> fieldNames = doc.getFields();
    for (String fieldName : fieldNames) {
      doc.renameField(fieldName, normalizeFieldName(fieldName));
    }
    return null;
  }

  private String normalizeFieldName(String fieldName) {
    // TODO: better field name normalization..
    String normFieldName = fieldName.replaceAll(" ", "_");
    normFieldName = normFieldName.replaceAll("/", "_");
    normFieldName = normFieldName.replaceAll("-", "_");
    normFieldName = normFieldName.replaceAll("_+", "_");
    normFieldName = normFieldName.toLowerCase();
    return normFieldName;
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
