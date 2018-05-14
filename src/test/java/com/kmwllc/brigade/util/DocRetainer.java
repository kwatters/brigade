package com.kmwllc.brigade.util;

import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.event.DefaultConnectorListener;

import java.util.ArrayList;
import java.util.List;

public class DocRetainer extends DefaultConnectorListener {
  private List<Document> docs;

  public DocRetainer() {
    docs = new ArrayList<>();
  }

  public List<Document> getDocs() {
    return docs;
  }

  public void setDocs(List<Document> docs) {
    this.docs = docs;
  }

  @Override
  public void onDocument(Document doc) {
    docs.add(doc);
  }
}
