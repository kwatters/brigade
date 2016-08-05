package com.kmwllc.brigade.interfaces;

import java.util.List;

import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.document.ProcessingStatus;

public interface DocumentListener {

  public String getName();

  public ProcessingStatus onDocument(Document doc);

  public ProcessingStatus onDocuments(List<Document> docs);

  public boolean onFlush();

}
