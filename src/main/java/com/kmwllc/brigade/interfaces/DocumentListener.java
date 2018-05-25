package com.kmwllc.brigade.interfaces;

import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.document.ProcessingStatus;

import java.util.List;

/**
 * TODO: review this , this is what the workflow server should implement.
 * 
 * @author kwatters
 *
 */
public interface DocumentListener {

  String getName();

  ProcessingStatus onDocument(Document doc);

  ProcessingStatus onDocuments(List<Document> docs);

  boolean onFlush();

}
