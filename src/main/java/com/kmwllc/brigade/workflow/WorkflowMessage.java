package com.kmwllc.brigade.workflow;

import com.kmwllc.brigade.document.Document;

/**
 * This is the basic unit of work for a document that is passing through a workflow.
 * @author kwatters
 *
 */
public class WorkflowMessage {

  private String type;
  private Document doc;
  private String workflow;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Document getDoc() {
    return doc;
  }

  public void setDoc(Document doc) {
    this.doc = doc;
  }

  public String getWorkflow() {
    return workflow;
  }

  public void setWorkflow(String workflow) {
    this.workflow = workflow;
  }

}
