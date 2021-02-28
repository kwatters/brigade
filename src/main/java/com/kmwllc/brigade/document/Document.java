package com.kmwllc.brigade.document;

import com.kmwllc.brigade.stage.StageFailure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * The basic class that represents a document flowing through brigade.
 * <p>
 * Basic idea is that a document had a unique id and a map of key to list of
 * object pairs.
 * <p>
 * Documents can also maintain a processing status on them so that if a stage fires and exception
 * the document can be marked as having some issue.
 * <p>
 *   Documents also maintain a list of StageFailures which may be accrued as the document makes
 *   its way through a pipeline.  How these failures are handled is determined by the StageExecutionMode
 *   of the pipeline and/or current stage being processed.
 * </p>
 *
 * @author kwatters
 */
public class Document {

  private String id;
  private HashMap<String, ArrayList<Object>> data;
  // TODO: should this be moved? or maybe a more rich object?
  private ProcessingStatus status;

  private ArrayList<Document> childrenDocs;
  private List<StageFailure> failures;


  public Document(String id) {
    this.id = id;
    data = new HashMap<>();
    status = ProcessingStatus.OK;
    failures = new ArrayList<>();
  }

  /**
   * Get contents of field in this document or null if the document does not contain the field
   * @param fieldName Name of field
   * @return List of contents or null if field not found
   */
  public ArrayList<Object> getField(String fieldName) {
    if (data.containsKey(fieldName)) {
      return data.get(fieldName);
    } else {
      return null;
    }
  }

  /**
   * Set field values to the specified list for specified field name.
   * @param fieldName Name of field
   * @param value List of contents
   */
  public void setField(String fieldName, ArrayList<Object> value) {
    data.put(fieldName, value);
  }

  /**
   * Set field value to a single object for specified field name.  In the background, This constructs
   * a list with one element and sets that as the values for the field.
   * @param fieldName Name of field
   * @param value Content for field
   */
  public void setField(String fieldName, Object value) {
    ArrayList<Object> values = new ArrayList<>();
    values.add(value);
    data.put(fieldName, values);
  }

  /**
   * Convenience method which only sets field to value if the specified value is not null.
   * @param fieldName Name of field
   * @param value Value for field
   */
  public void setFieldIfNotNull(String fieldName, Object value) {
    if (value != null){
      setField(fieldName, value);
    }
  }

  /**
   * Change the name of a field, keeping contents the same
   * @param oldField original field name
   * @param newField new name for field
   */
  public void renameField(String oldField, String newField) {
    if (data.containsKey(oldField)) {
      // TODO: test me to make sure this is correct.
      data.put(newField, data.get(oldField));
      data.remove(oldField);
    }

  }

  /**
   * Add specified value to the specified field.  If field is not currently in document, add it.
   * @param fieldName Name of field
   * @param value Value to add to field contents
   */
  public void addToField(String fieldName, Object value) {
    if (data.containsKey(fieldName) && (data.get(fieldName) != null)) {
      data.get(fieldName).add(value);
    } else {
      ArrayList<Object> values = new ArrayList<>();
      values.add(value);
      data.put(fieldName, values);
    }
  }

  /**
   * Get the first value for specified field.  Returns null if the field is not
   * in the document
   * @param fieldName Name of field
   * @return First value for field as an object
   */
  public String getFirstValueAsString(String fieldName) {
    List<Object> s = data.get(fieldName);
    if (s==null||s.isEmpty()) {
      return null;
    }
    return s.get(0).toString();
  }

  public Object getFirstValue(String fieldName) {
    List<Object> s = data.get(fieldName);
    if (s==null||s.isEmpty()) {
      return null;
    }
    return s.get(0);
  }
  
  /**
   * Get the unique ID for this document
   * @return Document id
   */
  public String getId() {
    return id;
  }

  /**
   * Set unique Id of document to the specified value
   * @param id New document id
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Answer whether document contains specified field
   * @param fieldName Name of field
   * @return whether the document contains this field
   */
  public boolean hasField(String fieldName) {
    return data.containsKey(fieldName);
  }

  /**
   * Return a set of all fields on a given document. This is unordered.
   *
   * @return Set of fields
   */
  public Set<String> getFields() {
    return data.keySet();
  }

  /**
   * Remove field with specified name from the document
   * @param fieldName Name of field to remove
   */
  public void removeField(String fieldName) {
    data.remove(fieldName);

  }

  /**
   * Get processing status for this document
   * @return status
   */
  public ProcessingStatus getStatus() {
    return status;
  }

  /**
   * Set processing status for this document to specified value
   * @param status status
   */
  public void setStatus(ProcessingStatus status) {
    this.status = status;
  }

  /**
   * Get the Map that contains all fields and their contents
   * @return Map of fieldNames -> field contents
   */
  public HashMap<String, ArrayList<Object>> getData() {
    return data;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((data == null) ? 0 : data.hashCode());
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((status == null) ? 0 : status.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Document other = (Document) obj;
    if (data == null) {
      if (other.data != null)
        return false;
    } else if (!data.equals(other.data))
      return false;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (status != other.status)
      return false;
    return true;
  }

  @Override
  public String toString() {
    // TODO: add the tostring of each of the children docs.
    StringBuilder docToStr = new StringBuilder();
    docToStr.append("Document [id=" + id + ", data=" + data + ", status=" + status);
    if (childrenDocs != null) {
      for (Document child : childrenDocs) {
        docToStr.append(" child=" + child.toString());
      }
    }
    docToStr.append("]");
    return docToStr.toString();
  }

  public void addChildDocument(Document child) {
    if (childrenDocs == null) {
      childrenDocs = new ArrayList<>();
    }
    childrenDocs.add(child);
  }

  /**
   * Get any child documents of this document
   * @return List of child documents
   */
  public ArrayList<Document> getChildrenDocs() {
    return childrenDocs;
  }

  /**
   * Set the list of child documents to the specified list.
   * @param childrenDocs What the child documents are to be
   */
  public void setChildrenDocs(ArrayList<Document> childrenDocs) {
    this.childrenDocs = childrenDocs;
  }

  /**
   * Get the number of child documents
   * @return number of docs
   */
  public int getNumberOfChildrenDocs() {
    if (childrenDocs == null) {
      return 0;
    } else {
      return childrenDocs.size();
    }
  }

  /**
   * Remove all child documents from this document
   */
  public void removeChildrenDocs() {
    childrenDocs = null;
  }

  /**
   * Answer whether this document has any child documents
   * @return whether this document has any child documents
   */
  public boolean hasChildren() {
    if (childrenDocs == null) {
      return false;
    } else {
      if (childrenDocs.size() > 0) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Get all StageFailures which have been accrued by this document.
   * @return List of failures
   */
  public List<StageFailure> getFailures() {
    return failures;
  }

  /**
   * Set the list of StageFailures to the specified list.
   * @param failures List of failures
   */
  public void setFailures(List<StageFailure> failures) {
    this.failures = failures;
  }

  /**
   * Add the given failure to the list of failures for this document
   * @param failure Failure object to add
   */
  public void addFailure(StageFailure failure) {
    failures.add(failure);
  }

  /**
   * Answer whether this document has accrued any failures
   * @return whether this document has accrued any failures
   */
  public boolean hasFailures() {
    return failures.size() > 0;
  }
}
