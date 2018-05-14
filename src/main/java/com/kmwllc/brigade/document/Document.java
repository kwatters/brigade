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

  public ArrayList<Object> getField(String fieldName) {
    if (data.containsKey(fieldName)) {
      return data.get(fieldName);
    } else {
      return null;
    }
  }

  public void setField(String fieldName, ArrayList<Object> value) {
    data.put(fieldName, value);
  }

  public void setField(String fieldName, Object value) {
    ArrayList<Object> values = new ArrayList<>();
    values.add(value);
    data.put(fieldName, values);
  }

  public void setFieldIfNotNull(String fieldName, Object value) {
    if (value != null){
      setField(fieldName, value);
    }
  }

  public void renameField(String oldField, String newField) {
    if (data.containsKey(oldField)) {
      // TODO: test me to make sure this is correct.
      data.put(newField, data.get(oldField));
      data.remove(oldField);
    }

  }

  public void addToField(String fieldName, Object value) {
    if (data.containsKey(fieldName) && (data.get(fieldName) != null)) {
      data.get(fieldName).add(value);
    } else {
      ArrayList<Object> values = new ArrayList<>();
      values.add(value);
      data.put(fieldName, values);
    }
  }

  public String getFirstValueAsString(String fieldName) {
    List<Object> s = data.get(fieldName);
    if (s==null||s.isEmpty()) {
      return null;
    }
    return s.get(0).toString();
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public boolean hasField(String fieldName) {
    return data.containsKey(fieldName);
  }

  /**
   * Return a set of all fields on a given document. This is unordered.
   *
   * @return
   */
  public Set<String> getFields() {
    return data.keySet();
  }

  public void removeField(String fieldName) {
    data.remove(fieldName);

  }

  public ProcessingStatus getStatus() {
    return status;
  }

  public void setStatus(ProcessingStatus status) {
    this.status = status;
  }

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

  public ArrayList<Document> getChildrenDocs() {
    return childrenDocs;
  }

  public void setChildrenDocs(ArrayList<Document> childrenDocs) {
    this.childrenDocs = childrenDocs;
  }

  public int getNumberOfChildrenDocs() {
    if (childrenDocs == null) {
      return 0;
    } else {
      return childrenDocs.size();
    }
  }

  public void removeChildrenDocs() {
    childrenDocs = null;
  }

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

  public List<StageFailure> getFailures() {
    return failures;
  }

  public void setFailures(List<StageFailure> failures) {
    this.failures = failures;
  }

  public void addFailure(StageFailure failure) {
    failures.add(failure);
  }

  public boolean hasFailures() {
    return failures.size() > 0;
  }
}
