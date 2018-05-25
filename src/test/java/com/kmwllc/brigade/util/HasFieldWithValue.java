package com.kmwllc.brigade.util;

import com.kmwllc.brigade.document.Document;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class HasFieldWithValue extends TypeSafeMatcher<Document> {
  private final String fieldName;
  private final Object fieldValue;

  public HasFieldWithValue(String fieldName, Object fieldValue) {
    this.fieldName = fieldName;
    this.fieldValue = fieldValue;
  }


  @Override
  public void describeTo(Description description) {
    description.appendText("value of field (").appendValue(fieldName).appendText(") to be ").
            appendValue(fieldValue.toString());
  }

  @Override
  protected void describeMismatchSafely(Document doc, Description description) {
    Object actualValue = doc.hasField(fieldName) ? doc.getField(fieldName).get(0).toString() : "null";
    description.appendText("value of field (").appendValue(fieldName).appendText(") was ")
            .appendValue(actualValue.toString());
  }

  @Override
  protected boolean matchesSafely(Document doc) {
    return doc != null && doc.hasField(fieldName) &&
            doc.getField(fieldName).get(0).toString().equals(fieldValue.toString());
  }

  @Factory
  public static Matcher<Document> hasFieldWithValue(String fieldName, Object fieldValue) {
    return new HasFieldWithValue(fieldName, fieldValue);
  }
}
