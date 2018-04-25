package com.kmwllc.brigade.document;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DocumentTest {

  @Test
  public void testGetField() {
    Document d1 = new Document("1");
    d1.setField("a", "aaa");
    assertEquals("aaa", d1.getField("a").get(0));
    assertNull(d1.getField("b"));
  }

  @Test
  public void testSetField() {
    Document d1 = new Document("1");
    d1.setField("a", "aaa");
    d1.setField("a", "bbb");
    assertEquals(1, d1.getField("a").size());
    assertEquals("bbb", d1.getField("a").get(0));
    d1.setField("b", "bbb");
    assertEquals("bbb", d1.getField("b").get(0));
  }

  @Test
  public void testRenameField() {
    Document d1 = new Document("1");
    d1.setField("a", "aaa");
    d1.renameField("a", "b");
    assertEquals(1, d1.getField("b").size());
    assertEquals("aaa", d1.getField("b").get(0));
    assertNull(d1.getField("a"));
  }

  @Test
  public void testAddToField() {
    Document d1 = new Document("1");
    d1.setField("a", "aaa");
    d1.addToField("a", "bbb");
    d1.addToField("b", "ccc");
    assertEquals(2, d1.getField("a").size());
    assertEquals(1, d1.getField("b").size());
    assertEquals("ccc", d1.getField("b").get(0));
  }

  @Test
  public void testGetFirstValueAsString() {
    Document d1 = new Document("1");
    d1.setField("a", "aaa");
    d1.addToField("a", "bbb");
    d1.setField("b", 123);
    d1.addToField("b", "ccc");
    assertEquals("aaa", d1.getFirstValueAsString("a"));
    assertEquals("123", d1.getFirstValueAsString("b"));
    assertNull(d1.getFirstValueAsString("c"));
  }
}
