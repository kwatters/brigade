package com.kmwllc.brigade.utils;

/**
 * Implementing this interface defines how the fields of a document can be
 * renamed by a Connector implementation.  Example implementations may,
 * for example, capitalize field names or normalize punctuation on them.
 */
public interface FieldNameMapper {
  /**
   * Return name of the fieldNameMapper if specified, else return the
   * simple name of the implementing class.
   * @return Name of fieldNameMapper or simple class name
   */
  default String getName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Implement this method to dictate how field names should be remapped
   * @param orig The original field name
   * @return Remapped field name
   */
  String mapFieldName(String orig);
}
