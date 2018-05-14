package com.kmwllc.brigade.utils;

public interface FieldNameMapper {
  default String getName() {
    return this.getClass().getSimpleName();
  }
  String mapFieldName(String orig);
}
