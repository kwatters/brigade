package com.kmwllc.brigade.util;

import com.kmwllc.brigade.utils.FieldNameMapper;

public class UnderscoreFieldNames implements FieldNameMapper {
  @Override
  public String mapFieldName(String orig) {
    return orig.replaceAll("\\s+", "_");
  }
}
