package com.kmwllc.brigade.util;

import com.kmwllc.brigade.utils.FieldNameMapper;

public class LowercaseFieldNames implements FieldNameMapper {
  @Override
  public String mapFieldName(String orig) {
    return orig.toLowerCase();
  }
}
