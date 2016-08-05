package com.kmwllc.brigade.config;

import java.util.HashMap;

public class StageConfig extends Config {

  // private HashMap<String, Object> config = null;

  private String stageName = "defaultStage";
  private String stageClass = "com.kmwllc.brigade.stage.AbstractStage";

  public StageConfig(String stageName, String stageClass) {
    config = new HashMap<String, Object>();
    this.stageName = stageName;
    this.stageClass = stageClass;
  }

  public StageConfig() {
    // depricate this constructor?
    config = new HashMap<String, Object>();
  }

  public void setStringParam(String name, String value) {
    config.put(name, value);
  }

  public String getStringParam(String name) {
    if (config.containsKey(name)) {
      Object val = config.get(name);
      if (val instanceof String) {
        return ((String) val).trim();
      } else {
        // TOOD: this value was not a string?
        return val.toString().trim();
      }
    } else {
      return null;
    }
  }

  public String getStageName() {
    return stageName;
  }

  public void setStageName(String stageName) {
    this.stageName = stageName;
  }

  public String getStageClass() {
    return stageClass;
  }

  public void setStageClass(String stageClass) {
    this.stageClass = stageClass;
  }

}
