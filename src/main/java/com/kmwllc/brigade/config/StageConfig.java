package com.kmwllc.brigade.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kmwllc.brigade.stage.StageExceptionMode;

public interface StageConfig extends Config<StageConfig> {

  String getStageName();

  String getStageClass();

  @JsonProperty("stageExecutionMode")
  String getStageExecutionModeClass();

  @JsonIgnore
  StageExceptionMode getStageExecutionMode();

  void setStageExceptionMode(StageExceptionMode mode);
}
