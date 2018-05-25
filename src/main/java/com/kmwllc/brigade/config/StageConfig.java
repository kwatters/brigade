package com.kmwllc.brigade.config;

import com.kmwllc.brigade.stage.StageExceptionMode;

/**
 * Configuration to build stages.  Currently does not support the ConfigBuilder
 * interface (possible future work).  Stages are able to set a StageExecutionMode
 * which determines what behavior will occur when an exception is thrown by a
 * stage implementation.  If the stageExecutionMode is not set explicitly, Brigade
 * will use the setting for the pipeline as default.
 */
public interface StageConfig extends Config<StageConfig> {

  /**
   * Get the name of the stage
   * @return Name of the stage
   */
  String getStageName();

  /**
   * Get the fully-qualified class the stage is an instance of
   * @return
   */
  String getStageClass();

  /**
   * Get name for the StageExecutionMode to be used for this stage.  This should
   * be the name of the enum (e.g. NEXT_DOC, NEXT_STAGE, etc..) It is used to
   * instantiate the enum from a serialized configuration.
   * @return Name for the StageExecutionMode
   */
  String getStageExceptionModeClass();

  /**
   * Get StageExecutionMode for the Stage.  If stage does not set this explicitly,
   * the stageExecutionMode setting for the pipeline will be applied.
   * @return StageExecutionMode instance
   */
  StageExceptionMode getStageExceptionMode();

  /**
   * Set the StageExecutionMode for this stage to the specified value. If stage
   * does not set this explicitly, the stageExecutionMode setting for the pipeline
   * will be applied.
   * @param mode StageExceptionMode to apply
   */
  void setStageExceptionMode(StageExceptionMode mode);
}
