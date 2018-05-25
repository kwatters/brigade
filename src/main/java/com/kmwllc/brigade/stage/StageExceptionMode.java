package com.kmwllc.brigade.stage;

/**
 * Defines what a pipeline should do in the event that one of the stages
 * throws an exception.  The modes are:<ul>
 *   <li>NEXT_STAGE - Pipeline should continue processing the document,
 *   moving on to the next stage of the pipeline</li>
 *   <li>NEXT_DOC - Pipeline should continue, but should move on to
 *   the next document in the queue</li>
 *   <li>STOP_WORKFLOW - Pipeline should stop.  No more documents will
 *   be processed and the Connector thread will be interrupted</li>
 * </ul>
 * This mode can be set at both the level of the pipeline and individual
 * stages.  Each stage inherits the stageExceptionMode setting of the
 * pipeline.  If the pipeline does not explicitly set a stageExceptionMode,
 * NEXT_DOC will be used as the default setting.  Setting stageExceptionMode
 * on an individual stage will override the pipeline's setting for that stage.
 */
public enum StageExceptionMode {
  NEXT_STAGE, NEXT_DOC, STOP_WORKFLOW;
}
