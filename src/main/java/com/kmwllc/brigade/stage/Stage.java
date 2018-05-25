package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.document.Document;

import java.util.List;

/**
 * Describes a stage in Brigade.  In general, custom stages should extend AbstractStage
 * rather than implementing this interface.
 */
public interface Stage {
  void flush();

  String getEnabled();

  String getSkipIfField();

  String getName();

  List<Document> processDocument(Document doc) throws Exception;

  StageExceptionMode getStageExceptionMode();

  void setStageExceptionMode(StageExceptionMode mode);
}
