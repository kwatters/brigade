package com.kmwllc.brigade.event;

import com.kmwllc.brigade.stage.StageFailure;

import java.util.List;

/**
 * Listens for events from the pipeline:<ul>
 *   <li>docComplete - Document has successfully completed all stages of the pipeline</li>
 *   <li>docFail - Exceptions were thrown as the document traversed the pipeline</li>
 * </ul>
 * These are intended to be lightweight; they provide registered listeners only with a document id
 * and, for docFail, a list of StageFailures.  If you need access to the entire document, use the
 * DocumentListener instead.
 */
public interface CallbackListener {

  /**
   * Fired when document has successfully complete the pipeline
   * @param docId Unique id of the document
   */
  void docComplete(String docId);

  /**
   * Fired when document has failed to complete the pipeline due to one or more exceptions being thrown
   * @param docId Unique id of the document
   * @param failures Failures accrued during pipeline
   */
  void docFail(String docId, List<StageFailure> failures);
}
