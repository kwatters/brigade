package com.kmwllc.brigade.event;

import java.util.List;

import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.document.ProcessingStatus;

/**
 * Listener that is fired when a document has either completed a pipeline or failed to do so
 * due to exception.
 * <p/>
 * You should use this listener only if you need to inspect the contents of the document itself.
 * For a lighter-weight listener, consider CallbackListener.
 *
 * 
 * @author kwatters
 *
 */
public interface DocumentListener {

  /**
   * Thrown either when a document has completed a pipeline or has failed to do so due to exceptions
   * thrown during the pipeline run.
   * @param doc Document in its current state when the event occurred
   */
  void onDocument(Document doc);
}
