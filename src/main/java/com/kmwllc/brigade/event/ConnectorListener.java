package com.kmwllc.brigade.event;

/**
 * Super interface which implements all the listeners which may be associated with a connector.
 */
public interface ConnectorListener extends CallbackListener, ConnectorEventListener, DocumentListener {
  /**
   * Get the name of the connector.  Default behavior is to return the simple name of the implementing class
   * @return Name of connector or, if unavailable, simple name of class
   */
  default String getName() {
    return this.getClass().getSimpleName();
  }
}
