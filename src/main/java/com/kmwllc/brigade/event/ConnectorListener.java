package com.kmwllc.brigade.event;

public interface ConnectorListener extends CallbackListener, ConnectorEventListener, DocumentListener {
  default String getName() {
    return this.getClass().getSimpleName();
  }
}
