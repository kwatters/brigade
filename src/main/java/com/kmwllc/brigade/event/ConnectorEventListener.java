package com.kmwllc.brigade.event;

import com.kmwllc.brigade.config.ConnectorConfig;

public interface ConnectorEventListener {
  void connectorBegin(ConnectorConfig cc);
  void connectorEnd();
}
