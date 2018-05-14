package com.kmwllc.brigade.event;

import com.kmwllc.brigade.config.ConnectorConfig;

public class SetSysName extends DefaultConnectorListener {
  @Override
  public void connectorBegin(ConnectorConfig cc) {
    System.setProperty("name", "Matt");
  }

  @Override
  public void connectorEnd() {
    System.clearProperty("name");
  }
}
