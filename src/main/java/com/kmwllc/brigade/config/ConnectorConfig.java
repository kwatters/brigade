package com.kmwllc.brigade.config;

public interface ConnectorConfig extends Config<ConnectorConfig> {
    String getConnectorName();
    String getConnectorClass();
}
