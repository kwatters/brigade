package com.kmwllc.brigade.event;

import com.kmwllc.brigade.config.ConnectorConfig;

/**
 * Listens for events fired by the connector during its lifecycle:<ul>
 *   <li>connectorBegin - Connector has been registered and is about to start</li>
 *   <li>connectorEnd - Connector has completed its work</li>
 * </ul>
 */
public interface ConnectorEventListener {
  /**
   * Fired when Connector has been registered with ConnectorServer and is about to start
   * @param cc ConnectorConfig object for this connector
   */
  void connectorBegin(ConnectorConfig cc);

  /**
   * Fired when Connector has completed its work
   */
  void connectorEnd();
}
