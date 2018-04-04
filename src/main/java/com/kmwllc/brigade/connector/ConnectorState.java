package com.kmwllc.brigade.connector;

/**
 * This ENUM represents the state of a connector.  
 * It's the responsibility for the implementing connector to update this state.
 * 
 * @author kwatters
 *
 */
public enum ConnectorState {
  STOPPED, RUNNING, INTERRUPTED, ERROR, OFF
}
