package com.kmwllc.brigade.event;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.StageFailure;

import java.util.List;

/**
 * Default no-op implementation of ConnectorListener.  This is intended as a base class that
 * individual ConnectorListeners can extend without having to implement all the events.
 */
public class DefaultConnectorListener implements ConnectorListener {
  @Override
  public void docComplete(String docId) {

  }

  @Override
  public void docFail(String docId, List<StageFailure> failures) {

  }

  @Override
  public void connectorBegin(ConnectorConfig cc) {

  }

  @Override
  public void connectorEnd() {

  }

  @Override
  public void onDocument(Document doc) {

  }
}
