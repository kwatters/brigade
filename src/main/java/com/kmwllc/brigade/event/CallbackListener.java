package com.kmwllc.brigade.event;

import com.kmwllc.brigade.stage.StageFailure;

import java.util.List;

public interface CallbackListener {

  void docComplete(String docId);
  void docFail(String docId, List<StageFailure> failures);
}
