package com.kmwllc.brigade.stage;

public class StageFailure {
  private String stageName;
  private Exception exception;
  private long timestamp;

  public StageFailure(String stageName, Exception exception) {
    this.stageName = stageName;
    this.exception = exception;
    timestamp = System.currentTimeMillis();
  }

  public String getStageName() {
    return stageName;
  }

  public void setStageName(String stageName) {
    this.stageName = stageName;
  }

  public Exception getException() {
    return exception;
  }

  public void setException(Exception e) {
    this.exception = exception;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
