package com.kmwllc.brigade.stage;

/**
 * Represents a failure that occurred in a stage during pipeline processing.
 * Holds the following properties:<ul>
 *   <li>stageName - Name of the stage from which the failure occurred</li>
 *   <li>exception - Exception that was thrown by the stage</li>
 *   <li>timestamp - Time the exception occurred (this defaults to current time)</li>
 * </ul>
 * Each document maintains a list of StageFailures as it progresses through the pipeline
 */
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
