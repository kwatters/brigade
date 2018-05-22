package com.kmwllc.brigade.workflow;

import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.event.CallbackListener;
import com.kmwllc.brigade.event.DocumentListener;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Workflow : top level workflow class that controls the thread that do the work
 * of processing documents on each stage.
 */
public class Workflow {

  private final int numWorkerThreads;
  private final int queueLength;
  private final LinkedBlockingQueue<Document> queue;
  private String name = "defaultWorkflow";
  // The workflow has it's own copy of each stage. to avoid thread safety issues
  // when running with more than 1 thread. (todo:review this design pattern for something
  // more thread poolesque?)
  private WorkflowWorker[] workers;
  private WorkflowConfig workflowConfig;
  public final static Logger log = LoggerFactory.getLogger(Workflow.class.getCanonicalName());
  private Map<String, String> props;
  private List<DocumentListener> documentListeners = new ArrayList<>();
  private List<CallbackListener> callbackListeners = new ArrayList<>();

  // constructor
  public Workflow(WorkflowConfig workflowConfig, Map<String, String> props) {
    // create each of the worker threads. each with their own copy of the stages
    numWorkerThreads = workflowConfig.getNumWorkerThreads();
    queueLength = workflowConfig.getQueueLength();
    queue = new LinkedBlockingQueue<>(queueLength);
    this.workflowConfig = workflowConfig;
    // We need to load a config then we need to create each of the stages for the config
    // and add those to our stage list.
    this.name = workflowConfig.getName();
    this.props = props;
  }

  // initialize the workflow
  public void initialize() throws Exception {
    workers = new WorkflowWorker[numWorkerThreads];
    for (int i = 0; i < numWorkerThreads; i++) {
      initializeWorkerThread(i);
    }
  }

  // init the worker threads
  private void initializeWorkerThread(int threadNum) throws Exception {
    WorkflowWorker worker = null;
    try {
      worker = new WorkflowWorker(workflowConfig, queue, props);
    } catch (ClassNotFoundException e) {
      // TODO: better handling?
      log.warn("Error starting the worker thread. {}", e.getLocalizedMessage());
      throw new Exception(e);
    }

    worker.setCallbackListeners(callbackListeners);
    worker.setDocumentListeners(documentListeners);
    worker.setStageExceptionMode(workflowConfig.getStageExecutionMode());

    worker.start();
    workers[threadNum] = worker;
  }

  public void processDocument(Document doc) throws InterruptedException {
//    if (currEx != null) {
//      throw new InterruptedException("Exception occurred");
//    }
    // put the document on the processing queue.
    if (doc != null) {
      queue.put(doc);
    } else {
      queue.put(doc);
    }
  }

  // flush all the stages on each worker thread.
  public void flush() throws Exception {
    for (int i = 0; i < numWorkerThreads; i++) {
      if (workers[i].isError()) {
        log.warn("Workflow worker in error");
        throw new Exception("Workflow worker threw exception");
      }
    }
    while (!queue.isEmpty()) {

    }

    // now wait for the threads to no longer be running
    while (true) {

      boolean oneIsRunning = false;
      for (int i = 0; i < numWorkerThreads; i++) {
        oneIsRunning |= workers[i].isProcessing();
      }
      if (!oneIsRunning) {
        break;
      }
    }
    // Each worker will get flushed. (each worker flushes its stage)
    for (WorkflowWorker worker : workers) {
      if (worker.isError()) {
        throw new InterruptedException("Exception found during workflow.flush");
      }
      worker.flush();
    }
    log.info("Workflow {} flushed.", name);

  }

  public String getName() {
    return name;
  }

  public List<DocumentListener> getDocumentListeners() {
    return documentListeners;
  }

  public void setDocumentListeners(List<DocumentListener> documentListeners) {
    this.documentListeners = documentListeners;
  }

  public List<CallbackListener> getCallbackListeners() {
    return callbackListeners;
  }

  public void setCallbackListeners(List<CallbackListener> callbackListeners) {
    this.callbackListeners = callbackListeners;
  }

  public void addDocumentListener(DocumentListener l) {
    documentListeners.add(l);
  }

  public void addCallbackListener(CallbackListener l) {
    callbackListeners.add(l);
  }
}
