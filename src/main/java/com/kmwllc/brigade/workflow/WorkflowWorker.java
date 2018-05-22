package com.kmwllc.brigade.workflow;

import com.google.common.base.Strings;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.document.ProcessingStatus;
import com.kmwllc.brigade.event.CallbackListener;
import com.kmwllc.brigade.event.DocumentListener;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.stage.AbstractStage;
import com.kmwllc.brigade.stage.Stage;
import com.kmwllc.brigade.stage.StageExceptionMode;
import com.kmwllc.brigade.stage.StageFailure;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static com.kmwllc.brigade.stage.StageExceptionMode.NEXT_STAGE;
import static com.kmwllc.brigade.stage.StageExceptionMode.NEXT_DOC;
import static com.kmwllc.brigade.stage.StageExceptionMode.STOP_WORKFLOW;

/**
 * WorkflowWorker : this is a list of stages that will poll the workflow queue
 * and process documents through that list of stages.
 */
public class WorkflowWorker extends Thread {
  public final static Logger log = LoggerFactory.getLogger(WorkflowWorker.class.getCanonicalName());
  boolean processing = false;
  boolean running = false;
  boolean error = false;
  private List<Stage> stages;
  private Map<String, String> props;
  private List<DocumentListener> documentListeners = new ArrayList<>();
  private List<CallbackListener> callbackListeners = new ArrayList<>();
  private StageExceptionMode stageExceptionMode;

  private final LinkedBlockingQueue<Document> queue;


  WorkflowWorker(WorkflowConfig<StageConfig> workflowConfig, LinkedBlockingQueue<Document> queue,
                 Map<String, String> props) throws ClassNotFoundException {
    // set the thread name
    this.setName("WorkflowWorker-" + workflowConfig.getName());
    this.queue = queue;
    this.props = props;
    stages = workflowConfig.getStages();
  }

  @Override
  public void run() {
    Document doc = null;
    running = true;
    while (running) {
      try {
        doc = queue.take();
        // when can this case happen
        if (doc == null) {
          log.info("Doc was null from workflow queue. setting running to false.");
          running = false;
        } else {
          processing = true;
          // process from the start of the workflow
          processDocumentInternal(doc, 0);

          if (doc.hasFailures()) {
            fireDocFail(doc.getId(), doc.getFailures());
          } else {
            fireDocComplete(doc.getId());
          }
          // On document is fired regardless of whether failures occurred
          fireOnDocument(doc);
          processing = false;
        }
      } catch (Exception e) {
        // TODO: handle these properly
        log.warn("Workflow Worker Died! {}", e);
        //e.printStackTrace();
        running = false;
        processing = false;
        error = true;
        queue.clear();
      }
    }
  }

  public boolean isProcessing() {
    return processing;
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isError() {
    return error;
  }



  public void processDocumentInternal(Document doc, int stageOffset) throws Exception {
    // TODO: what to do...
    int i = 0;
    for (Stage s : stages.subList(i, stages.size())) {
      if (!prereq(s, props, doc)) {
        continue;
      }

      // Override only if not set
      if (s.getStageExceptionMode() == null) {
        s.setStageExceptionMode(stageExceptionMode);
      }

      // create a pool of stages, so that when you call processDocument
      // or each thread should have it's own pool?
      List<Document> childDocs = null;
      synchronized (doc) {
        try {
          childDocs = s.processDocument(doc);
        } catch (Exception e) {
          StageFailure sf = new StageFailure(s.getName(), e);
          doc.addFailure(sf);
          if (s.getStageExceptionMode().equals(NEXT_STAGE)) {
            continue;
          } else if (s.getStageExceptionMode().equals(STOP_WORKFLOW)) {
            // this will ultimately trigger the connector thread to be interrupted
            fireDocFail(doc.getId(), doc.getFailures());
            fireOnDocument(doc);
            throw new Exception(e);
          } else if (s.getStageExceptionMode().equals(NEXT_DOC)) {
            doc.setStatus(ProcessingStatus.DROP);
            return;
          }
        }
      }
      i++;
      if (childDocs != null) {
        // process each of the children docs down the rest of the pipeline
        for (Document childDoc : childDocs) {
          processDocumentInternal(childDoc, i);
        }
      }
      // TODO:should I create a completely new concept for callbacks?
      if (doc.getStatus().equals(ProcessingStatus.DROP)) {
        // if it's a drop, break here.
        break;
      }
    }
  }

  public void addStage(AbstractStage stage) {
    stages.add(stage);
  }

  public void flush() {
    for (Stage s : stages) {
      s.flush();
    }
  }

  private boolean prereq(Stage s, Map<String, String> props, Document d) {
    String enabledProp = s.getEnabled();
    boolean propDisabled = !Strings.isNullOrEmpty(enabledProp) && props.get(enabledProp) != null
            && props.get(enabledProp).equalsIgnoreCase("false");
    if (propDisabled) {
      return false;
    }
    String skipIfField = s.getSkipIfField();
    boolean fieldDisabled = !Strings.isNullOrEmpty(skipIfField) && d.hasField(skipIfField)
            && d.getField(skipIfField).get(0).toString().equalsIgnoreCase("true");
    return !fieldDisabled;
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

  private void fireDocComplete(String docId) {
    callbackListeners.forEach(l -> l.docComplete(docId));
  }

  private void fireDocFail(String docId, List<StageFailure> failures) {
    callbackListeners.forEach(l -> l.docFail(docId, failures));
  }

  private void fireOnDocument(Document doc) {
    documentListeners.forEach(l -> l.onDocument(doc));
  }

  public StageExceptionMode getStageExceptionMode() {
    return stageExceptionMode;
  }

  public void setStageExceptionMode(StageExceptionMode stageExceptionMode) {
    this.stageExceptionMode = stageExceptionMode;
  }
}
