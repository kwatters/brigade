package com.kmwllc.brigade.workflow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.document.ProcessingStatus;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.stage.AbstractStage;

import org.slf4j.Logger;

/**
 * 
 * WorkflowWorker : this is a list of stages that will poll the workflow queue
 * and process documents through that list of stages.
 */
public class WorkflowWorker extends Thread {
  public final static Logger log = LoggerFactory.getLogger(WorkflowWorker.class);
  boolean processing = false;
  private ArrayList<AbstractStage> stages;

  private final LinkedBlockingQueue<Document> queue;

  WorkflowWorker(WorkflowConfig workflowConfig, LinkedBlockingQueue<Document> queue) throws ClassNotFoundException {
    // set the thread name
    this.setName("WorkflowWorker-" + workflowConfig.getName());
    this.queue = queue;
    stages = new ArrayList<AbstractStage>();
    for (StageConfig stageConf : workflowConfig.getStages()) {
      String stageClass = stageConf.getStageClass().trim();
      String stageName = stageConf.getStageName();
      log.info("Starting stage: {} class: {}", stageName, stageClass);
      Class<?> sc = Workflow.class.getClassLoader().loadClass(stageClass);
      try {
        AbstractStage stageInst = (AbstractStage) sc.newInstance();
        stageInst.startStage(stageConf);
        addStage(stageInst);
      } catch (InstantiationException e) {
        log.warn("Error Creating Stage : {}", e);
      } catch (IllegalAccessException e) {
        log.warn("Error Creating Stage : {}", e);
      }
    }
  }

  public void run() {
    Document doc;
    boolean running = true;
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
          processing = false;
        }
      } catch (InterruptedException e) {
        // TODO: handle these properly
        log.warn("Workflow Worker Died! {}", e.getMessage());
        e.printStackTrace();
      }
    }
  }

  public boolean isProcessing() {
    return processing;
  }

  public void processDocumentInternal(Document doc, int stageOffset) {
    // TODO: what to do...
    int i = 0;
    for (AbstractStage s : stages.subList(i, stages.size())) {
      // create a pool of stages, so that when you call processDocument
      // or each thread should have it's own pool?
      List<Document> childDocs = s.processDocument(doc);
      i++;
      if (childDocs != null) {
        // process each of the children docs down the rest of the pipeline
        for (Document childDoc : childDocs) {
          processDocumentInternal(childDoc, i);
        }
      }
      // TODO:should I create a completely new concept for
      // callbacks?
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
    for (AbstractStage s : stages) {
      s.flush();
    }
  }

}
