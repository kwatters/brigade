package com.kmwllc.brigade.workflow;

import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.config2.WorkflowConfig2;

import java.util.HashMap;

/**
 * This is the singleton instance of the workflow server.  
 * This is a container for all of the running workflows in the current system.
 * 
 * @author kwatters
 *
 */
public class WorkflowServer2 {

  private static WorkflowServer2 instance = null;

  private HashMap<String, Workflow2> workflowMap;

  // singleton, the constructor is private.
  private WorkflowServer2() {
    workflowMap = new HashMap<>();
  }

  // This is a singleton also
  public static WorkflowServer2 getInstance() {
    if (instance == null) {
      instance = new WorkflowServer2();
      return instance;
    } else {
      return instance;
    }
  }

  public void addWorkflow(WorkflowConfig2 config) throws Exception {
    Workflow2 w = new Workflow2(config);
    w.initialize();
    workflowMap.put(w.getName(), w);
  }

  public void processMessage(WorkflowMessage msg) throws InterruptedException {
    // Handle the message here!
    Workflow2 w = workflowMap.get(msg.getWorkflow());
    w.processDocument(msg.getDoc());
  }

  public void flush(String workflow) throws Exception {
    // flush the given workflow
    Workflow2 w = workflowMap.get(workflow);
    w.flush();
  }

  public String[] listWorkflows() {
    String[] ws = new String[workflowMap.keySet().size()];
    workflowMap.keySet().toArray(ws);
    return ws;
  }

}
