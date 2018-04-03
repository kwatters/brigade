package com.kmwllc.brigade.workflow;

import com.kmwllc.brigade.config.WorkflowConfig;

import java.util.HashMap;

/**
 * This is the singleton instance of the workflow server.  
 * This is a container for all of the running workflows in the current system.
 * 
 * @author kwatters
 *
 */
public class WorkflowServer {

  private static WorkflowServer instance = null;

  private HashMap<String, Workflow> workflowMap;

  // singleton, the constructor is private.
  private WorkflowServer() {
    workflowMap = new HashMap<>();
  }

  // This is a singleton also
  public static WorkflowServer getInstance() {
    if (instance == null) {
      instance = new WorkflowServer();
      return instance;
    } else {
      return instance;
    }
  }

  public void addWorkflow(WorkflowConfig config) throws Exception {
    Workflow w = new Workflow(config);
    w.initialize();
    workflowMap.put(w.getName(), w);
  }

  public void processMessage(WorkflowMessage msg) throws InterruptedException {
    // Handle the message here!
    Workflow w = workflowMap.get(msg.getWorkflow());
    w.processDocument(msg.getDoc());
  }

  public void flush(String workflow) throws Exception {
    // flush the given workflow
    Workflow w = workflowMap.get(workflow);
    w.flush();
  }

  public String[] listWorkflows() {
    String[] ws = new String[workflowMap.keySet().size()];
    workflowMap.keySet().toArray(ws);
    return ws;
  }

}
