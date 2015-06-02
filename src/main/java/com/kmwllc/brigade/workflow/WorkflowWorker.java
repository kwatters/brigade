package com.kmwllc.brigade.workflow;

import com.kmwllc.brigade.document.Document;

public class WorkflowWorker extends Thread {

	private Workflow w;
	boolean processing = false;
	
	WorkflowWorker(Workflow workflow) {
		this.w = workflow;
	}

	public void run() {
		Document doc;
		boolean running = true;
		while (running) {
			try {
				doc = w.getDocToProcess();
				if (doc == null) {
					running = false;
				} else {
					processing = true;
					w.processDocumentInternal(doc);
					processing = false;
				}
			} catch (InterruptedException e) {
				// TODO: handle these properly
				e.printStackTrace();
			} 
		}
	}

	public boolean isProcessing() {
		return processing;
	}

}
