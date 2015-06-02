package com.kmwllc.brigade.config;

import java.util.ArrayList;

import com.kmwllc.brigade.stage.SendToSolr;
import com.kmwllc.brigade.stage.SetStaticFieldValue;

public class WorkflowConfiguration extends Configuration {

	ArrayList<StageConfiguration> stageConfs;
	
	private String name = "defaultWorkflow";
	public WorkflowConfiguration() {
		// TODO Auto-generated constructor stub
		stageConfs = new ArrayList<StageConfiguration>();
		// default workflow static config
	}
	
	public void addStageConfig(StageConfiguration config) {
		stageConfs.add(config);
	}

	public ArrayList<StageConfiguration> getStageConfs() {
		// TODO Auto-generated method stub
		return stageConfs;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
