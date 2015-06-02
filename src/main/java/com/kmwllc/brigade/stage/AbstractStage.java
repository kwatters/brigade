package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfiguration;
import com.kmwllc.brigade.document.Document;

public abstract class AbstractStage {

	public abstract void startStage(StageConfiguration config);
	public abstract void processDocument(Document doc);
	public abstract void stopStage();
	public abstract void flush();

}
