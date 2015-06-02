package com.kmwllc.brigade.stage;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import com.kmwllc.brigade.document.Document;

public abstract class AbstractStageTest extends TestCase {

	private Document doc;
	private AbstractStage stage;
	public abstract AbstractStage createStage();
	public abstract Document createDocument();
	public abstract void validateDoc(Document doc);
	
	@Before 
	public void setup() {
		// TODO: some base class initialization
	}
	
	@Test
	public void test() {
	
		// Why doesn't the annotation do this?
		setup();
		stage = createStage();
		doc = createDocument();
		stage.processDocument(doc);
		validateDoc(doc);
		
	}

}
