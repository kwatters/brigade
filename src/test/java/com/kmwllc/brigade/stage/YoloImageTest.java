package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.json.JsonStageConfig;
import com.kmwllc.brigade.document.Document;

public class YoloImageTest extends AbstractStageTest {

  @Override
  public AbstractStage createStage() {
    // TODO Auto-generated method stub
    YoloImage stage = new YoloImage();
	StageConfig config = new JsonStageConfig();
	//config.setStringParam("source", "foo");
	//config.setStringParam("dest", "bar");
	stage.startStage(config);

    return stage;
  }

  @Override
  public Document createDocument() {
    // TODO Auto-generated method stub
    Document doc = new Document("doc1");
    String filename = "C:/dev/workspace.mrl2/myrobotlab/src/test/resources/OpenCV/FaceRecognizer/Test/1.jpg";
    doc.setField("filename", filename);
    return doc;
  }

  @Override
  public void validateDoc(Document doc) {
    // TODO Auto-generated method stub

    System.out.println("Hello world.");
    
  }

}
