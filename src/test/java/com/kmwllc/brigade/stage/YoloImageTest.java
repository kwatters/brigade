package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.document.Document;

public class YoloImageTest extends AbstractStageTest {

  @Override
  public AbstractStage createStage() {
    // TODO Auto-generated method stub
    YoloImage stage = new YoloImage();
    return stage;
  }

  @Override
  public Document createDocument() {
    // TODO Auto-generated method stub
    Document doc = new Document("doc1");
//    String filename = "C:/dev/workspace/myrobotlab/src/test/resources/OpenCV/FaceRecognizer/Test/1.jpg";
//    doc.setField("filename", filename);
    return doc;
  }

  @Override
  public void validateDoc(Document doc) {
    // TODO Auto-generated method stub

    System.out.println("Hello world.");
    
  }

}
