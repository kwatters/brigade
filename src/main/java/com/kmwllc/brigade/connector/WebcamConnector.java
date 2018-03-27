package com.kmwllc.brigade.connector;

import java.awt.image.BufferedImage;

import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.bytedeco.javacv.VideoInputFrameGrabber;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;

public class WebcamConnector extends AbstractConnector {

  private int cameraIndex = 0;
  private FrameGrabber grabber;
  private int frameNumber = 0;
  
  @Override
  public void setConfig(ConnectorConfig config) {
    // TODO Auto-generated method stub
    cameraIndex = config.getIntegerParam("cameraIndex", cameraIndex);

  }

  @Override
  public void initialize() {
    
    try {
      grabber = new VideoInputFrameGrabber(cameraIndex);
      grabber.start();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } 
  }

  
  @Override
  public void startCrawling() throws Exception {
    //
    while (ConnectorState.RUNNING.equals(getState())) {
      Frame f = grabber.grab();
      Java2DFrameConverter converter = new Java2DFrameConverter();
      BufferedImage img = converter.convert(f);
      frameNumber++;
      // we got a frame.. let's create a document with it
      Document doc = new Document(docIdPrefix + frameNumber);
      doc.setField("bytes", img);
      doc.setField("width", img.getWidth());
      doc.setField("height", img.getHeight());
      doc.setField("color_model", img.getColorModel());
      feed(doc);
    }
  }

  @Override
  public void stopCrawling() {
    // 
    setState(ConnectorState.STOPPED);
    try {
      grabber.close();
    } catch (org.bytedeco.javacv.FrameGrabber.Exception e) {
      e.printStackTrace();
    }
  }

}
