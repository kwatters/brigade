package com.kmwllc.brigade.stage.image;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import javax.imageio.ImageIO;
import org.slf4j.Logger;

import com.kmwllc.brigade.document.Document;

public class Classification extends Document {
  
  public Classification(String id) {
    super(id);
    setTs(System.currentTimeMillis());
  }

  public Classification(String id, float confidence, Rectangle rect) {
    super(id);
    setLabel(id);
    setTs(System.currentTimeMillis());
    setConfidence(confidence);
    setBoundingBox(rect);
  }

  public void setTs(long ts) {
    setField("ts", ts);
  }

  public Long getTs() {
    return (Long) getFirstValue("ts");
  }

  public void setConfidence(float confidence) {
    setField("confidence", confidence);
  }

  public Float getConfidence() {
    return (Float) getFirstValue("confidence");
  }

  public void setBoundingBox(int x, int y, int width, int height) {
    setField("bounding_box", new Rectangle(x, y, width, height));
  }

  public Rectangle getBoundingBox() {
    return (Rectangle) getFirstValue("bounding_box");
  }

  public void setLabel(String label) {
    setField("label", label);
  }

  public String getLabel() {
    return (String) getFirstValue("label");
  }

  public void setImage(BufferedImage image) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ImageIO.write(image, "png", baos);
      baos.flush();
      byte[] bytes = baos.toByteArray();
      baos.close();
      setField("image", bytes);
    } catch (Exception e) {
      //log.error("setImage threw", e);
      System.err.print("SetImage blew error: ");
      e.printStackTrace();
    }
  }

  public BufferedImage getImage() {
    try {
      byte[] bytes = (byte[]) getFirstValue("image");
      if (bytes != null) {
        InputStream in = new ByteArrayInputStream(bytes);
        BufferedImage bi = ImageIO.read(in);
        return bi;
      }

    } catch (Exception e) {
      System.err.println("Set image thew");
      e.printStackTrace();
    }
    return null;
  }

  public void setObject(Object frame) {
    setField("imageObject", frame);
  }

  public Object getObject() {
    return getFirstValue("imageObject");
  }

  public void setBoundingBox(Rectangle rect) {
    setField("bounding_box", rect);
  }

}
