package com.kmwllc.brigade.stage;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.bytedeco.javacv.OpenCVFrameConverter;
import org.bytedeco.javacv.OpenCVFrameConverter.ToIplImage;
import org.bytedeco.opencv.opencv_core.IplImage;
import org.bytedeco.opencv.opencv_core.Mat;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

public abstract class AbstractImageStage extends AbstractStage {

  private String filenameField = "filename";
  transient private final OpenCVFrameConverter.ToIplImage grabberConverter = new OpenCVFrameConverter.ToIplImage();

  @Override
  public void flush() {
    // TODO Auto-generated method stub

  }

  @Override
  public List<Document> processDocument(Document doc) throws Exception {
    // TODO Auto-generated method stub
    // These stages only operate on images.
    // They will operate on a cv image..  Mat (or maybe a Frame?)  probably a frame.
    if (!isImage(doc))
      return null;
    
    // load a frame from a cvimage.
    // Let's assume the filename is accessable.  
    String filename = doc.getFirstValueAsString(filenameField);
    // TODO: read the file.
    // TODO: are we closing handles here properly?
    byte[] bytes = FileUtils.readFileToByteArray(new File(filename));
    IplImage frame = bytesToImage(bytes);
    Mat inputMat = grabberConverter.convertToMat(grabberConverter.convert(frame));

    // Now we have bytes and an input Mat for yoloness.
    
    // This stage should return metadata about the image.
    HashMap<String, Object> metadata = processImage(inputMat, doc);
    
    for (String key : metadata.keySet()) {
      doc.addToField(key, metadata.get(key));
    }
    // no children docs for now.  
    return null;
    
  }

  public abstract HashMap<String, Object> processImage(Mat inputMat, Document doc);

  /**
   * deserialize from a png byte array to an IplImage
   * 
   * @param bytes
   * @return
   * @throws IOException
   */
  public IplImage bytesToImage(byte[] bytes) throws IOException {
    //
    // let's assume we're a buffered image .. those are serializable :)
    BufferedImage bufImage = ImageIO.read(new ByteArrayInputStream(bytes));
    ToIplImage iplConverter = new OpenCVFrameConverter.ToIplImage();
    Java2DFrameConverter java2dConverter = new Java2DFrameConverter();
    IplImage iplImage = iplConverter.convert(java2dConverter.convert(bufImage));
    // now convert the buffered image to ipl image
    return iplImage;
    // Again this could be try with resources but the original example was in
    // Scala
  }
  
  private boolean isImage(Document doc) {
    // TODO Auto-generated method stub
    // TODO: it'd be nice to have a proper mime type for this..b ut for now
    // simple file extension
    HashSet<String> imageExtensions = new HashSet<String>();
    imageExtensions.add("bmp");
    imageExtensions.add("gif");
    imageExtensions.add("jpg");
    imageExtensions.add("jpeg");
    imageExtensions.add("png");
    imageExtensions.add("tif");
    imageExtensions.add("tiff");

    String filename = doc.getFirstValueAsString(filenameField);
    String ext = FilenameUtils.getExtension(filename);
    
    return imageExtensions.contains(ext);
  }

  @Override
  public void startStage(StageConfig config) {
    // TODO Auto-generated method stub

  }

  @Override
  public void stopStage() {
    // TODO Auto-generated method stub

  }

}
