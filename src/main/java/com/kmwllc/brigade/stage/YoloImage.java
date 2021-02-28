package com.kmwllc.brigade.stage;

import static org.bytedeco.opencv.global.opencv_core.CV_32F;
import static org.bytedeco.opencv.global.opencv_dnn.blobFromImage;

import java.util.ArrayList;
import java.util.HashMap;

import org.bytedeco.javacv.OpenCVFrameConverter;
import org.bytedeco.opencv.opencv_core.IplImage;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Rect;
import org.bytedeco.opencv.opencv_core.Scalar;
import org.bytedeco.opencv.opencv_core.Size;
import org.bytedeco.opencv.opencv_dnn.Net;

import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.image.Classification;

public class YoloImage extends AbstractImageStage {

  transient private OpenCVFrameConverter.ToIplImage converterToIpl = new OpenCVFrameConverter.ToIplImage();
  transient private final OpenCVFrameConverter.ToIplImage grabberConverter = new OpenCVFrameConverter.ToIplImage();
  transient private Net net;
  // zero offset to where the confidence level is in the output matrix of the
  // darknet.
  private static final int CONFIDENCE_INDEX = 4;
  private float confidenceThreshold = 0.25F;
  private boolean debug = false;
  ArrayList<String> classNames;
  
  @Override
  public HashMap<String, Object> processImage(Mat inputMat, Document doc) {
    // TODO Auto-generated method stub
    
    // Ok.. let's Yolo away!

    log.debug("Starting yolo on frame...");
    log.info("yoloFrame - begin");
    // this is our list of objects that have been detected in a given frame.
    ArrayList<Classification> yoloObjects = new ArrayList<Classification>();
    // convert that frame to a matrix (Mat) using the frame converters in javacv

    // log.info("Yolo frame start");
  
    // log.info("Input mat created");
    // TODO: I think yolo expects RGB color (which is inverted in the next step)
    // so if the input image isn't in RGB color, we might need a cvCutColor
    log.info("yoloFrame - blobFromImage");
    Mat inputBlob = blobFromImage(inputMat, 1 / 255.F, new Size(416, 416), new Scalar(), true, false, CV_32F);
    // put our frame/input blob into the model.
    // log.info("input blob created");
    log.info("yoloFrame - blob {}", inputBlob);
    net.setInput(inputBlob);

    log.debug("Feed forward!");
    // log.info("Input blob set on network.");
    // ask for the detection_out layer i guess? not sure the details of the
    // forward method, but this computes everything like magic!
    Mat detectionMat = net.forward("detection_out");
    // log.info("output detection matrix produced");
    log.debug("detection matrix computed");
    // iterate the rows of the detection matrix.
    for (int i = 0; i < detectionMat.rows(); i++) {
      Mat currentRow = detectionMat.row(i);
      float confidence = currentRow.getFloatBuffer().get(CONFIDENCE_INDEX);
      if (confidence < confidenceThreshold) {
        // skip the noise
        continue;
      }

      // System.out.println("\nCurrent row has " + currentRow.size().width() +
      // "=width " + currentRow.size().height() + "=height.");
      // currentRow.position(probability_index);
      // int probability_size = detectionMat.cols() - probability_index;
      // detectionMat;

      // String className = getWithDefault(classNames, i);
      // System.out.print("\nROW (" + className + "): " +
      // currentRow.getFloatBuffer().get(4) + " -- \t\t");
      for (int c = CONFIDENCE_INDEX + 1; c < currentRow.size().get(); c++) {
        float val = currentRow.getFloatBuffer().get(c);
        // TODO: this filtering logic is probably wrong.
        if (val > 0.0) {
          String label = classNames.get(c - CONFIDENCE_INDEX - 1);
        
          // System.out.println("Index : " + c + "->" + val + " label : " +
          // classNames.get(c-probability_index) );
          // let's just say this is something we've detected..
          // ok. in theory this is something we think it might actually be.
          float x = currentRow.getFloatBuffer().get(0);
          float y = currentRow.getFloatBuffer().get(1);

          float width = currentRow.getFloatBuffer().get(2);
          float height = currentRow.getFloatBuffer().get(3);
          int xLeftBottom = (int) ((x - width / 2) * inputMat.cols());
          int yLeftBottom = (int) ((y - height / 2) * inputMat.rows());
          int xRightTop = (int) ((x + width / 2) * inputMat.cols());
          int yRightTop = (int) ((y + height / 2) * inputMat.rows());

          if (xLeftBottom < 0) {
            xLeftBottom = 0;
          }
          if (yLeftBottom < 0) {
            yLeftBottom = 0;
          }

          // crop the right top
          if (xRightTop > inputMat.cols()) {
            xRightTop = inputMat.cols();
          }

          if (yRightTop > inputMat.rows()) {
            yRightTop = inputMat.rows();
          }

          log.debug(label + " (" + confidence + "%) [(" + xLeftBottom + "," + yLeftBottom + "),(" + xRightTop + "," + yRightTop + ")]");
          Rect boundingBox = new Rect(xLeftBottom, yLeftBottom, xRightTop - xLeftBottom, yRightTop - yLeftBottom);
          // grab just the bytes for the ROI defined by that rect..
          // get that as a mat, save it as a byte array (png?) other encoding?
          // TODO: have a target size?

          IplImage cropped = extractSubImage(inputMat, boundingBox);
          Classification obj = new Classification(String.format("%s.%s-%d", "data.getName()", "name", "data.getFrameIndex()"));
          obj.setLabel(label);
          obj.setBoundingBox(xLeftBottom, yLeftBottom, xRightTop - xLeftBottom, yRightTop - yLeftBottom);
          obj.setConfidence(confidence);
          // obj.setImage(data.getDisplay());
          // for non-serializable "local" image objects
          // TODO: the original frame as reference? 
          // obj.setObject(frame);
          yoloObjects.add(obj);
        }
      }
    }
    log.info("yoloFrame - end");
    // TODO: change the return type?
    // TODO: map the yolo objects to the return
    HashMap<String, Object> results = new HashMap<String,Object>();
    results.put("yolo", yoloObjects);
    return results;
  }

  private IplImage extractSubImage(Mat inputMat, Rect boundingBox) {
    log.info("extractSubImage - begin");
    //
    log.debug(boundingBox.x() + " " + boundingBox.y() + " " + boundingBox.width() + " " + boundingBox.height());

    // TODO: figure out if the width/height is too large! don't want to go array
    // out of bounds
    Mat cropped = new Mat(inputMat, boundingBox);

    IplImage image = converterToIpl.convertToIplImage(converterToIpl.convert(cropped));
    // This mat should be the cropped image!

    log.info("extractSubImage - end");
    return image;
  }
  
  
  
}
