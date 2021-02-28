package com.kmwllc.brigade.stage;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.datavec.api.util.ClassPathResource;
import org.datavec.image.loader.NativeImageLoader;
import org.deeplearning4j.nn.graph.ComputationGraph;

import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;
import org.deeplearning4j.zoo.PretrainedType;
import org.deeplearning4j.zoo.ZooModel;
import org.deeplearning4j.zoo.model.VGG16;
import org.deeplearning4j.zoo.util.ClassPrediction;
import org.deeplearning4j.zoo.util.imagenet.ImageNetLabels;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.api.preprocessor.DataNormalization;
import org.nd4j.linalg.dataset.api.preprocessor.VGG16ImagePreProcessor;
import org.nd4j.linalg.factory.Nd4j;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

public class Deeplearning4j extends AbstractStage {

  // private String modelFilename = "vgg16.zip";
  // private MultiLayerNetwork network;
  private ComputationGraph vgg16;
  private String inputField = "bytes";
  private double confidenceThreshold = 0.75;
  transient private ImageNetLabels imageNetLabels = null;
  @Override
  public void startStage(StageConfig config)  {
    // TODO: load a proper model... o/w first we'll just use vgg16 as an example
    try {
      loadVGG16();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    // load the config.. initial the dl4j network/model
    // configure the input/output fields to use for passing into the model.
    //    modelFilename = config.getStringParam("modelFilename", modelFilename);
    //    
    //    try {
    //      File savedNetwork = new ClassPathResource(modelFilename).getFile();
    //      network = ModelSerializer.restoreMultiLayerNetwork(modelFilename);
    //    } catch (FileNotFoundException e) {
    //      // TODO Auto-generated catch block
    //      e.printStackTrace();
    //    } catch (IOException e) {
    //      // TODO Auto-generated catch block
    //      e.printStackTrace();
    //    }
    
    
    
    

  }

  @Override
  public List<Document> processDocument(Document doc) throws Exception {
    // TODO Auto-generated method stub
    // 
    // TODO: we don't know that it's a buffered image.. but .. let's assume it is for this example stage.
    BufferedImage buffImg = (BufferedImage)doc.getField(inputField).get(0);
    Map<String, Double> results = classifyImageVGG16(buffImg);
    for (String key : results.keySet()) {
      Double confidence = results.get(key);
      if (confidence > confidenceThreshold) {
        doc.setField(key, confidence);
      }
    }
    
    return null;
  }

  @Override
  public void stopStage() {
    // TODO Auto-generated method stub

  }

  @Override
  public void flush() {
    // TODO Auto-generated method stub

  }

  
  // This is for the Model Zoo support to load in the VGG16 model.  
  public void loadVGG16() throws IOException {
    ZooModel zooModel = VGG16.builder().build();
    vgg16 = (ComputationGraph) zooModel.initPretrained(PretrainedType.IMAGENET);
    // TODO: return true/false if the model was loaded properly/successfully.
    imageNetLabels = new ImageNetLabels();
  }
  
  
  
  public Map<String, Double> classifyImageVGG16(BufferedImage buffImg) throws IOException {
    NativeImageLoader loader = new NativeImageLoader(224, 224, 3);
    INDArray image = loader.asMatrix(buffImg);
    // TODO: we should consider the model as not only the model, but also the input transforms
    // for that model.
    DataNormalization scaler = new VGG16ImagePreProcessor();
    scaler.transform(image);
    INDArray[] output = vgg16.output(false,image);
    // TODO: return a more native datastructure!
    //String predictions = TrainedModels.VGG16.decodePredictions(output[0]);
    // log.info("Image Predictions: {}", predictions);
    return decodeVGG16Predictions(output[0]);
  }
  
//adapted from dl4j TrainedModels.VGG16 class.
 public Map<String, Double> decodeVGG16Predictions(INDArray predictions) {
//   LinkedHashMap<String, Double> recognizedObjects = new LinkedHashMap<String, Double>(); 
//   ArrayList<String> labels;
//   String predictionDescription = "";
//   int[] top5 = new int[5];
//   float[] top5Prob = new float[5];
//   labels = ImageNetLabels.getLabels();
//   //brute force collect top 5
//   int i = 0;
//   for (int batch = 0; batch < predictions.size(0); batch++) {
//       if (predictions.size(0) > 1) {
//           predictionDescription += String.valueOf(batch);
//       }
//       predictionDescription += " :";
//       INDArray currentBatch = predictions.getRow(batch).dup();
//       while (i < 5) {
//           top5[i] = Nd4j.argMax(currentBatch, 1).getInt(0, 0);
//           top5Prob[i] = currentBatch.getFloat(batch, top5[i]);
//           // interesting, this cast looses precision.. float to double.
//           recognizedObjects.put(labels.get(top5[i]), (double)top5Prob[i]);
//           currentBatch.putScalar(0, top5[i], 0);
//           predictionDescription += "\n\t" + String.format("%3f", top5Prob[i] * 100) + "%, " + labels.get(top5[i]);
//           i++;
//       }
//   }
   
   LinkedHashMap<String, Double> recognizedObjects = new LinkedHashMap<String, Double>();
   // ArrayList<String> labels;
   List<ClassPrediction> classes = imageNetLabels.decodePredictions(predictions, 10).get(0);
   for (ClassPrediction label : classes) {
     log.info(label.getLabel() + ":" + label.getProbability());
     recognizedObjects.put(label.getLabel(), label.getProbability());
   }
   return recognizedObjects;
 }
  
}
