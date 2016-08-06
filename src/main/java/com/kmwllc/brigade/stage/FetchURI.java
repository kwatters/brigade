package com.kmwllc.brigade.stage;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

/**
 * This stage will fetch a web page defined by the uriField and store its byte
 * array in the bytesField.
 * 
 * @author kwatters
 *
 */
public class FetchURI extends AbstractStage {

  private String uriField = "uri";
  private String bytesField = "bytes";

  @Override
  public void startStage(StageConfig config) {
    if (config != null) {
      uriField = config.getProperty("uriField", "uri");
      bytesField = config.getProperty("bytesField", "bytes");
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    // TODO: support https and other protocols
    for (Object o : doc.getField(uriField)) {
      byte[] page;
      try {
        page = fetchUrlAsByteArray(o.toString());
        doc.addToField(bytesField, page);
      } catch (IOException e) {
        log.warn("IO Exception in Flush URI : {}", e);
        continue;
      }
    }
    return null;
  }

  private byte[] fetchUrlAsByteArray(String uri) throws IOException {
    URL url = new URL(uri);
    InputStream in = null;
    in = url.openStream();
    DataInputStream dis = new DataInputStream(new BufferedInputStream(in));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    IOUtils.copy(dis, baos);
    return baos.toByteArray();
  }

  @Override
  public void stopStage() {
    // no-op for this stage

  }

  @Override
  public void flush() {
    // no-op for this stage

  }

  public String getUriField() {
    return uriField;
  }

  public void setUriField(String uriField) {
    this.uriField = uriField;
  }

  public String getBytesField() {
    return bytesField;
  }

  public void setBytesField(String bytesField) {
    this.bytesField = bytesField;
  }

}
