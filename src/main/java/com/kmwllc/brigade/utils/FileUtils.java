package com.kmwllc.brigade.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class FileUtils {


  public static Properties loadProperties(String propertiesFile) throws IOException {
    Properties props = new Properties();
    InputStream in = new FileInputStream(propertiesFile);
    props.load(in);
    in.close();
    return props;
  }
  
  public static HashMap<String, String> loadPropertiesAsMap(String propertiesFile) throws IOException {
    Properties props = FileUtils.loadProperties(propertiesFile);
    HashMap<String, String> propMap = new HashMap<String, String>();
    for (Object key : props.keySet()) {
      propMap.put(key.toString(), props.getProperty(key.toString()));
    }
    return propMap;
  }
  
  static public final String toString(File file) throws IOException {
    byte[] bytes = toByteArray(file);
    if (bytes == null) {
      return null;
    }
    return new String(bytes);
  }
  
  /**
   * IntputStream to byte array
   * 
   * @param is
   * @return
   */
  static public final byte[] toByteArray(InputStream is) {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      int nRead;
      byte[] data = new byte[16384];
      while ((nRead = is.read(data, 0, data.length)) != -1) {
        baos.write(data, 0, nRead);
      }

      baos.flush();
      baos.close();
      return baos.toByteArray();
    } catch (Exception e) {
      // TODO: proper log messages
      e.printStackTrace();
    }

    return null;
  }
  /**
   * simple file to byte array
   * 
   * @param file
   *          - file to read
   * @return byte array of contents
   * @throws IOException
   */
  static public final byte[] toByteArray(File file) throws IOException {

    FileInputStream fis = null;
    byte[] data = null;

    fis = new FileInputStream(file);
    data = toByteArray(fis);

    fis.close();

    return data;
  }
  
  static public final String toString(String filename) throws IOException {
    return toString(new File(filename));
  }

  
}
