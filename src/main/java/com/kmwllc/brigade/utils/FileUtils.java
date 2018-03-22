package com.kmwllc.brigade.utils;

import com.kmwllc.brigade.logging.LoggerFactory;
import org.slf4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Properties;

public class FileUtils {
    private final static Logger log = LoggerFactory.getLogger(FileUtils.class.getCanonicalName());

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
     * @param file - file to read
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

    public static Reader getReader(String fileName) throws Exception {
        File f = _getFile(fileName);
        if (f.isAbsolute()) {
            return new FileReader(f);
        } else {
            InputStream in = _getInputStream(fileName);
            return new InputStreamReader(in);
        }
    }

    public static InputStream getInputStream(String fileName) throws Exception {
        File f = _getFile(fileName);
        if (f.isAbsolute()) {
            return new FileInputStream(f);
        } else {
            InputStream in = _getInputStream(fileName);
            return in;
        }
    }

    private static InputStream _getInputStream(String fileName) throws Exception {
        InputStream in = FileUtils.class.getClassLoader().getResourceAsStream(fileName);
        if (in == null) {
            log.warn(String.format("File (%s) not found in classpath", fileName));
            throw new Exception("File not found in classpath");
        }
        return in;
    }

    private static File _getFile(String fileName) throws Exception {
        File f = new File(fileName);
        if (f.isAbsolute() && !f.exists()) {
            log.warn(String.format("File (%s) not found", fileName));
            throw new Exception("File Not Found");
        }
        return f;
    }


}
