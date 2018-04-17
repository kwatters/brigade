package com.kmwllc.brigade;

import com.kmwllc.brigade.utils.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by matt on 3/27/17.
 */
public class BrigadeUtils {

    public static Map<String, String> loadPropertiesAsMap(String fname) throws IOException {
        Properties props = new Properties();
    	InputStream in = null;
    	File f = new File(fname);
    	if (f.exists()) {
        	// try to load it directly as a file.
    		in = new FileInputStream(new File(fname)); 
    	} else {
        	// if not. try the classpath.
        	in = BrigadeUtils.class.getClassLoader().getResourceAsStream(fname);
        }
        props.load(in);
        in.close();
        Map<String, String> output = new HashMap<>();
        for (Object key : props.keySet()) {
            output.put(key.toString(), props.getProperty(key.toString()));
        }
        return output;
    }

    public static String fileToString(String fname) throws IOException {
    	InputStream in = null;
    	File f = new File(fname);
    	if (f.exists()) {
        	// try to load it directly as a file.
    		in = new FileInputStream(new File(fname)); 
    	} else {
        	// if not. try the classpath.
        	in = BrigadeUtils.class.getClassLoader().getResourceAsStream(fname);
        }
        byte[] bytes = FileUtils.toByteArray(in);
        in.close();
        if (bytes == null) {
            return null;
        }
        return new String(bytes);
    }
}
