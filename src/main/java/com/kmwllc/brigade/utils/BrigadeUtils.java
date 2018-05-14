package com.kmwllc.brigade.utils;

import com.kmwllc.brigade.config.BrigadeProperties;
import com.kmwllc.brigade.config.ConfigException;
import com.kmwllc.brigade.config.ConfigFactory;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Optional;
import java.util.Properties;

import static com.kmwllc.brigade.config.ConfigFactory.JSON;
import static com.kmwllc.brigade.config.ConfigFactory.LEGACY_XML;

/**
 * Created by matt on 3/27/17.
 */
public class BrigadeUtils {

    public static BrigadeProperties loadPropertiesAsMap(InputStream in) throws IOException {
        Properties props = new Properties();
        props.load(in);
        in.close();
        return getBrigadeProperties(props);
    }

    public static BrigadeProperties getBrigadeProperties(Properties props) {
        BrigadeProperties output = new BrigadeProperties();
        for (Object key : props.keySet()) {
            String k = key.toString();
            String propValue = props.getProperty(key.toString());
            if (System.getProperty(k) != null) {
                propValue = System.getProperty(k);
            }

            output.put(k, propValue);
        }
        return output;
    }

    public static BrigadeProperties loadPropertiesAsMap(Reader r) throws IOException {
        Properties props = new Properties();
        props.load(r);
        r.close();
        return getBrigadeProperties(props);
    }

    public static String fileToString(InputStream in) throws IOException {
        byte[] bytes = FileUtils.toByteArray(in);
        in.close();
        if (bytes == null) {
            return null;
        }
        return new String(bytes);
    }

    // Naive way to check format.  Good enough for now...
    public static String sniffConfigFormat(String s) throws Exception {
        char first = firstChar(s);
        switch (first) {
            case '<':
                return LEGACY_XML;
            case '{':
                return JSON;
            default:
                throw new Exception("Unknown config format");
        }
    }

    // Get the first non-whitespace char
    private static char firstChar(String s){
        char[] chars = s.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (!Character.isWhitespace(chars[i])) {
                return chars[i];
            }
        }
        return ' ';
    }


    public static String expandProps(String s, BrigadeProperties properties) {
        StrSubstitutor sub = new StrSubstitutor(properties);
        s = sub.replace(s);
        s = sub.replaceSystemProperties(s);
        return s;
    }

    public static Optional<String> getConfigAsString(InputStream in, Optional<BrigadeProperties> properties) {
        Optional<String> connectorString = Optional.empty();
        try {
            final String origString = BrigadeUtils.fileToString(in);
            connectorString = properties.map(p -> BrigadeUtils.expandProps(origString, p));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return connectorString;
    }

    public static ConfigFactory getConfigFactory(String cs) throws ConfigException {
        String configFormat = null;
        try {
            configFormat = sniffConfigFormat(cs);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ConfigFactory.instance(configFormat);
    }
}
