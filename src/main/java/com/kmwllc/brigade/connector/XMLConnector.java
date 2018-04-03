package com.kmwllc.brigade.connector;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.connector.xml.ChunkingXMLHandler;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.utils.FileUtils;
import com.kmwllc.brigade.utils.RecordingInputStream;
import org.slf4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.net.URL;
import java.util.regex.Pattern;

public class XMLConnector extends AbstractConnector {

    public final static Logger log = LoggerFactory.getLogger(XMLConnector.class.getCanonicalName());

    private String fileFolder = null;
    private String xmlRootPath = null;
    private String xmlIDPath = null;
    private String docIDPrefix = "doc_";
    private String fileRegex = null;
    private Pattern fileRegexPattern;
    private String urlFile = null;

    @Override
    public void setConfig(ConnectorConfig config) {
        // update the connector config

        this.workflowName = config.getProperty("workflowName", "ingest");
        fileFolder = config.getProperty("fileFolder", fileFolder);
        fileRegex = config.getProperty("fileRegex", fileRegex);
        xmlRootPath = config.getProperty("XmlRootPath", xmlRootPath);
        xmlIDPath = config.getProperty("XmlIDPath", xmlIDPath);
        docIDPrefix = config.getProperty("DocIDPrefix", docIDPrefix);
        urlFile = config.getProperty("urlFile", urlFile);
    }

    @Override
    public void startCrawling() throws Exception {

        // Everything in between
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser saxParser = null;

        try {
            spf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, false);
            saxParser = spf.newSAXParser();
        } catch (ParserConfigurationException | SAXException e) {
            log.warn("SAX Parser Error {}", e);
        }

        try {
            XMLReader xmlReader = saxParser.getXMLReader();
            ChunkingXMLHandler xmlHandler = new ChunkingXMLHandler();
            xmlHandler.setConnector(this);
            xmlHandler.setDocumentRootPath(xmlRootPath);
            xmlHandler.setDocumentIDPath(xmlIDPath);
            xmlHandler.setDocIDPrefix(docIDPrefix);
            xmlReader.setContentHandler(xmlHandler);

            if (urlFile != null) {
            	// We are going to process the file of urls instead of the file directory specficiation.

            	String[] lines = FileUtils.toString(new File(urlFile)).split("\n");
            	for (String line : lines) {
            		line = line.trim();
            		System.out.println("URL TO CRAWL: "+ line);
            		InputStream in = new URL(line).openStream();
            		BufferedInputStream bis = new BufferedInputStream(in);
            		RecordingInputStream ris = new RecordingInputStream(bis);
            		InputSource xmlSource = new InputSource(ris);
            		xmlHandler.setRis(ris);
            		xmlReader.parse(xmlSource);
            		in.close();
            	}
            	
            	
            } else {

            	File xmlDir = new File(fileFolder);
            	File[] filesToParse = xmlDir.listFiles(new FilenameFilter() {
            		@Override
            		public boolean accept(File dir, String name) {
            			return fileRegexPattern.matcher(name).matches();
            		}
            	});

            	for (File file : filesToParse) {
            		log.info("Parsing file: {}", file);
            		FileInputStream fis = new FileInputStream(file);
            		RecordingInputStream ris = new RecordingInputStream(fis);
            		InputSource xmlSource = new InputSource(ris);
            		xmlHandler.setRis(ris);
            		xmlReader.parse(xmlSource);
            		// xmlReader.parse(convertToFileURL(filename));
            	}

            }
        } catch (IOException | SAXException e) {
           log.warn("SAX Parser Error {}", e);
        }

        
        
        flush();

        stopCrawling();
        
    }

    @Override
    public void stopCrawling() {
    	// no op
    	long deltaS = (System.currentTimeMillis() - getStartTime()) / 1000;
    	log.info("Stop Crawling called, Sent {} docs in {} seconds.", getFeedCount() , deltaS);
    }


    @Override
    public void initialize() {
      if (fileRegex != null) {
        fileRegexPattern = Pattern.compile(fileRegex);
      }
    }
}
