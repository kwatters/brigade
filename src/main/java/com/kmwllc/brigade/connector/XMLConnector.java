package com.kmwllc.brigade.connector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.XMLConstants;

import org.slf4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;


import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.connector.xml.ChunkingXMLHandler;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.utils.RecordingInputStream;

public class XMLConnector  extends AbstractConnector {
  
  public final static Logger log = LoggerFactory.getLogger(XMLConnector.class.getCanonicalName());
  
  private String filename = null;
  private String xmlRootPath = null;
  private String xmlIDPath = null;
  private String docIDPrefix = "doc_";

  @Override
  public void setConfig(ConnectorConfig config) {
    // update the connector config
    
    this.workflowName = config.getProperty("workflowName", "ingest");
    filename = config.getProperty("filename", filename);
    xmlRootPath = config.getProperty("XmlRootPath", xmlRootPath);
    xmlIDPath = config.getProperty("XmlIDPath", xmlIDPath);
    docIDPrefix = config.getProperty("DocIDPrefix", docIDPrefix);    
  }
  
  @Override
  public void startCrawling() {
    state = ConnectorState.RUNNING;
    
    // Everything in between
    SAXParserFactory spf = SAXParserFactory.newInstance();
    // spf.setNamespaceAware(false); ? Expose this?
    spf.setNamespaceAware(true);
    SAXParser saxParser = null;
    
    try {
      spf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, false);
      saxParser = spf.newSAXParser();
    } catch (ParserConfigurationException | SAXException e) {
      // TODO Auto-generated catch block
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

      FileInputStream fis = new FileInputStream(new File(filename));
      RecordingInputStream ris = new RecordingInputStream(fis);
      InputSource xmlSource = new InputSource(ris);
      xmlHandler.setRis(ris);

      xmlReader.parse(xmlSource);
      // xmlReader.parse(convertToFileURL(filename));
    } catch (IOException | SAXException e) {
      // TODO Auto-generated catch block
      log.warn("SAX Parser Error {}", e);
    }
    
    state = ConnectorState.STOPPED;
  }

  @Override
  public void stopCrawling() {
    // TODO Auto-generated method stub
    
  }



  @Override
  public void initialize() {
    // TODO Auto-generated method stub
    
  }
}
