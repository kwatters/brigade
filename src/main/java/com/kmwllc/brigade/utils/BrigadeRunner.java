package com.kmwllc.brigade.utils;

import com.kmwllc.brigade.Brigade;
import com.kmwllc.brigade.config.BrigadeConfig;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.config.ConfigFactory;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static com.kmwllc.brigade.config.ConfigFactory.JSON;
import static com.kmwllc.brigade.config.ConfigFactory.LEGACY_XML;

/**
 * Created by matt on 3/22/17.
 */
public class BrigadeRunner {

  private final InputStream workflowFile;
  private final InputStream connectorFile;
  private final InputStream propertiesFile;

  public BrigadeRunner(InputStream propertiesFile, InputStream connectorFile, InputStream workflowFile) {
    this.propertiesFile = propertiesFile;
    this.connectorFile = connectorFile;
    this.workflowFile = workflowFile;
  }

  // Naive way to check format.  Good enough for now...
  private String sniffConfigFormat(String s) throws Exception {
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
  private char firstChar(String s){
    char[] chars = s.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      if (!Character.isWhitespace(chars[i])) {
        return chars[i];
      }
    }
    return ' ';
  }

  public void exec() throws Exception {
    Map<String, String> propMap = null;
    try {
      propMap = BrigadeUtils.loadPropertiesAsMap(propertiesFile);
    } catch (IOException e) {
      e.printStackTrace();
    }
    String connectorString = null;
    try {
      connectorString = BrigadeUtils.fileToString(connectorFile);
    } catch (IOException e) {
      e.printStackTrace();
    }
    String workflowString = null;
    try {
      workflowString = BrigadeUtils.fileToString(workflowFile);
    } catch (IOException e) {
      e.printStackTrace();
    }

    StrSubstitutor sub = new StrSubstitutor(propMap);
    connectorString = sub.replace(connectorString);
    connectorString = sub.replaceSystemProperties(connectorString);
    String connectorConfigFormat = sniffConfigFormat(connectorString);

    workflowString = sub.replace(workflowString);
    workflowString = sub.replaceSystemProperties(workflowString);
    String workflowConfigFormat = sniffConfigFormat(workflowString);

    ConfigFactory connectorConfigFactory = ConfigFactory.instance(connectorConfigFormat);
    ConnectorConfig connectorConfig = connectorConfigFactory.deserializeConnector(connectorString);

    ConfigFactory workflowConfigFactory = ConfigFactory.instance(workflowConfigFormat);
    WorkflowConfig workflowConfig = workflowConfigFactory.deserializeWorkflow(workflowString);

    // init the brigade config!
    BrigadeConfig config = new BrigadeConfig();
    config.addConnectorConfig(connectorConfig);
    config.addWorkflowConfig(workflowConfig);
    config.setProps(propMap);

    // Start up the Brigade Server
    Brigade brigadeServer = Brigade.getInstance();
    brigadeServer.setConfig(config);
    try {
      brigadeServer.start();

      if (brigadeServer.isRunning()) {
        try {
          brigadeServer.startConnector(connectorConfig.getConnectorName());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        // TODO: this should do a flush! and then shutdown..
        try {
          brigadeServer.waitForConnector(connectorConfig.getConnectorName());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      brigadeServer.shutdown(false);
      throw e;
    }

    // System.exit(0);
  }
}