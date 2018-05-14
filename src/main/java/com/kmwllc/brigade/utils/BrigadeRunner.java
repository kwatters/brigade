package com.kmwllc.brigade.utils;

import com.kmwllc.brigade.Brigade;
import com.kmwllc.brigade.config.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by matt on 3/22/17.
 */
public class BrigadeRunner {

  private final BrigadeProperties brigadeProperties;
  private final ConnectorConfig connectorConfig;
  private final WorkflowConfig<StageConfig> workflowConfig;

  public static BrigadeRunner init(String propertiesFile, String connectorFile, String workflowFile)
          throws IOException, ConfigException {
    BrigadeProperties bp = null;
    ConnectorConfig cc = null;
    WorkflowConfig<StageConfig> wc = null;
    if (isFile(propertiesFile)) {
      bp = BrigadeProperties.fromFile(propertiesFile);
    } else {
      bp = BrigadeProperties.fromStream(getStream(propertiesFile));
    }
    // bootstrap existing properties
    if (isFile(propertiesFile)) {
      bp = BrigadeProperties.fromFile(propertiesFile, bp);
    } else {
      bp = BrigadeProperties.fromStream(getStream(propertiesFile), bp);
    }

    if (isFile(connectorFile)) {
      cc = ConnectorConfig.fromFile(connectorFile, bp);
    } else {
      cc = ConnectorConfig.fromStream(getStream(connectorFile), bp);
    }

    if (isFile(workflowFile)) {
      wc = WorkflowConfig.fromFile(workflowFile, bp);
    } else {
      wc = WorkflowConfig.fromStream(getStream(workflowFile), bp);
    }

    return new BrigadeRunner(bp, cc, wc);
  }

  public BrigadeRunner(BrigadeProperties brigadeProperties, ConnectorConfig connectorConfig,
                       WorkflowConfig<StageConfig> workflowConfig) {
    this.brigadeProperties = brigadeProperties;
    this.connectorConfig = connectorConfig;
    this.workflowConfig = workflowConfig;
  }

  private static boolean isFile(String fileName) {
    File f = new File(fileName);
    return f.exists();
  }

  private static InputStream getStream(String fileName) {
    InputStream in =  BrigadeRunner.class.getClassLoader().getResourceAsStream(fileName);
    return in;
  }

  public void exec() throws Exception {

    // init the brigade config!
    BrigadeConfig config = new BrigadeConfig();
    config.addConnectorConfig(connectorConfig);
    config.addWorkflowConfig(workflowConfig);
    config.setProps(brigadeProperties);

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
          throw e;
        }

        // TODO: this should do a flush! and then shutdown..
        try {
          brigadeServer.waitForConnector(connectorConfig.getConnectorName());
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw e;
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
