package com.kmwllc.brigade.utils;

import com.kmwllc.brigade.Brigade;
import com.kmwllc.brigade.config.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Represents a Brigade "job" to be run on the Brigade server.  It includes all the config
 * objects which govern execution of the pipeline.
 * <p>
 * Calling exec() will begin execution of the pipeline.  At this point, all the configuration objects
 * are "frozen".  Changes made to them after this point will not be reflected in the pipeline execution.
 * Created by matt on 3/22/17.
 */
public class BrigadeRunner {

  private final BrigadeProperties brigadeProperties;
  private final ConnectorConfig connectorConfig;
  private final WorkflowConfig<StageConfig> workflowConfig;

  /**
   * Convenience invocation of BrigadeRunner which builds from given configurations.  This is how
   * BrigadeRunner is invoked when Brigade is run from the command line.
   * <p>
   * Note that it is not clear whether we are building the configurations from a file or stream and so
   * the following logic is performed for each of propertiesFile, connectorFile, workflowFile:<ul>
   *   <li>If there is a file in the filesystem at the path given, build the config object from the file</li>
   *   <li>If not, attempt to build from a stream of a classpath resource at the given path</li>
   *   <li>An exception is thrown otherwise</li>
   * </ul>
   * @param propertiesFile Path to properties
   * @param connectorFile Path to connector config
   * @param workflowFile Path to workflow config
   * @return BrigadeRunner instance ready to manipulate programmatically or exec
   * @throws IOException If could not locate one of the paths to config object
   * @throws ConfigException If an error occurs while instantiating a config object
   */
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

  /**
   * Begin executing the pipeline based upon configuration objects.  Once this is called, the configuration
   * is "frozen" (ie. config settings cannot be changed programmatically)
   *
   * @throws Exception If an exception occurred while running the pipeline
   */
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
