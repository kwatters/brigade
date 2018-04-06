package com.kmwllc.brigade.util;

import com.kmwllc.brigade.Brigade;
import com.kmwllc.brigade.Brigade2;
import com.kmwllc.brigade.config.BrigadeConfig;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.config2.BrigadeConfig2;
import com.kmwllc.brigade.config2.ConfigFactory;
import com.kmwllc.brigade.config2.ConnectorConfig2;
import com.kmwllc.brigade.config2.WorkflowConfig2;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Map;

/**
 * Created by matt on 3/22/17.
 */
public class BrigadeRunner2 {

    private final InputStream workflowFile;
    private final InputStream connectorFile;
    private final InputStream propertiesFile;
    private final String configFormat;

    public BrigadeRunner2(InputStream propertiesFile, InputStream connectorFile, InputStream workflowFile,
                          String configFormat) {
        this.propertiesFile = propertiesFile;
        this.connectorFile = connectorFile;
        this.workflowFile = workflowFile;
        this.configFormat = configFormat;
    }

    public void exec() throws Exception {
        Map<String,String> propMap = null;
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
        workflowString = sub.replace(workflowString);
        workflowString = sub.replaceSystemProperties(workflowString);

        ConfigFactory configFactory = ConfigFactory.instance(configFormat);
        ConnectorConfig2 connectorConfig = configFactory.deserializeConnector(new StringReader(connectorString));
        WorkflowConfig2 workflowConfig = configFactory.deserializeWorkflow(new StringReader(workflowString));

        // init the brigade config!
        BrigadeConfig2 config = new BrigadeConfig2();
        config.addConnectorConfig(connectorConfig);
        config.addWorkflowConfig(workflowConfig);

        // Start up the Brigade Server
        Brigade2 brigadeServer = Brigade2.getInstance();
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
