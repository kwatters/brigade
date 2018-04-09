package com.kmwllc.brigade.util;

import org.junit.rules.ExternalResource;

import java.io.InputStream;

/**
 * Created by matt on 3/27/17.
 */
public class BrigadeHelper2 extends ExternalResource {

    private BrigadeRunner2 brigadeRunner;

    private final String workflowFile;
    private final String connectorFile;
    private final String propertiesFile;

    public BrigadeHelper2(String propertiesFile, String connectorFile, String workflowFile) {
        this.propertiesFile = propertiesFile;
        this.connectorFile = connectorFile;
        this.workflowFile = workflowFile;
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        brigadeRunner = new BrigadeRunner2(getStream(propertiesFile), getStream(connectorFile),
                getStream(workflowFile));
    }

    private InputStream getStream(String fileName) {
        return BrigadeHelper2.class.getClassLoader().getResourceAsStream(fileName);
    }

    @Override
    protected void after() {
        // Nothing to do?
        super.after();
    }

    public void exec() throws Exception {
        brigadeRunner.exec();
    }
}
