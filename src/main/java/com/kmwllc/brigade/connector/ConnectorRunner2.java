package com.kmwllc.brigade.connector;

import com.kmwllc.brigade.logging.LoggerFactory;
import org.slf4j.Logger;

public class ConnectorRunner2 extends Thread {

    public final static Logger log = LoggerFactory.getLogger(ConnectorRunner2.class.getCanonicalName());

    AbstractConnector2 connector = null;

    public ConnectorRunner2(AbstractConnector2 abstractConnector) {
        connector = abstractConnector;
    }

    @Override
    public void run() {
        super.run();
        try {
            connector.start();
        } catch (InterruptedException e) {
            log.warn("Connector start error {}", e);
            // LOG what if the connector can't start, or is interrupted.
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        // Start the thread
    }

}
