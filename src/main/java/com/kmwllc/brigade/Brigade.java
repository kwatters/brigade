package com.kmwllc.brigade;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.xml.XmlConfiguration;

import com.kmwllc.brigade.config.BrigadeConfiguration;
import com.kmwllc.brigade.config.Configuration;
import com.kmwllc.brigade.config.ConnectorConfiguration;
import com.kmwllc.brigade.config.StageConfiguration;
import com.kmwllc.brigade.config.WorkflowConfiguration;
import com.kmwllc.brigade.connector.AbstractConnector;
import com.kmwllc.brigade.connector.ConnectorServer;
import com.kmwllc.brigade.workflow.Workflow;
import com.kmwllc.brigade.workflow.WorkflowServer;

public class Brigade {

	// brigade is a singleton server instance
	private static Brigade brigadeServer = null;
	private BrigadeConfiguration config = null;
	private boolean running = false;
	private Server webServer = null;

	private Brigade() {
		// Don't allow it to be instantiated directly.
	}

	private void loadConfig() throws ClassNotFoundException {
		// TODO : Load an actual config file.
		// we should potentially have an xstream serialized version of a brigade config.
		// The config can be a workflow and a connector config

		// Add and initialize all workflows
		WorkflowServer ws = WorkflowServer.getInstance();
		for (WorkflowConfiguration wC : config.getWorkflowConfigs()) {
			ws.addWorkflow(wC);
		}

		// Add all connector configs
		// Add and initialize all workflows
		ConnectorServer cS = ConnectorServer.getInstance();
		for (ConnectorConfiguration cc : config.getConnectorConfigs()) {
			// Add
			// Shit need to create a connector server.
			// Connector w = new Workflow(wc);
			// WorkflowServer ws = WorkflowServer.getInstance();
			// ws.addWorkflow("ingest", w);

			cS.addConnector(cc);
		}
	}

	public static Brigade getInstance() {
		if (brigadeServer == null) {
			brigadeServer = new Brigade();
		}
		return brigadeServer;
	}

	public void start() {
		// TODO: load config
		// start the server .. etc.. etc..
		// Add a default connector

		if (config == null) {
			// create a default config.
			config = createBrigadeConfiguration();
		}

		try {
			loadConfig();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// initalize the connectors and workflows here?

		running = true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// Start up the Brigade Server
		Brigade brigadeServer = Brigade.getInstance();

		BrigadeConfiguration config = createBrigadeConfiguration();
		brigadeServer.setConfig(config);
	
		brigadeServer.start();

		String homeDir = ".";
		String host = null;
		String port = null;
		// String host = "127.0.0.1";
		// String port = "8080";
		try {
			System.out.println("Starting Jetty...");
			brigadeServer.startJetty(homeDir, host, port);
			if (host == null) {
				host = "localhost";
			} 
			if (port == null) {
				port = "8080";
			}
			System.out.println("Jetty Started... http://" + host + ":" + port);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Lets start the brigade server up
		// and leave it running.

		brigadeServer.run();

		// brigadeServer.shutdown();

	}

	private static BrigadeConfiguration createBrigadeConfiguration() {
		// TODO Auto-generated method stub
		BrigadeConfiguration bc = new BrigadeConfiguration();

		WorkflowConfiguration wC = new WorkflowConfiguration();
		wC.setName("ingest");

		StageConfiguration s1Conf = new StageConfiguration();
		s1Conf.setStageClass("com.kmwllc.brigade.stage.SetStaticFieldValue");
		s1Conf.setStageName("set title");
		s1Conf.setStringParam("fieldName", "title");
		s1Conf.setStringParam("value", "Hello World.");

		StageConfiguration s2Conf = new StageConfiguration();
		s2Conf.setStageClass("com.kmwllc.brigade.stage.SetStaticFieldValue");
		s2Conf.setStageName("set title");
		s2Conf.setStringParam("fieldName", "text");
		s2Conf.setStringParam("value", "Welcome to Brigade.");

		StageConfiguration s3Conf = new StageConfiguration();
		s3Conf.setStageName("Solr Sender");
		s3Conf.setStageClass("com.kmwllc.brigade.stage.SendToSolr");
		s3Conf.setStringParam("solrUrl", "http://localhost:8983/solr");
		s3Conf.setStringParam("idField", "id");

		wC.addStageConfig(s1Conf);
		wC.addStageConfig(s2Conf);
		wC.addStageConfig(s3Conf);
		// Create a workflow

		ConnectorConfiguration cC = new ConnectorConfiguration();
		cC.setConnectorName("testConnector");
		cC.setWorkflow("ingest");
		cC.setStringParam("stop", "100000");
		cC.setConnectorClass("com.kmwllc.brigade.connector.DocumentSequenceConnector");

		bc.addWorkflowConfig(wC);
		bc.addConnectorConfig(cC);

		return bc;
	}

	private void run() {
		// TODO Auto-generated method stub
		// This should block for us.
		try {
			webServer.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println("Interrupted...");
			running = false;
			// e.printStackTrace();
		}
	}

	private boolean startJetty(String homeDir, String port, String host)
			throws Exception {
		// TODO start the jetty server for the admin gui

		// connfigure home, host and port. jetty.xml will consume these system
		// properties.

		System.setProperty("jetty.home", homeDir);

		if (host != null) {
			host = host.trim();
			if (host.length() > 0 && !host.equals("0.0.0.0")) {
				System.setProperty("jetty.host", host);
				host = null;
			}
		}

		if (port != null && port.trim().length() > 0) {
			System.setProperty("jetty.port", port);
		}

		// if the jetty config file isn't in the config dir, create it
		File jettyFile = new File(homeDir, "/etc/jetty.xml");
		if (!jettyFile.exists()) {
			ClassLoader loader = this.getClass().getClassLoader();
			InputStream in = loader.getResourceAsStream("default_jetty.xml");
			writeStreamToFile(in, jettyFile);
		}
		File defFile = new File(homeDir, "/etc/webdefault.xml");
		if (!defFile.exists()) {
			ClassLoader loader = this.getClass().getClassLoader();
			InputStream in = loader
					.getResourceAsStream("default_webdefault.xml");
			writeStreamToFile(in, defFile);
		}

		// create and configure the jetty server
		InputStream in = new FileInputStream(jettyFile);

		// System.out.println("PORT : " + System.getProperty("jetty.port"));
		// System.out.println("HOST : " + System.getProperty("jetty.host"));

		XmlConfiguration configuration = new XmlConfiguration(in);
		webServer = new Server();
		configuration.configure(webServer);
		in.close();

		// Install the admin ui
		// BrigadeAdmin admin = new BrigadeAdmin();

		// webServer.setHandler(admin);

		// Install the admin gui..
		WebAppContext webapp = new WebAppContext();
		webapp.setContextPath("/");
		String warPath = homeDir + "/webapps/BrigadeAdmin.war";
		webapp.setWar(warPath);
		System.out.println("Deployed Handler " + warPath);
		webServer.setHandler(webapp);
		webServer.start();
		// Do I need to call this?
		System.out.println("Calling start on web app");
		webapp.start();
		System.out.println("Called start on web app");
		// This join will cause us to block, so lets not do that.
		// webServer.join();

		return true;

	}

	public static void writeStreamToFile(InputStream in, File file)
			throws IOException {
		file.getParentFile().mkdirs();
		byte[] bytes = IOUtils.toByteArray(in);
		in.close();
		FileOutputStream fos = new FileOutputStream(file);
		fos.write(bytes, 0, bytes.length);
		fos.close();
	}

	public void shutdown() {
		// TODO Auto-generated method stub
		// when we're done , shutdown the web server
		System.out.println("Shutting down.");
		stopJetty();
		running = false;
		System.out.println("Shut down.");

		// We are done, exit
		System.exit(0);
	}

	private void stopJetty() {
		// TODO Auto-generated method stub
		try {
			webServer.stop();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
		System.out.println("Stopped jetty...");
	}

	public Configuration getConfig() {
		return config;
	}

	public void setConfig(BrigadeConfiguration config) {
		this.config = config;
	}

	public boolean isRunning() {
		return running;
	}

	public void addConnector(ConnectorConfiguration connectorConfig) {
		config.addConnectorConfig(connectorConfig);
	}

	public void addWorkflow(WorkflowConfiguration workflowConfig) {
		config.addWorkflowConfig(workflowConfig);
	}
	
	public void startConnector(String connectorName)
			throws InterruptedException {
		// TODO Auto-generated method stub
		// This needs to fire off a job.
		// Thread me .. join later
		ConnectorServer cS = ConnectorServer.getInstance();
		if (cS.hasConnector(connectorName)) {
			System.out.println("Called start connector : " + connectorName);
			// TODO: move the start call to the connector server class
			// TODO: also this is currently synchronous .. we want this to be a
			// message
			// so we don't block the ui here. (maybe?)
			// cS.getConnector(connectorName).start();
			cS.startConnector(connectorName);
			System.out.println("Connector started.");
		} else {
			System.out.println("Unknown connector : " + connectorName);
		}
	}

	public String[] listConnectors() {
		// TODO: just expose the connector server to the UI.
		// don't put all these UI specific methods on the brigade main app
		// class.
		System.out.println("List connectord called...");
		return ConnectorServer.getInstance().listConnectors();
	}

	public String[] listWorkflows() {
		// TODO: just expose the connector server to the UI.
		// don't put all these UI specific methods on the brigade main app
		// class.
		System.out.println("List workflows called");
		return WorkflowServer.getInstance().listWorkflows();
	}

}
