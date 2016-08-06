package com.kmwllc.brigade;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.slf4j.Logger;

import com.kmwllc.brigade.config.BrigadeConfig;
import com.kmwllc.brigade.config.Config;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import com.kmwllc.brigade.connector.ConnectorServer;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.utils.FileUtils;
import com.kmwllc.brigade.workflow.WorkflowServer;

public class Brigade {

  public final static Logger log = LoggerFactory.getLogger(Brigade.class.getCanonicalName());
	// brigade is a singleton server instance
	private static Brigade brigadeServer = null;
	private BrigadeConfig config = null;
	private boolean running = false;
	private Server webServer = null;

	private Brigade() {
		// Don't allow it to be instantiated directly.
	}

	private void loadConfig() throws ClassNotFoundException {
		// we have an xstream serialized version of a brigade config.
		// The config can be a workflow and a connector config

		// Add and initialize all workflows
		WorkflowServer ws = WorkflowServer.getInstance();
		for (WorkflowConfig wC : config.getWorkflowConfigs()) {
			ws.addWorkflow(wC);
		}

		// Add all connector configs
		// Add and initialize all workflows
		ConnectorServer cS = ConnectorServer.getInstance();
		for (ConnectorConfig cc : config.getConnectorConfigs()) {
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
		  log.warn("Error config was null!");
		  System.exit(-1);
			//config = createBrigadeConfiguration();
		}

		try {
			loadConfig();
		} catch (ClassNotFoundException e) {
		  log.warn("Load config error : {}", e );
		}

		// initalize the connectors and workflows here?
		running = true;
	}

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws InterruptedException, IOException, ParseException {

    // create Options object
    Options options = new Options();

    options.addOption("c", true, "specify the connector config file.");
    options.addOption("w", true, "specify the workflow config file.");
    options.addOption("p", true, "specify the properties file.");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse( options, args);

    // validate command line args
    if(cmd.hasOption("h") || !(cmd.hasOption("c") && cmd.hasOption("w") && cmd.hasOption("p") ) ) {
        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "java -jar brigade.jar -c connector.xml -w workflow.xml -p brigade.properties", options );
        System.exit(1);
    }

    // set the params
    //        TestUtils.initEnvirionment();
    
    int connectorBatchSize = 5000;
    String propertiesFile = cmd.getOptionValue("p");
    String connectorFile = cmd.getOptionValue("c");
    String workflowFile = cmd.getOptionValue("w");
    
    HashMap<String,String> propMap = FileUtils.loadPropertiesAsMap(propertiesFile);
    String connectorXML = FileUtils.toString(connectorFile);
    String workflowXML = FileUtils.toString(workflowFile);

    StrSubstitutor sub = new StrSubstitutor(propMap);
    connectorXML = sub.replace(connectorXML);
    workflowXML = sub.replace(workflowXML);

    ConnectorConfig connectorConfig = ConnectorConfig.fromXML(connectorXML);
    WorkflowConfig workflowConfig = WorkflowConfig.fromXML(workflowXML);

    // This is a startup script to run harry
    //AbstractConnector connector = (CSVConnector)Runtime.createAndStart("connector", "CSVConnector");
    //DocumentPipeline pipeline = (DocumentPipeline)Runtime.createAndStart("pipeline", "DocumentPipeline");

    //connector.setConfig(connectorConfig);
    //pipeline.setConfig(workflowConfig);

    //pipeline.initalize();
    //pipeline.startService();

    // attach the doc proc to the connector
    //connector.addDocumentListener(pipeline);
    //connector.setBatchSize(connectorBatchSize);

    // start crawling...
    //Thread.sleep(1000);
    //connector.startCrawling();

    // wait for crawl to stop and for the inbox to be empty.
    // TODO: this might not wait for all the stages in the doc proc to finish.
    //connector.flush();
    //pipeline.flush();

    // TODO: forcing a system.exit causes the crawl/ingestion to stop prematurely when batching is enabled.
    // Are we done?  Exit
    //Thread.sleep(100);

    //System.out.println("We have finished indexing.  Exiting now.");
    //System.exit(0);
	  
    // init the brigade config!
	  BrigadeConfig config = new BrigadeConfig();
	  config.addConnectorConfig(connectorConfig);
	  config.addWorkflowConfig(workflowConfig);
	  
		// Start up the Brigade Server
		Brigade brigadeServer = Brigade.getInstance();
		brigadeServer.setConfig(config);
		brigadeServer.start();
				
		brigadeServer.startConnector(connectorConfig.getConnectorName());
		
		// System.exit(0);
		System.out.println("Here we are...");

	}

	private void run() {
	  // TODO: remove all the jetty stuff from the build.
		// This should block for us.
		try {
			webServer.join();
		} catch (InterruptedException e) {
		  log.warn("Webserver exception : {}", e);
			running = false;
		}
	}

	private boolean startJetty(String homeDir, String port, String host) throws Exception {
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
		// when we're done , shutdown the web server
		log.info("Shutting down.");
		// TODO: remove jetty stuff from build.
		stopJetty();
		running = false;
		log.info("Shut down.");
		// We are done, exit
		System.exit(0);
	}

	private void stopJetty() {
		try {
			webServer.stop();
		} catch (Exception e) {
		  log.warn("Error stopping web server: {}", e);
		  return;
		}
		log.info("Stopped jetty...");
	}

	public Config getConfig() {
		return config;
	}

	public void setConfig(BrigadeConfig config) {
		this.config = config;
	}

	public boolean isRunning() {
		return running;
	}

	public void addConnector(ConnectorConfig connectorConfig) {
		config.addConnectorConfig(connectorConfig);
	}

	public void addWorkflow(WorkflowConfig workflowConfig) {
		config.addWorkflowConfig(workflowConfig);
	}
	
	public void startConnector(String connectorName) throws InterruptedException {
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
			log.info("Connector started.");
		} else {
			log.info("Unknown connector : " + connectorName);
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
