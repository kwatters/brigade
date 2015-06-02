package com.kmwllc.brigade.connector;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import au.com.bytecode.opencsv.CSVReader;

import com.kmwllc.brigade.config.ConnectorConfiguration;
import com.kmwllc.brigade.document.Document;

public class CSVConnector extends AbstractConnector {

	private String filename;
	private String[] columns;
	private String idField;
	private String idPrefix; 
	private String separator;
	private int numFields;
	private int idColumn = -1;
	
	@Override
	public void initialize(ConnectorConfiguration config) {
		// TODO Auto-generated method stub
		filename = config.getProperty("filename", "data/myfile.csv");	
		columns = config.getProperty("columnnames", "id,column1,column2").split(",");
		idField = config.getProperty("idcolumn", "id");
		idPrefix = config.getProperty("idprefix", "doc_");
		separator = config.getProperty("separator", ",");
		if (separator.length() > 1) {
			// This is an error condition we can only have a character as a separator.
			
		}
		
		numFields = columns.length;
		for (int i = 0 ; i < numFields ; i ++) {
			if (columns[i].equals(idField)) {
				idColumn = i;
				break;
			}
		}
	}

	@Override
	public void start() throws InterruptedException {
		// TODO: add a directory traversal ..
		// log.info("Starting CSV Connector");
		File fileToCrawl = new File(filename);
		if (!fileToCrawl.exists()) {
			// error. file not found.
			System.out.println("File not found..." + filename);
			return;
		}
		
		FileReader reader = null;
		try {
			reader = new FileReader(fileToCrawl);
		} catch (FileNotFoundException e) {
			// This should not happen
			e.printStackTrace();
		}
		CSVReader csvReader = new CSVReader(reader, separator.charAt(0));
		
		String[] nextLine;
		
		try {
			
			while ((nextLine = csvReader.readNext()) != null) {
			    String id = idPrefix + nextLine[idColumn];
			    Document docToSend = new Document(id);
			    for (int i = 0; i < numFields ; i++) {
			    	docToSend.addToField(columns[i], nextLine[i]);
			    	i++;
			    }
			    feed(docToSend);			    
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// shouldn't see this.. but who knows.
			e.printStackTrace();
		}
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub

	}

}

