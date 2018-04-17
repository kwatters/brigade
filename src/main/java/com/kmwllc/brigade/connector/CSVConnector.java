package com.kmwllc.brigade.connector;

import au.com.bytecode.opencsv.CSVReader;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.utils.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Reader;

public class CSVConnector extends AbstractConnector {

  public final static Logger log = LoggerFactory.getLogger(CSVConnector.class.getCanonicalName());
  private String filename;
  private String[] columns;
  private String idField;
  private String separator = ",";
  private int numFields;
  private int idColumn = -1;
  private boolean useRowAsId = true;
  private int skipRows = 0;
  private boolean firstRowAsColumns = false;
  private int limit = -1;

  public CSVConnector() {
    super();
  }

  @Override
  public void setConfig(ConnectorConfig config) {
    workflowName = config.getProperty("workflowName");

    setDocIdPrefix(config.getStringParam("docIdPrefix", ""));
    filename = config.getProperty("filename");
    columns = config.getStringArrayParam("columns");
    idField = config.getProperty("idField");

    separator = config.getProperty("separator", separator);
    numFields = config.getIntegerParam("numFields", numFields);
    useRowAsId = config.getBoolParam("useRowAsId", useRowAsId);
    skipRows = config.getIntegerParam("skipRows", skipRows);
    firstRowAsColumns = config.getBoolParam("firstRowAsColumns", firstRowAsColumns);
  }

  public void initialize() {
    numFields = columns.length;
    for (int i = 0; i < numFields; i++) {
      if (columns[i].equals(idField)) {
        idColumn = i;
        break;
      }
    }
  }

  @Override
  public void startCrawling() throws Exception {
    // compile the map to for header to column number.
    // TODO: add a directory traversal ..

    Reader reader = null;
    try {
      reader = FileUtils.getReader(filename);
    } catch (Exception e) {
      throw new IOException(e);
    }

    CSVReader csvReader = new CSVReader(reader, separator.charAt(0));
    if (firstRowAsColumns) {
      // we should read the first row as the column header
      try {
        columns = csvReader.readNext();
      } catch (IOException e) {
    	e.printStackTrace();
        log.warn("Error in csv connector: {}", e);
      }
    }
    // pick out which column has the primary key / id field.
    initialize();

    int rowNum = 0;
    String[] nextLine;

    try {

      while ((nextLine = csvReader.readNext()) != null) {
        // TODO: replace this with connector state, and make private isRunning again.
        
        if (limit > -1 && rowNum == limit) {
          log.info("Read the limit of {} rows exiting.", limit);
          break;
        }
        if (!state.equals(ConnectorState.RUNNING)) {
          // we've been interrupted.
          log.info("Crawl interrupted, stopping crawl.");
          state = ConnectorState.INTERRUPTED;
          break;
        }
        rowNum++;
        if (nextLine.length != numFields) {
          log.warn("Warning on row {} number of columns is {} and we expected {}", nextLine.length, numFields);
          log.warn("Num Fields: {}", numFields);
        }
        if (rowNum <= skipRows) {
          continue;
        }
        String id;
        if (useRowAsId) {
          id = getDocIdPrefix() + rowNum;
        } else {
          id = getDocIdPrefix() + nextLine[idColumn];
        }
        Document docToSend = new Document(id);

        for (int i = 0; i < numFields; i++) {
          if (i >= nextLine.length) {
            log.warn("Malformed row num {} in csv file.  Missing columns. ", rowNum);
            // In the event of a malformed row, we just skip the missing columns.
            break;
          }
          String v = nextLine[i];
          if (!StringUtils.isEmpty(v)) {
            docToSend.addToField(columns[i], v);
          }
        }
        feed(docToSend);
      }
    } catch (IOException e) {
      // shouldn't see this.. but who knows.
      log.error("IO Exception during crawl. {}", e.getMessage());
    }

    log.info("Finsihed reading CSV File, flushing final data now... processed {} rows", rowNum);
    flush();
    log.info("Connector state has stopped. {} rows crawled.", rowNum);
  }

  @Override
  public void stopCrawling() {
    // no-op for this connector
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public String[] getColumns() {
    return columns;
  }

  public void setColumns(String[] columns) {
    this.columns = columns;
  }

  public String getIdField() {
    return idField;
  }

  public void setIdField(String idField) {
    this.idField = idField;
  }

  public String getSeparator() {
    return separator;
  }

  public void setSeparator(String separator) {
    this.separator = separator;
  }

  public int getNumFields() {
    return numFields;
  }

  public void setNumFields(int numFields) {
    this.numFields = numFields;
  }

  public int getIdColumn() {
    return idColumn;
  }

  public void setIdColumn(int idColumn) {
    this.idColumn = idColumn;
  }

}
