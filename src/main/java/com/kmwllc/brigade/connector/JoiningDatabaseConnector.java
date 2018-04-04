package com.kmwllc.brigade.connector;

import com.google.common.base.Strings;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.slf4j.Logger;

import java.sql.*;


public class JoiningDatabaseConnector extends AbstractConnector {

  public final static Logger log = LoggerFactory.getLogger(JoiningDatabaseConnector.class.getCanonicalName());
  private static final long serialVersionUID = 1L;
  private String driver;
  private String connectionString;
  private String jdbcUser;
  private String jdbcPassword;
  private String preSql;
  private String sql;
  private String postSql;
  private String idField;
  private String joinSql;
  private String joinField;
  private Connection connection = null;

  @Override
  public void setConfig(ConnectorConfig config) {
    // TODO: need a way to make sure the base class gets it's chance to get the config.
    workflowName = config.getProperty("workflowName");
    driver = config.getStringParam("driver");
    connectionString = config.getStringParam("connectionString");
    jdbcUser = config.getStringParam("jdbcUser");
    jdbcPassword = config.getStringParam("jdbcPassword");
    preSql = config.getStringParam("preSql");
    postSql = config.getStringParam("postSql");
    sql = config.getStringParam("sql");
    idField = config.getStringParam("idField");
    joinSql = config.getStringParam("joinSql");
    joinField = config.getStringParam("joinField");
    // TODO: move to base class functionality
    docIdPrefix = config.getStringParam("docIdPrefix");
  }

  @Override
  public void initialize() {

  }

  private void createConnection() throws ClassNotFoundException, SQLException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      // TODO better error handling/logging
      e.printStackTrace();
      setState(ConnectorState.ERROR);
      throw(e);
    }
    try {
      connection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
    } catch (SQLException e) {
      // TODO better logging/error handling.
      e.printStackTrace();
      setState(ConnectorState.ERROR);
      throw(e);
    }
  }

  @Override
  public void startCrawling() throws Exception {
    // connect to the database.
    createConnection();
    // run the pre-sql (if specified)
    runSql(preSql);

    ResultSet rs = null;
    try {
      log.info("Running primary sql");
      Statement state = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      rs = state.executeQuery(sql);
    } catch (SQLException e) {
      e.printStackTrace();
      setState(ConnectorState.ERROR);
      throw(e);
    }

    // prepare the other sql query
    ResultSet rs2 = null;
    try {
      // TODO: can we do this with forward only ?
      log.info("Running other sql");
      Statement state2 = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      rs2 = state2.executeQuery(joinSql);
    } catch (SQLException e) {
      e.printStackTrace();
      setState(ConnectorState.ERROR);
      throw(e);
    }

    log.info("Describing result sets...");

    String[] columns = null;
    String[] columns2 = null;
    try {
      // get column names for sql 
      columns = getColumnNames(rs);
      // get the column name map for the second result set.
      columns2 = getColumnNames(rs2);
    } catch (SQLException e) {
      e.printStackTrace();
      setState(ConnectorState.ERROR);
      return;
    }

    int idColumn = -1;
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].equalsIgnoreCase(idField)) {
        idColumn = i + 1;
        break;
      }
    }

    log.info("Processing rows...");
    try {
      while (rs.next()) {
        // Need the ID column from the RS.
        String id = rs.getString(idColumn);
        Document doc = new Document(id);
        // Add each column / field name to the doc
        for (int i = 0; i < columns.length; i++) {
          doc.addToField(columns[i].toLowerCase(), rs.getString(i + 1));
        }
        // this is the primary key that the result set is ordered by.
        Integer joinId = rs.getInt(joinField);
        int childId = -1;
        while (rs2.next()) {
          // TODO: support non INT primary key
          Integer otherJoinId = rs2.getInt(joinField);

          if (otherJoinId < joinId) {
            // advance until we get to the id on the right side that we want.
            continue;
          }
          if (otherJoinId > joinId) {
            // we've gone too far.. lets back up and break out , move forward the primary result set.
            rs2.previous();
            break;
          }
          childId++;
          Document child = new Document(Integer.toString(childId));
          for (String c : columns2) {
            String fieldName = c.trim().toLowerCase();
            String fieldValue = rs2.getString(c);
            child.addToField(fieldName, fieldValue);
          }
          // add the accumulated child doc.
          doc.addChildDocument(child);
        }
        // TODO: can we remove this?
        if (!rs2.isLast()) {
          rs2.previous();
        }
        // feed the accumulated document.
        feed(doc);
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // the post sql.
    runSql(postSql);
    flush();
    connection.close();
    setState(ConnectorState.STOPPED);
  }

  /**
   * Return an array of column names.
   */
  private String[] getColumnNames(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    String[] names = new String[meta.getColumnCount()];
    for (int i = 0; i < names.length; i++) {
      names[i] = meta.getColumnLabel(i + 1).toLowerCase();
      log.info("column {} ", names[i]);
    }
    return names;
  }

  private void runSql(String sql) {
    if (!Strings.isNullOrEmpty(sql)) {
      try {
        Statement state = connection.createStatement();
        state.executeUpdate(sql);
        state.close();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  @Override
  public void stopCrawling() {
    // TODO Auto-generated method stub
    setState(ConnectorState.STOPPED);
  }

  public String getDriver() {
    return driver;
  }

  public void setDriver(String driver) {
    this.driver = driver;
  }

  public String getConnectionString() {
    return connectionString;
  }

  public void setConnectionString(String connectionString) {
    this.connectionString = connectionString;
  }

  public String getJdbcUser() {
    return jdbcUser;
  }

  public void setJdbcUser(String jdbcUser) {
    this.jdbcUser = jdbcUser;
  }

  public String getJdbcPassword() {
    return jdbcPassword;
  }

  public void setJdbcPassword(String jdbcPassword) {
    this.jdbcPassword = jdbcPassword;
  }

  public String getPreSql() {
    return preSql;
  }

  public void setPreSql(String preSql) {
    this.preSql = preSql;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getPostSql() {
    return postSql;
  }

  public void setPostSql(String postSql) {
    this.postSql = postSql;
  }

  public String getIdField() {
    return idField;
  }

  public void setIdField(String idField) {
    this.idField = idField;
  }

}
