package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.slf4j.Logger;

import java.sql.*;
import java.util.List;

public abstract class AbstractJDBCStage extends AbstractStage {

  public final static Logger log = LoggerFactory.getLogger(AbstractJDBCStage.class.getCanonicalName());
  private String driver;
  private String connectionString;
  private String jdbcUser;
  private String jdbcPassword;
  protected Connection connection;
  

  public void startStage(StageConfig config) {
    // TODO Auto-generated method stub
    driver = config.getProperty("driver");
    connectionString = config.getProperty("connectionString");
    jdbcUser = config.getProperty("jdbcUser");
    jdbcPassword = config.getProperty("jdbcPassword");
    // TODO: make this more flexible / dynamic.
    createConnection();

  }

  @Override
  public abstract List<Document> processDocument(Document doc) throws Exception;
  
  private void createConnection() {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    try {
      connection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public synchronized ResultSet executeQuery(String sql) throws SQLException {
    // TODO: does this need to be synchronized?  maybe yes .. eek!
    // log.info("Run SQL {}", sql);
    ResultSet result = null;
    Statement s = connection.createStatement();
    result = s.executeQuery(sql);
    // TODO: get the auto incremented id..
    return result;
  }

  
 

}
