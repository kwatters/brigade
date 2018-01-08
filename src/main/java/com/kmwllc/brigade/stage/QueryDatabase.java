package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class QueryDatabase extends AbstractStage {

  public final static Logger log = LoggerFactory.getLogger(QueryDatabase.class.getCanonicalName());
  private String driver;
  private String connectionString;
  private String jdbcUser;
  private String jdbcPassword;
  private String sql = null;
  private String keyField;
  private Map<String,String> fieldNameMap;
  protected Connection connection = null;
  
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

  @Override
  public void startStage(StageConfig config) {
    driver = config.getProperty("driver");
    connectionString = config.getProperty("connectionString");
    jdbcUser = config.getProperty("jdbcUser");
    jdbcPassword = config.getProperty("jdbcPassword");
    keyField = config.getProperty("keyField");
    sql = config.getProperty("sql");
    fieldNameMap = config.getMapParam("fieldNameMap");
    createConnection();
  }

  protected ResultSet executeQuery(String sql) throws SQLException {
    ResultSet result = null;
    Statement s = connection.createStatement();
    result = s.executeQuery(sql);
    // TODO: get the auto incremented id..
    return result;
  }

  @Override
  public List<Document> processDocument(Document doc) throws Exception {
    if (doc.hasField(keyField)) {
      for (Object o : doc.getField(keyField)) {
        String v = o.toString();
        if (StringUtils.isEmpty(v)) {
          continue;
        }
        Statement s = connection.createStatement();
        String filledSql = fillSqlTemplate(v, sql);
        // log.info("Running SQL: {}", filledSql);
        ResultSet rs = s.executeQuery(filledSql);
        // now we need to iterate the results
        while (rs.next()) {
          // Need the ID column from the RS.
          for (String columnName : fieldNameMap.keySet()) {
            Object value = rs.getObject(columnName);
            doc.addToField(fieldNameMap.get(columnName), value);
          }
        }
        rs.close();
        s.close();
      }
    }
    return null;
  }

  private String fillSqlTemplate(String value, String tmplate) {
    // TODO: we might want to be able to quote the data.. this might 
    // go away when we use prepared statements here.
    boolean addQuotes = true;
    if (addQuotes) {
      value = "\"" + value + "\"";
    }
    // TODO: derive this from the current document!
    HashMap<String,String> propMap = new HashMap<String,String>();
    propMap.put("key", value);
    StrSubstitutor sub = new StrSubstitutor(propMap);
    return sub.replace(sql);
  }

  @Override
  public void stopStage() {
    // TODO Auto-generated method stub

  }

  @Override
  public void flush() {
    // TODO Auto-generated method stub

  }

}
