package com.kmwllc.brigade.connector;

import com.google.common.base.Strings;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;

import java.sql.*;


public class DatabaseConnector extends AbstractConnector {

    private static final long serialVersionUID = 1L;
    private String driver;
    private String connectionString;
    private String jdbcUser;
    private String jdbcPassword;
    private String preSql;
    private String sql;
    private String postSql;
    private String idField;
    private Connection connection = null;

    @Override
    public void setConfig(ConnectorConfig config) {
        workflowName = config.getProperty("workflowName");
        driver = config.getStringParam("driver");
        connectionString = config.getStringParam("connectionString");
        jdbcUser = config.getStringParam("jdbcUser");
        jdbcPassword = config.getStringParam("jdbcPassword");
        preSql = config.getStringParam("preSql");
        postSql = config.getStringParam("postSql");
        sql = config.getStringParam("sql");
        idField = config.getStringParam("idField");
    }

    @Override
    public void initialize() {

    }

    // @Override
    // public void initialize(ConnectorConfiguration config) {
    // driver = config.getProperty("jdbcDriver");
    // connectionString = config.getProperty("connectionString");
    // jdbcUser = config.getProperty("jdbcUser");
    // jdbcPassword = config.getProperty("jdbcPassword");
    // idField = config.getProperty("idField");
    // preSql = config.getProperty("preSql");
    // sql = config.getProperty("sql");
    // postSql = config.getProperty("postSql");
    // // Create the connection
    // createConnection();
    // }

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
    public void startCrawling() throws Exception {
        // Here is where we start up our connector
        setState(ConnectorState.RUNNING);

        // connect to the database.
        createConnection();

        // run the pre-sql
        runPreSql();

        Statement state = null;
        try {
            state = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        //
        ResultSet rs = null;
        if (state != null) {
            try {
                rs = state.executeQuery(sql);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        if (rs != null) {
            String[] columns = null;
            try {
                columns = getColumnNames(rs);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            int idColumn = -1;
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equalsIgnoreCase(idField)) {
                    idColumn = i + 1;
                    break;
                }
            }

            try {
                while (rs.next()) {
                    // Need the ID column from the RS.
                    String id = rs.getString(idColumn);
                    Document doc = new Document(id);
                    // Add each column / field name to the doc
                    for (int i = 0; i < columns.length; i++) {
                        doc.addToField(columns[i].toLowerCase(), rs.getString(i + 1));
                    }
                    // Process this row!
                    feed(doc);
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        // the post sql.
        runPostSql();

        flush();
        setState(ConnectorState.STOPPED);
    }

    /**
     * Return an array of column names.
     */
    private String[] getColumnNames(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        String[] names = new String[meta.getColumnCount()];
        for (int i = 0; i < names.length; i++) {
            names[i] = meta.getColumnName(i + 1);
        }
        return names;
    }

    private void runPreSql() {
        if (!Strings.isNullOrEmpty(preSql)) {
            try {
                Statement state = connection.createStatement();
                state.executeUpdate(preSql);
                state.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private void runPostSql() {
        if (!Strings.isNullOrEmpty(postSql)) {
            try {
                Statement state = connection.createStatement();
                state.executeUpdate(postSql);
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
