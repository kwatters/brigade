package com.kmwllc.brigade.connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.kmwllc.brigade.config.ConnectorConfiguration;
import com.kmwllc.brigade.document.Document;

public class DatabaseConnector extends AbstractConnector {

	private String driver;
	private String connectionString;
	private String jdbcUser;
	private String jdbcPassword;
	
	private String preSql;
	private String Sql;
	private String postSql;
	private String idField;
	
	private Connection connection = null;
	
	@Override
	public void initialize(ConnectorConfiguration config) {
		
        driver = config.getProperty("jdbcDriver");
        connectionString = config.getProperty("connectionString");
        jdbcUser = config.getProperty("jdbcUser");
        jdbcPassword = config.getProperty("jdbcPassword");

        idField = config.getProperty("idField");

        preSql = config.getProperty("preSql");
        Sql = config.getProperty("Sql");
        postSql = config.getProperty("postSql");
        
        // Create the connection
        createConnection();
        
	}

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
	public void start() throws InterruptedException {
		// Here is where we start up our connector
		setState(ConnectorState.RUNNING);
		// run the pre-sql
		runPreSql();

		Statement state = null;
		try {
			state = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//
		ResultSet rs = null;
		if (state != null) {
			try {
				rs = state.executeQuery(Sql);
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
					idColumn = i;
					break;
				}
			}
			

            try {
				while (rs.next()) {
					// Need the ID column from the RS.
					String id = rs.getString(idColumn);
					Document doc = new Document(id);
					
					// Add each column / field name to the doc
					
					for (int i = 0 ; i < columns.length; i++) {
						doc.addToField(columns[i], rs.getString(i));
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
		try {
			Statement state = connection.createStatement();
			int res = state.executeUpdate(preSql);
			state.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void runPostSql() {
		try {
			Statement state = connection.createStatement();
			int res = state.executeUpdate(postSql);
			state.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		flush();
		setState(ConnectorState.STOPPED);
	}

}
