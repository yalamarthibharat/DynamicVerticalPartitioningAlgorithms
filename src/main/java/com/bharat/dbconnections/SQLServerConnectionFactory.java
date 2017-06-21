package com.bharat.dbconnections;

import com.bharat.propertyhelper.KeyConstants;
import com.bharat.propertyhelper.PropertyHelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Manages connections to SQLServer
 */
public class SQLServerConnectionFactory {
	/*
		MSSQLSERVER jdbc url
	 */
	public static final String SERVER_URL = PropertyHelper.getValueFromConfig(KeyConstants.CONNECTION_MSSQL_JDBC_KEY);

	private static Connection sqlConnection = null;

	/*
		To overrride the default public constructor
	 */
	private SQLServerConnectionFactory() {
	}

	/**
	 * Return an existing connection / creates one if required.
	 *
	 * @return
	 */
	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		if (sqlConnection == null) {
			try {
				//Need to load the class to register with DriverManager.
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				sqlConnection = DriverManager.getConnection(SERVER_URL);
			} catch (ClassNotFoundException e) {
				System.err.println("Make sure you have the required MSSQL JDBC driver on classpath");
				throw e;
			} catch (SQLException e) {
				System.err.println("Check the JDBC URL: " + SERVER_URL);
				throw e;
			}
		}
		return sqlConnection;
	}
}
