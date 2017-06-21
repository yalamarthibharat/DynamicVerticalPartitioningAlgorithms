package com.bharat.propertyhelper;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Bharat Yalamarthi on 6/17/2017.
 */
public class PropertyHelperTest {

	@Test
	public void getValueFromConfig() throws Exception {
		assertTrue(PropertyHelper.getValueFromConfig("connection.mssql.jdbc").equals("jdbc:sqlserver://localhost;databaseName=autostore;integratedSecurity=true;"));
	}

}