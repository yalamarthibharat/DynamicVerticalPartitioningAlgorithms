package com.bharat.propertyhelper;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Uses typesafe config to retrieve values from application.conf file.
 * Created by Bharat Yalamarthi on 6/17/2015.
 */
public class PropertyHelper {

	/*
		Uses typesafe to get the config
	 */
	private static final Config CONFIG = ConfigFactory.load();

	/**
	 * Returns the value of that property if exists; otherwise returns null
	 * @param propertyKey
	 * @return
	 */
	public static String getValueFromConfig(String propertyKey) {
		return CONFIG.getString(propertyKey);
	}

}
