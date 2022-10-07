package com.castsoftware.exporter.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExporterUtils {

	/**
	 * Export a map to  a string with format key: value ... ,
	 * @param properties List of properties to export
	 * @return String with key: value
	 */
	public static String stringMapToString(Map<String, Object> properties, String delimiter) {
		return properties.entrySet()
				.stream()
				.map(x -> String.format("%s: %s", x.getKey(), x.getValue()))
				.collect(Collectors.joining(delimiter)); // Join the list
	}

}
