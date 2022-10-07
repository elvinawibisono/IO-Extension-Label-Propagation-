package com.castsoftware.exporter.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;


public class Formatter {

	/**
	 * Convert Object to string
	 * @param obj Object to convert
	 * @return
	 */
	public static String toString(Object obj) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			return "";
		}
	}

	/**
	 * Convert a list of object to a list of strng
	 * @param objects Object List
	 * @return
	 */
	public static String[] toString(List<Object> objects) {
		List<String> arr = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();

		for(Object o : objects) {
			try {
			arr.add(mapper.writeValueAsString(o));
			} catch (JsonProcessingException e) {
				arr.add(null);
			}
		}

		return arr.toArray(new String[0]);
	}
}
