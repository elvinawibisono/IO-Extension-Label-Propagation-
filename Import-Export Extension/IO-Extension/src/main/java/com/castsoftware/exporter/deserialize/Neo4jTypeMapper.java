package com.castsoftware.exporter.deserialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Neo4jTypeMapper {

	/**
	 * Zip files
	 * @param headers List of node property
	 * @param values List of values
	 * @return
	 * @throws Exception
	 */
	public static Map<String, String> zip(List<String> headers, List<String> values) throws Exception {
		if(headers.size() != values.size())
			throw new Exception("The header length and the value length don't have the same size");

		Map<String, String> zipped = new HashMap<>();
		for (int i = 0; i < headers.size(); i++) {
			zipped.put(headers.get(i), values.get(i));

		}

		return zipped;
	}


	/**
	 * Verify the map
	 * @param map Map to verify
	 * @param key Key to extract
	 * @return
	 */
	public static String verifyMap( Map<String, String> map, String key) throws Exception {
		if(!map.containsKey(key)) throw new Exception(String.format("The map parameter does not contain key : '%s'.", key));
		return map.get(key);

		
	}

	/**
	 * Verify the map
	 * @param map Map to verify
	 * @param key Key to extract
	 * @return
	 */
	public static Object getNeo4jType( Map<String, String> map, String key) {
		String o = map.get(key);
		return Neo4jTypeMapper.getNeo4jType(o);

	}


	/**
	 * Test with regex the string and deserialize to the correct type
	 * @param o String to process
	 * @return The object with the correct type to insert
	 */
	public static Object getNeo4jType(String o) {
		if(o == null) return null;
		if(o.equals("null")) return null;

		// RemoveCSV Double quotes
		o = o.replaceFirst("(^\\\"+)|(\\\"+$)", "");

		Pattern pattern;
		Matcher matcher;

		// Long
		final String longRegex = "^\\d+$";
		pattern = Pattern.compile(longRegex, Pattern.MULTILINE);
		matcher = pattern.matcher(o);
		if(matcher.find()) return Long.parseLong(o);

		// boolean
		final String boolRegex = "^(TRUE|FALSE)$";
		pattern = Pattern.compile(boolRegex, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(o);
		if(matcher.find()) return Boolean.parseBoolean(o);

		// byte
		final String byteRegex = "^(0|1)$";
		pattern = Pattern.compile(byteRegex, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(o);
		if(matcher.find()) return Byte.parseByte(o);


		// float
		final String regexFloat = "^\\d+\\.\\d+$";
		pattern = Pattern.compile(regexFloat, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(o);
		if(matcher.find()) return Float.parseFloat(o);


		// List of primitive
		final String regexList = "\\[(?:[^\\]].)+]";
		pattern = Pattern.compile(regexList, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(o);
		if(matcher.find()) {
			// Break list
			String sub = o.substring(0, o.length() - 1); // Remove square brace
			String[] items = sub.split(","); // Split the list

			List<Object> objectList = new ArrayList<>();
			for(String s: items) objectList.add(getNeo4jType(s)); // Insert

			return objectList;
		};

		// String , trim and remove \"
		o = o.trim();
		return o;
	}

	/**
	 * Convert the object to a string list
	 * @param obj Object to convert
	 * @return
	 */
	public static List<String> getAsStringList(Object obj) throws Exception {
		if(obj == null) throw new NullPointerException("'obj' cannot be null");
		String o = obj.toString();

		// List of primitive
		final String regexList = "\\[(([^,\\]]+,?)+)\\]"; // Verify if the form is ["...."]
		Pattern pattern = Pattern.compile(regexList, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(o);
		if(matcher.find()) {
			// Break list
			String sub = o.substring(1, o.length() - 1); // Remove square brace
			String[] items = sub.split(","); // Split the list

			List<String> objectList = new ArrayList<>();
			for(String s: items) {
				s = s.replaceFirst("^\"", "");
				s = s.replaceFirst("\"$", "");
				s = s.trim();
				objectList.add(s);
			} // Insert

			return objectList;
		} else {
			throw new Exception(String.format("The object is not a list. Object : %s", obj.toString()));
		}
	}

}
