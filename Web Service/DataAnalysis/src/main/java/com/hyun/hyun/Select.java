package com.hyun.hyun;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.TreeSet;

import org.sqlite.SQLiteConfig;

public class Select {

	public Set<String> itemList() {
		Set<String> set = new TreeSet<String>();
		String table = "common_group4_new";
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			SQLiteConfig config = new SQLiteConfig();
			Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.110.112:1521:orcl", "kopo",
					"kopo");
			String query = "select ITEM from " + table;
			PreparedStatement preparedStatement = connection.prepareStatement(query);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				String data = resultSet.getString("ITEM");
				set.add(data);
			}
			preparedStatement.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return set;
	}

	public String setItemList() {
		String setItem = "";
		Set<String> set = new TreeSet<String>();
		set = this.itemList();
		boolean first = true;
		for (String item : set) {
			if (first) {
				setItem = setItem + "<option value=" + item + " selected>" + item + "</option>";
				first = false;
			} else {
				setItem = setItem + "<option value=" + item + ">" + item + "</option>";
			}
		}
		return setItem;
	}

	public LinkedHashMap<String, Double> qty() {
		LinkedHashMap<String, Double> map = new LinkedHashMap<String, Double>();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			SQLiteConfig config = new SQLiteConfig();
			Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.110.112:1521:orcl", "kopo",
					"kopo");
			String query = "select YEARWEEK, TIME_SERIES from common_group4_new where REGIONSEG3 = 'SITEID0004' and ITEM = 'ITEM0115' order by YEARWEEK";
			System.out.println(query);
			PreparedStatement preparedStatement = connection.prepareStatement(query);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				String key = resultSet.getString("YEARWEEK");
				double value = Double.parseDouble(resultSet.getString("TIME_SERIES"));
				map.put(key, value);
			}
			preparedStatement.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}

	public String getQtyList() {
		String setQty = "";
		LinkedHashMap<String, Double> map = new LinkedHashMap<String, Double>();
		map = qty();
		for (String key : map.keySet()) {
			if (setQty != "") {
				setQty += ",";
			}
			setQty = setQty + "['" + key + "', " + map.get(key) + "]";
		}
		System.out.println(setQty);
		return setQty;
	}
}
