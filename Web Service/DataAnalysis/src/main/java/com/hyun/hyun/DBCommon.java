package com.hyun.hyun;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.sqlite.SQLiteConfig;

public class DBCommon<T> {
	private String dbFileName;
	private String tableName;
	private Connection connection;
	private ArrayList<T> dataList;
	private ArrayList<String> columnList;

	public DBCommon(String tableName) {
		this.tableName = tableName;
		this.columnList = new ArrayList<String>();
		this.columnList.add("average_window");
		this.columnList.add("predict_range");
		this.columnList.add("base_data");
	}
	
	public DBCommon() {
	}

	private void open() {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			SQLiteConfig config = new SQLiteConfig();
			this.connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.110.112:1521:orcl", "kopo",
					"kopo");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void close() {
		if (this.connection != null) {
			try {
				this.connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		this.connection = null;
	}

	public void selectData(T t) {
		try {
			Class<?> dataClass = t.getClass();
			Field[] dataClassFields = dataClass.getDeclaredFields();
			this.dataList = new ArrayList<T>();
			if (this.connection == null) {
				this.open();
			}
			String query = "SELECT * FROM " + this.tableName;
			PreparedStatement preparedStatement = this.connection.prepareStatement(query);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				T fieldData = (T) dataClass.getDeclaredConstructor().newInstance();
				for (Field field : dataClassFields) {
					String fieldType = field.getType().toString();
					String fieldName = field.getName();
					if (fieldType.matches("int")) {
						field.setInt(fieldData, resultSet.getInt(fieldName));
					} else {
						field.set(fieldData, resultSet.getString(fieldName));
					}
				}
				this.dataList.add(fieldData);
			}
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}

	public String selectDataTableTag(T t) {
		this.selectData(t);
		Class<?> dataClass = t.getClass();
		String returnString = "";
		for (int i = 0; i < this.dataList.size(); i++) {
			try {
				Method toTableTagStringMethod = dataClass.getDeclaredMethod("toTableTagString");
				returnString = returnString + (String) toTableTagStringMethod.invoke(this.dataList.get(i));
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return returnString;
	}

	public String selectColumnTableTag() {
		ArrayList columnList = new ArrayList<String>();
		String result = "";
		for (int i = 0; i < this.columnList.size(); i++) {
			result += "<option value = " + this.columnList.get(i) + ">" + this.columnList.get(i) + "</option>";
		}
		return result;
	}
	
	public void insertData(T t) {
		try {
			Class<?> dataClass = t.getClass();
			// Class.forName("com.politech.student.Student")

			Field[] dataClassFields = dataClass.getDeclaredFields();
			// student.getClass().getSimpleName()
			String fieldString = "";
			String valueString = "";
			for (Field field : dataClassFields) {
				if (!fieldString.isEmpty()) {
					fieldString = fieldString + ",";
				}
				String fieldType = field.getType().toString();
				String fieldName = field.getName();
				if (fieldName.matches("idx")) {
					continue;
				}
				fieldString = fieldString + fieldName;
				if (!valueString.isEmpty()) {
					valueString = valueString + ",";
				}
				if (fieldType.matches(".*String")) {
					valueString = valueString + "'" + field.get(t) + "'";
				} else {
					valueString = valueString + field.get(t);
				}
			}
			if (this.connection == null) {
				this.open();
			}
			String query = "INSERT INTO " + this.tableName + " (" + fieldString + ") VALUES (" + valueString + ")";
			Statement statement = this.connection.createStatement();
			int result = statement.executeUpdate(query);
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}
	
	public void updateData(String parameter, String value) {
		if (this.connection == null) {
			this.open();
		}
		try {
			String query = "UPDATE " + this.tableName + " SET value = " + value + " WHERE parameter = '" + parameter + "'";
			Statement statement = this.connection.createStatement();
			int result = statement.executeUpdate(query);
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}
	
	public void selectData2(T t) {
		try {
			Class<?> dataClass = t.getClass();
			Field[] dataClassFields = dataClass.getDeclaredFields();
			this.dataList = new ArrayList<T>();

			if (this.connection == null) {
				this.open();
			}
			String query = "SELECT * FROM COMMON_GROUP4" + 
					" where 1=1" + 
					" AND PRODUCTGROUP = 'PG04'" + 
					" AND YEARWEEK < 201310";
			PreparedStatement preparedStatement = this.connection.prepareStatement(query);
//			preparedStatement.setInt(1, 1);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				T fieldData = (T) dataClass.getDeclaredConstructor().newInstance();
				for (Field field : dataClassFields) {
					String fieldType = field.getType().toString();
					String fieldName = field.getName();
					if (fieldType.matches("int")) {
						field.setInt(fieldData, resultSet.getInt(fieldName));
					} else {
						field.set(fieldData, resultSet.getString(fieldName));
					}
				}
				this.dataList.add(fieldData);
			}
			System.out.println(resultSet);
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}
	
	public String selectDataTableTag2(T t) {
		this.selectData2(t);
		Class<?> dataClass = t.getClass();
		String returnString = "";
		System.out.println(this.dataList.size());
		for (int i = 0; i < this.dataList.size(); i++) {
			try {
				Method toTableTagStringMethod = dataClass.getDeclaredMethod("toTableTagString");
				returnString = returnString + (String) toTableTagStringMethod.invoke(this.dataList.get(i));
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return returnString;
	}
	
	public void selectData3(T t) {
		try {
			Class<?> dataClass = t.getClass();
			Field[] dataClassFields = dataClass.getDeclaredFields();
			this.dataList = new ArrayList<T>();

			if (this.connection == null) {
				this.open();
			}
			String query = "SELECT * FROM COMMON_GROUP4_NEW" + 
					" where 1=1" + 
					" AND PRODUCTGROUP = 'PG03'" + 
					" AND YEARWEEK > 201624";
			PreparedStatement preparedStatement = this.connection.prepareStatement(query);
//			preparedStatement.setInt(1, 1);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				T fieldData = (T) dataClass.getDeclaredConstructor().newInstance();
				for (Field field : dataClassFields) {
					String fieldType = field.getType().toString();
					String fieldName = field.getName();
					if (fieldType.matches("int")) {
						field.setInt(fieldData, resultSet.getInt(fieldName));
					} else {
						field.set(fieldData, resultSet.getString(fieldName));
					}
				}
				this.dataList.add(fieldData);
			}
			System.out.println(resultSet);
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}
	
	public String selectDataTableTag3(T t) {
		this.selectData3(t);
		Class<?> dataClass = t.getClass();
		String returnString = "";
		System.out.println(this.dataList.size());
		for (int i = 0; i < this.dataList.size(); i++) {
			try {
				Method toTableTagStringMethod = dataClass.getDeclaredMethod("toTableTagString");
				returnString = returnString + (String) toTableTagStringMethod.invoke(this.dataList.get(i));
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return returnString;
	}
	
	public LinkedHashMap<String, Double> selectData4(T t, String seg1, String seg2, String productGroup, String item) {
		LinkedHashMap<String, Double> map = new LinkedHashMap<String, Double>();
		try {
			Class<?> dataClass = t.getClass();
			Field[] dataClassFields = dataClass.getDeclaredFields();
			this.dataList = new ArrayList<T>();

			if (this.connection == null) {
				this.open();
			}
			String query = "SELECT * FROM COMMON_GROUP4_NEW" + 
					" where 1=1" + 
					" AND REGIONSEG = '" + seg1 + "'" +
					" AND REGIONSEG3 = '" + seg2 + "'" +
					" AND PRODUCTGROUP = '" + productGroup + "'" +
					" AND ITEM = '" + item + "'" + 
					"order by yearweek";
			PreparedStatement preparedStatement = this.connection.prepareStatement(query);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				T fieldData = (T) dataClass.getDeclaredConstructor().newInstance();
				for (Field field : dataClassFields) {
					String fieldType = field.getType().toString();
					String fieldName = field.getName();
					if (fieldType.matches("int")) {
						field.setInt(fieldData, resultSet.getInt(fieldName));
					} else {
						field.set(fieldData, resultSet.getString(fieldName));
					}
					String key = resultSet.getString("YEARWEEK");
					double value = Double.parseDouble(resultSet.getString("TIME_SERIES"));
					map.put(key, value);
				}
				
				this.dataList.add(fieldData);
			}
			System.out.println(resultSet);
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
		return map;
	}
	
	public String selectDataTableTag4(T t, String seg1, String seg2, String productGroup, String item) {
		this.selectData4(t, seg1, seg2, productGroup, item);
		Class<?> dataClass = t.getClass();
		String returnString = "";
		System.out.println(this.dataList.size());
		for (int i = 0; i < this.dataList.size(); i++) {
			try {
				Method toTableTagStringMethod = dataClass.getDeclaredMethod("toTableTagString");
				returnString = returnString + (String) toTableTagStringMethod.invoke(this.dataList.get(i));
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return returnString;
	}
	
	public String getQtyList(T t, String seg1, String seg2, String productGroup, String item) {
		String setQty = "";
		LinkedHashMap<String, Double> map = new LinkedHashMap<String, Double>();
		map = selectData4(t, seg1, seg2, productGroup, item);
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
