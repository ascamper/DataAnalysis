package com.hyun.hyun;

public class Parameter {
	
	public String parameter;
	public int value;
	
	public Parameter() {

	}

	public Parameter(String Parameter, int value) {
		this.parameter = Parameter;
		this.value = value;
	}

	public String toTableTagString() {
		String tagString = "";
		tagString = tagString + "<tr>";
		tagString = tagString + "<td>";
		tagString = tagString + this.parameter;
		tagString = tagString + "</td>";
		tagString = tagString + "<td>";
		tagString = tagString + this.value;
		tagString = tagString + "</td>";
		tagString = tagString + "<td>";
		tagString = tagString + "<input type = checkbox name = \"isChecked\" value = \"" + this.parameter + "\" />" ;
		tagString = tagString + "</td>";
		tagString = tagString + "</tr>";
		return tagString;
	}
	
}