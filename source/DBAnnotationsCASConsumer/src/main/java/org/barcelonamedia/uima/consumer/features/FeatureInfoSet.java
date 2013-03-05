/*
 * Copyright 2012 Fundació Barcelona Media
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *      http://www.apache.org/licenses/LICENSE-2.0

 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.barcelonamedia.uima.consumer.features;

import java.util.ArrayList;

import org.barcelonamedia.uima.consumer.utils.CollectionUtils;


public class FeatureInfoSet{

	/**
	 * UIMA Feature qualified name.
	 */
	private String qualifiedName;
	
	/**
	 * UIMA annotation type name this Feature belongs to.
	 */
	private String type;
	
	/**
	 * UIMA Feature name.
	 */
	private String name;	

	/**
	 * Collected Feature's values.
	 */
	ArrayList<String> values;
	
	/**
	 * String used for concatenating Feature's values.
	 */
	private String concatChar;
	
	/**
	 * String used for replacing Feature's values white spaces.
	 */
	private String whitespaceFillingChar;
	
	/**
	 * Name of the database table column which will store Feature's values.
	 */
	private String databaseColumnName;
	
	/**
	 * Filtering feature info set object.
	 */
	private FilteringFeatureInfoSet filteringFeatureInfoSet;

	
	public FeatureInfoSet(){
		
		this.values = new ArrayList<String>();
		
		this.filteringFeatureInfoSet = null;
	}
	
	public FeatureInfoSet(String qualifiedName, String type, String name, String concatChar, String whitespaceFillingChar, String databaseColumnName){
		
		this.qualifiedName = qualifiedName;
		this.type = type;
		this.name = name;
		this.concatChar = concatChar;
		this.whitespaceFillingChar = whitespaceFillingChar;
		this.databaseColumnName = databaseColumnName;
		
		this.filteringFeatureInfoSet = null;
	}


	public String getQualifiedName() {
		return qualifiedName;
	}

	public void setQualifiedName(String qualifiedName) {
		this.qualifiedName = qualifiedName;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValues() {
		return CollectionUtils.join(this.values, this.concatChar);
	}
	
	public void resetValues() {
		this.values = new ArrayList<String>();
	}

	public void setValues(ArrayList<String> values) {
		this.values = values;
	}

	public String getConcatChar() {
		return concatChar;
	}

	public void setConcatChar(String concatChar) {
		this.concatChar = concatChar;
	}

	public String getWhitespaceFillingChar() {
		return whitespaceFillingChar;
	}

	public void setWhitespaceFillingChar(String whitespaceFillingChar) {
		this.whitespaceFillingChar = whitespaceFillingChar;
	}

	public String getDatabaseColumnName() {
		return databaseColumnName;
	}

	public void setDatabaseColumnName(String databaseColumnName) {
		this.databaseColumnName = databaseColumnName;
	}

	public FilteringFeatureInfoSet getFilteringFeatureInfoSet() {
		return filteringFeatureInfoSet;
	}

	public void setFilteringFeatureInfoSet(
			FilteringFeatureInfoSet filteringFeatureInfoSet) {
		this.filteringFeatureInfoSet = filteringFeatureInfoSet;
	}
	
	public void addFeatureValues(int featureIndex, String featureValue){
		
		this.values.add(featureValue);
	}
	
	public void addFeatureValues(int featureIndex, ArrayList<String> featureValues){
		
		this.values.add(CollectionUtils.join(featureValues, this.whitespaceFillingChar));
	}
}
