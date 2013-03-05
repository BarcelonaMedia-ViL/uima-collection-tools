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


public class FeatureInfoSetManager{


	/**
	 * Delimiter for features definition for specifying which feature value 
	 * will be used for filtering the feature to be extracted.
	 */
	private static final String FILTERING_FEATURE_DELIMITER = "#";

	/**
	 * Delimiter used for the different values of a given feature.
	 */
	private static final String DEFAULT_FEATURES_CONCAT_CHAR = " ";
	
	/**
	 * Delimiter used to replace white spaces into the value of a feature.
	 */
	private static final String DEFAULT_FEATURES_WHITESPACE_CHAR = "_";
	
	/**
	 * Delimiter used in the feature qualified name (Type:feature) of the features to be extracted.
	 */
	private static final String TYPESYSTEM_FEATURES_DELIMITER = ":";
	
	
	private ArrayList<FeatureInfoSet> featureInfoSetList = new ArrayList<FeatureInfoSet>();
	
	/**
	 * FEATURES configuration parameter value
	 */
	private String[] features;
	
	/**
	 * FEATURES_DISPLAY_NAMES configuration parameter value
	 */
	private String[] featuresColumnNames;
	
	/**
	 * FEATURES_CONCAT_CHARS configuration parameter value
	 */
	private String[] featuresConcatChars;

	/**
	 * FEATURES_WHITESPACE_CHARS configuration parameter value
	 */
	private String[] featuresWhitespacesChars;

	
	public FeatureInfoSetManager() {

	}

	public FeatureInfoSetManager(String[] features, String[] features_column_names,
									String[] features_concat_chars, String[] features_whitespaces_chars){

		this.features = features;
		this.featuresColumnNames = features_column_names;
		this.featuresConcatChars = features_concat_chars;
		this.featuresWhitespacesChars = features_whitespaces_chars;
	}

	public ArrayList<FeatureInfoSet> getFeatureInfoSetList(){
		return featureInfoSetList;
	}

	public void setFeatures(String[] features){
		this.features = features;
	}

	public void setFeaturesColumnNames(String[] features_column_names){
		this.featuresColumnNames = features_column_names;
	}

	public void setFeaturesConcatChars(String[] features_concat_chars){
		this.featuresConcatChars = features_concat_chars;
	}

	public void setFeaturesWhitespacesChars(String[] features_whitespaces_chars){
		this.featuresWhitespacesChars = features_whitespaces_chars;
	}
	
	public void buildFeaturesInfoSet(){

		
		for(int index=0; index<this.features.length; index++){			
			
			FeatureInfoSet featureInfoSet = new FeatureInfoSet();			
			featureInfoSet.setDatabaseColumnName(this.featuresColumnNames[index]);
			
			//Retrieves feature to be extracted and filtering feature
			int filtering_feature_index = this.features[index].indexOf(FILTERING_FEATURE_DELIMITER);
			
			if(filtering_feature_index > 0){
				
				String feature_qualified_name = this.features[index].substring(0, filtering_feature_index);
				featureInfoSet.setQualifiedName(feature_qualified_name);
				
				String[] ts_f = feature_qualified_name.split(TYPESYSTEM_FEATURES_DELIMITER);				
				String type_name = ts_f[0];
				String feature_name = ts_f[1];	
				
				featureInfoSet.setType(type_name);
				featureInfoSet.setName(feature_name);

				
				String filtering_feature_value = this.features[index].substring((filtering_feature_index + 1), this.features[index].length());
				
				String filtering_fname = filtering_feature_value.substring(0, filtering_feature_value.indexOf("="));
				
				String filtering_fvalue = filtering_feature_value.substring(filtering_feature_value.indexOf("=") + 1, filtering_feature_value.length());
				

				FilteringFeatureInfoSet filteringFeatureInfoSet = new FilteringFeatureInfoSet();
				
				filteringFeatureInfoSet.setQualifiedName(type_name + TYPESYSTEM_FEATURES_DELIMITER + filtering_fname);
				filteringFeatureInfoSet.setRegex(filtering_fvalue);
				
				featureInfoSet.setFilteringFeatureInfoSet(filteringFeatureInfoSet);
			}
			else{
				
				String feature_qualified_name = this.features[index];

				featureInfoSet.setQualifiedName(feature_qualified_name);
				
				String[] ts_f = feature_qualified_name.split(TYPESYSTEM_FEATURES_DELIMITER);				
				String type_name = ts_f[0];
				String feature_name = ts_f[1];	
				
				featureInfoSet.setType(type_name);
				featureInfoSet.setName(feature_name);
			}
			
			if(this.featuresConcatChars != null){
				
				featureInfoSet.setConcatChar(this.featuresConcatChars[index]);
			}
			else{
				
				featureInfoSet.setConcatChar(DEFAULT_FEATURES_CONCAT_CHAR);
			}
			
			if(this.featuresWhitespacesChars != null){
				
				featureInfoSet.setWhitespaceFillingChar(this.featuresWhitespacesChars[index]);
			}
			else{
				
				featureInfoSet.setWhitespaceFillingChar(DEFAULT_FEATURES_WHITESPACE_CHAR);
			}
			
			this.featureInfoSetList.add(featureInfoSet);
		}
	}
	
	public String getFeaturesDatabaseColumns(){
		
		ArrayList<String> databaseColumnNames = new ArrayList<String>();
		
		for(FeatureInfoSet featureInfoSet : this.featureInfoSetList){
			
			if(!databaseColumnNames.contains(featureInfoSet.getDatabaseColumnName())){
			
				databaseColumnNames.add(featureInfoSet.getDatabaseColumnName());
			}
		}
		
		return CollectionUtils.join(databaseColumnNames, ",");
	}
	
	private int getNumberOfDatabaseColumns(){
		
		ArrayList<String> databaseColumnNames = new ArrayList<String>();
		
		int numberOfColumns = 0;
		
		for(FeatureInfoSet featureInfoSet : this.featureInfoSetList){
			
			if(!databaseColumnNames.contains(featureInfoSet.getDatabaseColumnName())){
			
				numberOfColumns += 1;
				databaseColumnNames.add(featureInfoSet.getDatabaseColumnName());
			}
		}
		
		return numberOfColumns;
	}
	
	public String[] getFeaturesValues(String doc_id){
		
		String[] featureValues = new String[this.getNumberOfDatabaseColumns() + 1];
		featureValues[0] = doc_id;
		
		int index = 1;
		
		ArrayList<String> processedDatabaseColumnNames = new ArrayList<String>();
		
		for(FeatureInfoSet featureInfoSet : this.featureInfoSetList){
			
			if(!processedDatabaseColumnNames.contains(featureInfoSet.getDatabaseColumnName())){
				
				featureValues[index] = featureInfoSet.getValues();
			
				processedDatabaseColumnNames.add(featureInfoSet.getDatabaseColumnName());
				index++;
			}
			else{				

				//Stores in the same column index of the database table
				int i = processedDatabaseColumnNames.indexOf(featureInfoSet.getDatabaseColumnName());
				
				//Normalizes index, as "doc_id" is in column 0 
				i+= 1;
				
				featureValues[i] = featureValues[i] + DEFAULT_FEATURES_CONCAT_CHAR + featureInfoSet.getValues();
			}
			
			featureInfoSet.resetValues();
		}
		
		return featureValues;
	}
}
