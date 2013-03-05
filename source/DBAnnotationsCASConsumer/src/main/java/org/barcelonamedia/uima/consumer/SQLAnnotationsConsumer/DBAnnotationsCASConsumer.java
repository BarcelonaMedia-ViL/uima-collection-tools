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

package org.barcelonamedia.uima.consumer.SQLAnnotationsConsumer;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.collection.CasConsumer_ImplBase;
import org.apache.uima.examples.SourceDocumentInformation;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceProcessException;
import org.apache.uima.util.ProcessTrace;
import org.barcelonamedia.uima.consumer.SQLAnnotationsConsumer.DAO.DAOException;
import org.barcelonamedia.uima.consumer.SQLAnnotationsConsumer.DAO.DAOFactory;
import org.barcelonamedia.uima.consumer.SQLAnnotationsConsumer.DAO.FeaturesDAO;
import org.barcelonamedia.uima.consumer.SQLAnnotationsConsumer.DTO.FeaturesDTO;
import org.barcelonamedia.uima.consumer.features.FeatureInfoSet;
import org.barcelonamedia.uima.consumer.features.FeatureInfoSetManager;
import org.barcelonamedia.uima.consumer.features.FilteringFeatureInfoSet;


public class DBAnnotationsCASConsumer extends CasConsumer_ImplBase{

	/** The logger object. */
	private static final Logger logger = Logger.getLogger(DBAnnotationsCASConsumer.class.toString());


	// Suported DBMS:  -----------------------------------------

	private static final String MySQL = "MySQL";

	//----------------------------------------------------------


	/** Correponds to a parameter that specifies DBMS to be used.
	 * The value of this variable is 'DBMS' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String PARAM_DBMS = "DBMS";

	/** Correponds to a parameter that specifies the server where DBMS is being hosted.
	 * The value of this variable is 'Server' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String PARAM_SERVER = "Server";

	/** Correponds to a parameter that specifies port to be used to connect to the specified DBMS.
	 * The value of this variable is 'Port' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String PARAM_PORT = "Port";

	/** Correponds to a parameter that specifies the name of the database to be used.
	 * The value of this variable is 'Database' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String PARAM_DATABASE = "Database";

	/** Correponds to a parameter that specifies the username fof the specified database.
	 * The value of this variable is 'User' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String PARAM_USER = "User";

	/** Correponds to a parameter that specifies the password fof the specified database.
	 * The value of this variable is 'Password' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String PARAM_PASSWORD = "Password";


	/** Correponds to a parameter that specifies the database table to be used.
	 * The value of this variable is 'table' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String PARAM_TABLE = "table";

	/** Correponds to a parameter that specifies the annotation to be used as each database row entry.
	 * The value of this variable is 'seg_type' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String SEG_TYPE_NAME = "seg_type";

	/** Correponds to a parameter that specifies the features to retrieve to be registered in the database.
	 * The value of this variable is 'features' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String FEATURES = "features";

	/** Correponds to a parameter that specifies the names of the relational databases columns for the different features.
	 * (features_display_names[n] name refers to features[n] feature).
	 * The value of this variable is 'features_display_names' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String FEATURES_COLUMN_NAMES = "features_column_names";

	/** Correponds to a parameter that specifies the character to be used to concatenate different features.
	 * (features_concat_chars[n] concat character will be used for features[n] feature).
	 * The value of this variable is 'features_concat_chars' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String FEATURES_CONCAT_CHARS = "features_concat_chars";

	/** Correponds to a parameter that specifies the character to be used to replace white spaces of features value.
	 * (features_whitespace_char[n] concat character will be used for features[n] feature).
	 * The value of this variable is 'features_whitespace_char' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBAnnotationsCASConsumer/desc/DBAnnotationsCASConsumer.xml"
	 **/
	private static final String FEATURES_WHITESPACE_CHARS = "features_whitespace_char";

	/** Correponds to a parameter that specifies whether whole document URI or document name is to be used as document ID into the database.
	 * The value of this variable is 'fullURI' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String FULL_URI = "fullURI";

	/** Correponds to a parameter that specifies whether to use an existing table
	 * (if it exists, otherwise a new one is created)
	 **/
	private static final String USE_EXISTING_TABLE = "useExistingTable";

	/**
	 * Key word for specifying that white space is to be used for concatenating features or tu replace white spaces into a feature value
	 * (This parameter is used because simple white space is not allowed in an XML tag)
	 */
	private static final String WHITE_SPACE_RESERVED_KEY = "BLANK";

	/** Name corresponding to the database table column containing document id */
	private static final String DOC_ID = "doc_id";

	/** 'ficticious' feature ID for anotación coveredText */
	private static final String ANNOTATION_ID = "id";

	/** 'ficticious' feature name for anotación id */
	private static final String ANNOTATION_COVERED_TEXT = "coveredText";


	/** SEG_TYPE_NAME configuration parameter value */
	private String seg_type_name;

	/** Handler of the features to be extracted **/
	FeatureInfoSetManager featureInfoSetManager;

	/** Annotation type used for splitting **/
	private Type seg_type;

	/** Full URI flag **/
	private Boolean fullURI;

	/** Use existing table flag **/
	private Boolean useExistingTable;

	/** DAO Factory object. */
	private DAOFactory daoFactory;

	/** XMI DAO object. */
	private FeaturesDAO featuresDAO;


	public void initialize() throws ResourceInitializationException{

		System.out.println("DBAnnotationsCASConsumer: initialize()...");
		logger.info("DBAnnotationsCASConsumer: initialize()...");

		String dbms = (String) getUimaContext().getConfigParameterValue(PARAM_DBMS);
		String server = (String) getUimaContext().getConfigParameterValue(PARAM_SERVER);
		int port = (Integer) getUimaContext().getConfigParameterValue(PARAM_PORT);
		String database = (String) getUimaContext().getConfigParameterValue(PARAM_DATABASE);
		String user = (String) getUimaContext().getConfigParameterValue(PARAM_USER);
		String password = (String) getUimaContext().getConfigParameterValue(PARAM_PASSWORD);

		String table = (String) getUimaContext().getConfigParameterValue(PARAM_TABLE);

		this.seg_type_name = (String) getUimaContext().getConfigParameterValue(SEG_TYPE_NAME);

		String[] features = (String[])getUimaContext().getConfigParameterValue(FEATURES);
		String[] features_column_names = (String[])getUimaContext().getConfigParameterValue(FEATURES_COLUMN_NAMES);
		String[] features_concat_chars = (String[])getUimaContext().getConfigParameterValue(FEATURES_CONCAT_CHARS);
		String[] features_whitespaces_chars = (String[])getUimaContext().getConfigParameterValue(FEATURES_WHITESPACE_CHARS);

		this.fullURI = (Boolean) getUimaContext().getConfigParameterValue(FULL_URI);
		this.useExistingTable = (Boolean) getUimaContext().getConfigParameterValue(USE_EXISTING_TABLE);


		if((dbms == null || dbms.length() == 0) || 
				(server == null || server.length() == 0) || 
				(new Integer(port) == null) || 
				(database == null || database.length() == 0) || 
				(user == null || user.length() == 0) || 
				(password == null || password.length() == 0) || 
				(table == null || table.length() == 0) || 
				(this.seg_type_name == null || this.seg_type_name.length() == 0) || 
				(features == null || features.length == 0) ||
				(features_column_names == null || features_column_names.length == 0) ||
				(features_concat_chars != null && features_concat_chars.length == 0) ||
				(features_whitespaces_chars != null && features_whitespaces_chars.length == 0)){

			throw new ResourceInitializationException();
		}

		if(!((features.length == features_column_names.length && 
				(features_concat_chars != null && features_column_names.length == features_concat_chars.length) &&
				(features_whitespaces_chars != null && features_column_names.length == features_whitespaces_chars.length)))){

			logger.log(Level.SEVERE, "DBAnnotationsCASConsumer :: initialize() :: ERROR: array params sizes mismatched.");
			System.err.println("ERROR: DBAnnotationsCASConsumer :: initialize() :: Error: array params sizes mismatched.");

			throw new ResourceInitializationException();
		}

		logger.info("DBAnnotationsCASConsumer: initialize() - dbms: " + dbms);
		logger.info("DBAnnotationsCASConsumer: initialize() - server: " + server);
		logger.info("DBAnnotationsCASConsumer: initialize() - port: " + port);
		logger.info("DBAnnotationsCASConsumer: initialize() - database: " + database);
		logger.info("DBAnnotationsCASConsumer: initialize() - user: " + user);
		logger.info("DBAnnotationsCASConsumer: initialize() - password: " + password);

		logger.info("DBAnnotationsCASConsumer: initialize() - table: " + table);
		logger.info("DBAnnotationsCASConsumer: initialize() - seg_type_name: " + this.seg_type_name);

		logger.info("DBAnnotationsCASConsumer: initialize() - features: " + features.toString());
		logger.info("DBAnnotationsCASConsumer: initialize() - features column names: " + features_column_names.toString());

		if(features_concat_chars != null){

			logger.info("DBAnnotationsCASConsumer: initialize() - features concat chars: " + features_concat_chars.toString());

			//Updates white spaces specified with the key work WHITE_SPACE_RESERVED_KEY
			for(int i=0; i<features_concat_chars.length; i++){

				if(features_concat_chars[i].equals(WHITE_SPACE_RESERVED_KEY)){

					features_concat_chars[i] = " ";
				}
			}
		}

		if(features_whitespaces_chars != null){

			logger.info("DBAnnotationsCASConsumer: initialize() - features whitespaces chars: " + features_whitespaces_chars.toString());

			//Updates white spaces specified with the key work WHITE_SPACE_RESERVED_KEY
			for(int i=0; i<features_whitespaces_chars.length; i++){

				if(features_whitespaces_chars[i].equals(WHITE_SPACE_RESERVED_KEY)){

					features_whitespaces_chars[i] = " ";
				}
			}
		}

		logger.info("DBAnnotationsCASConsumer: initialize() - full URI as doc Id: " + this.fullURI);

		logger.info("DBAnnotationsCASConsumer: initialize() - use existing table (if available): " + this.useExistingTable);


		this.featureInfoSetManager = new FeatureInfoSetManager(features, features_column_names, features_concat_chars, features_whitespaces_chars);
		this.featureInfoSetManager.buildFeaturesInfoSet();


		if(dbms.equals(MySQL)){

			System.out.println("DBAnnotationsCASConsumer: initialize() - Using MySQL as DBMS.");

			this.daoFactory = DAOFactory.getDAOFactory(DAOFactory.MYSQL);

			Hashtable<String, String> connectionParams = new Hashtable<String, String>();
			connectionParams.put("server", server);
			connectionParams.put("port", String.valueOf(port));
			connectionParams.put("database", database);
			connectionParams.put("user", user);
			connectionParams.put("password", password);

			this.featuresDAO = this.daoFactory.getFeaturesDAO(connectionParams);

			try{

				Hashtable<String, String> tableInfo = new Hashtable<String, String>();
				tableInfo.put("table", table);

				tableInfo.put("doc_id", DOC_ID);
				tableInfo.put("features", this.featureInfoSetManager.getFeaturesDatabaseColumns());

				this.featuresDAO.setTableInfo(tableInfo);	
				this.featuresDAO.createTable(tableInfo, this.useExistingTable);
				this.featuresDAO.init();
			}
			catch(DAOException e){

				logger.log(Level.SEVERE, e.getMessage());
				System.err.println("DBAnnotationsCASConsumer: ERROR in CAS Consumer " +e.getClass() + " with message:" + e.getMessage());
				throw new ResourceInitializationException(e);
			}
		}

		logger.info("DBAnnotationsCASConsumer: initialize() - Done.");
	}

	/**
	 * Initializes the type system.
	 */
	public void typeSystemInit(TypeSystem typeSystem){

		System.out.println("DBAnnotationsCASConsumer: typeSystemInit() - Loading provided types...");

		this.seg_type = typeSystem.getType(this.seg_type_name);

		System.out.println("DBAnnotationsCASConsumer: typeSystemInit() - Types loaded.");
	}

	/**
	 * Processes the CAS which was populated by the TextAnalysisEngines. <br>
	 * In this case, the CAS is converted to XMI and written into the output file .
	 * 
	 * @param aCAS
	 *          a CAS which has been populated by the TAEs
	 * 
	 * @throws ResourceProcessException
	 *           if there is an error in processing the Resource
	 * 
	 * @see org.apache.uima.collection.base_cpm.CasObjectProcessor#processCas(org.apache.uima.cas.CAS)
	 */
	public void processCas(CAS aCAS) throws ResourceProcessException{

		String doc_id = new String(); 

		try{

			JCas jcas = aCAS.getJCas();
			FSIterator<Annotation> it = jcas.getAnnotationIndex(SourceDocumentInformation.type).iterator();

			if(it.hasNext()){

				SourceDocumentInformation sdi = (SourceDocumentInformation) it.next();

				if(this.fullURI){

					doc_id = sdi.getUri().toString();
				}
				else{

					doc_id = new File(new URL(sdi.getUri()).getPath()).getName();
				}

				if(sdi.getOffsetInSource() > 0 || !sdi.getLastSegment()){

					doc_id += ("_" + sdi.getOffsetInSource() + "_" + sdi.getDocumentSize());
				}
			}
		}
		catch(Exception e){

			logger.log(Level.SEVERE, e.getMessage());
			throw new ResourceProcessException(e);
		}


		Iterator<AnnotationFS> seg_type_iterator = aCAS.getAnnotationIndex(this.seg_type).iterator();		

		while(seg_type_iterator.hasNext()){

			Annotation seg_annotation = (Annotation)seg_type_iterator.next();

			int seg_ann_begin = seg_annotation.getBegin();
			int seg_ann_end = seg_annotation.getEnd();


			for(int i=0; i < this.featureInfoSetManager.getFeatureInfoSetList().size(); i++){

				FeatureInfoSet featureInfoSet = this.featureInfoSetManager.getFeatureInfoSetList().get(i);
				FilteringFeatureInfoSet filteringFeatureInfoSet = featureInfoSet.getFilteringFeatureInfoSet();

				//Retrieves features to be extracted and features to filter
				String feature_qualified_name = featureInfoSet.getQualifiedName();				
				String type_name = featureInfoSet.getType();

				Type annotation_type = aCAS.getTypeSystem().getType(type_name);
				Feature feature = aCAS.getTypeSystem().getFeatureByFullName(feature_qualified_name);
				Feature filtering_feature = null;				

				if(filteringFeatureInfoSet != null){

					filtering_feature = aCAS.getTypeSystem().getFeatureByFullName(filteringFeatureInfoSet.getQualifiedName());
				}


				//Process id and coveredText "particular features"
				boolean idFeature = false;
				boolean coveredTextFeature = false;

				String feature_name = featureInfoSet.getName();

				if(feature_name.equals(ANNOTATION_ID)){

					idFeature = true;
				}

				if(feature_name.equals(ANNOTATION_COVERED_TEXT)){

					coveredTextFeature = true;
				}


				//Feature extraction process...
				if(feature != null || idFeature || coveredTextFeature){

					//If annotation is not the splitting annotation...
					if(annotation_type != this.seg_type){

						Iterator<AnnotationFS> annotation_iterator = aCAS.getAnnotationIndex(annotation_type).iterator();

						while(annotation_iterator.hasNext()){

							Annotation annotation = (Annotation)annotation_iterator.next();

							//Filters for avoiding loop over annotation subclasses
							if(annotation.getType().getName().equals(annotation_type.toString())){

								int ann_begin = annotation.getBegin();
								int ann_end = annotation.getEnd();

								//If annotation overlap...
								if((ann_begin < seg_ann_end) && (ann_end > seg_ann_begin)){

									if(!idFeature && !coveredTextFeature && annotation.getFeatureValueAsString(feature) == null){

										featureInfoSet.addFeatureValues(i, "");							
									}
									else{

										boolean filterPassed = false;

										if(filtering_feature != null){

											if(filteringFeatureInfoSet.getMatcher().reset(annotation.getFeatureValueAsString(filtering_feature)).find()){

												filterPassed = true;
											}
										}

										if((filteringFeatureInfoSet!=null && filterPassed) || filteringFeatureInfoSet==null){

											if(idFeature){

												featureInfoSet.addFeatureValues(i, String.valueOf(annotation.hashCode()));
											}
											else if(coveredTextFeature){

												String coveredText = annotation.getCoveredText();

												//Replace white spaces by the feature value delimiter
												ArrayList<String> splittedCoveredTextArray = new ArrayList<String>();
												String[] splittedCoveredText = coveredText.split(" ");		
												Collections.addAll(splittedCoveredTextArray, splittedCoveredText); 

												featureInfoSet.addFeatureValues(i, splittedCoveredTextArray);
											}
											else{

												String featureValue = annotation.getFeatureValueAsString(feature);

												//Replace white spaces by the feature value delimiter
												ArrayList<String> splittedFeatureValueArray = new ArrayList<String>();
												if (featureValue != null) {
													String[] splittedFeatureValue = featureValue.split(" ");
													Collections.addAll(splittedFeatureValueArray, splittedFeatureValue); 
												} else {
													logger.severe(String.format("missing feature: %s:%s (doc: %s, <%s>)",type_name,feature_name,doc_id,seg_annotation.getCoveredText()));
												}
												featureInfoSet.addFeatureValues(i, splittedFeatureValueArray);
											}
										}
									}
								}
								else if(ann_begin > seg_ann_end){

									//When annotations can't overlap anymore
									break;
								}
							}
						}
					}
					else{

						String feature_value = new String();

						if(idFeature){

							feature_value = String.valueOf(seg_annotation.hashCode());
						}
						else if(coveredTextFeature){

							feature_value = seg_annotation.getCoveredText();
						}
						else{

							feature_value = seg_annotation.getFeatureValueAsString(feature);
						}

						//Replace white spaces by the feature value delimiter
						ArrayList<String> splittedFeatureValueArray = new ArrayList<String>();
						
						if(feature_value != null){
							
							String[] splittedFeatureValue = feature_value.split(" ");
							Collections.addAll(splittedFeatureValueArray, splittedFeatureValue); 
						} 
						else{
							logger.severe(String.format("missing feature: %s:%s (doc: %s, <%s>)",type_name,feature_name,doc_id,seg_annotation.getCoveredText()));
						}
						featureInfoSet.addFeatureValues(i, splittedFeatureValueArray);
					}
				}
				else{
					featureInfoSet.addFeatureValues(i, "");
				}
			}

			FeaturesDTO featuresdto = new FeaturesDTO(this.featureInfoSetManager.getFeaturesValues(doc_id));

			try{

				this.featuresDAO.insert(featuresdto);
			}
			catch(DAOException e){

				System.err.println("DBAnnotationsCASConsumer: ERROR in CAS Consumer " +e.getClass() + " with message:" + e.getMessage());
			}
		}
	}

	/**
	 * 
	 * 
	 */
	public void collectionProcessComplete(ProcessTrace arg0) throws ResourceProcessException, IOException{

		try{

			this.featuresDAO.closeConnection();
		}
		catch(DAOException e){

			logger.log(Level.SEVERE, e.getMessage());
			throw new ResourceProcessException(e);
		}

		System.out.println("DBAnnotationsCASConsumer: collectionProcessComplete()...");
		logger.info("DBAnnotationsCASConsumer: collectionProcessComplete() - Done.");
	}
}
