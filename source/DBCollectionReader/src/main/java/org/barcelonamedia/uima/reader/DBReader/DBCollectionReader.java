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

package org.barcelonamedia.uima.reader.DBReader;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Hashtable;
import java.util.logging.Logger;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.examples.SourceDocumentInformation;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import org.barcelonamedia.uima.reader.DBReader.DAO.DAOException;
import org.barcelonamedia.uima.reader.DBReader.DAO.DAOFactory;
import org.barcelonamedia.uima.reader.DBReader.DAO.DocumentDAO;


public class DBCollectionReader extends CollectionReader_ImplBase{
	
	/** The logger object. */
	private static final Logger logger = Logger.getLogger(DBCollectionReader.class.toString());
	
	// Suported DBMS:  -----------------------------------------
	
	private static final String MySQL = "MySQL";
	
	//----------------------------------------------------------
	
	
	/** Correponds to a parameter that specifies DBMS to be used.
	 * The value of this variable is 'DBMS' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBCollectionReader/desc/DBCollectionReader"
	 **/
	private static final String PARAM_DBMS = "DBMS";
	
	/** Correponds to a parameter that specifies the server where DBMS is being hosted.
	 * The value of this variable is 'Server' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBCollectionReader/desc/DBCollectionReader"
	 **/
	private static final String PARAM_SERVER = "Server";
	
	/** Correponds to a parameter that specifies port to be used to connect to the specified DBMS.
	 * The value of this variable is 'Port' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBCollectionReader/desc/DBCollectionReader"
	 **/
	private static final String PARAM_PORT = "Port";
	
	/** Correponds to a parameter that specifies the name of the database to be used.
	 * The value of this variable is 'Database' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBCollectionReader/desc/DBCollectionReader"
	 **/
	private static final String PARAM_DATABASE = "Database";
	
	/** Correponds to a parameter that specifies the username fof the specified database.
	 * The value of this variable is 'User' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBCollectionReader/desc/DBCollectionReader"
	 **/
	private static final String PARAM_USER = "User";
	
	/** Correponds to a parameter that specifies the password fof the specified database.
	 * The value of this variable is 'Password' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBCollectionReader/desc/DBCollectionReader"
	 **/
	private static final String PARAM_PASSWORD = "Password";
	
	
	/** Correponds to an optional parameter that contains the language of the documents in the database.
	 * If not specified, the default system encoding will be used.
	 * The value of this variable is 'sql_select' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBCollectionReader/desc/DBCollectionReader"
	 **/
	private static final String PARAM_SQL = "sql_select";
	

	/** Correponds to a parameter that specifies the sql statement to be executed in order to 
	 * retrieve the proper information.
	 * The value of this variable is 'Language' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBCollectionReader/desc/DBCollectionReader"
	 **/
	private static final String PARAM_LANGUAGE = "Language";
	
	/** DAO Factory object. */
	private DAOFactory daoFactory;
	
	/** XMI DAO object. */
	private DocumentDAO documentDAO;
	
	private ResultSet documents;
	
	private int documentSize;
	
	private String language;
	
	private int documentCounter;
	
	/** ID of the document in process **/
	private String documentID;

	/** Document in process **/
	private String documentData;
	
	
	/**
     * Initialize the component. Retrieve the parameters and process them, 
     * parsing the field descriptions and preparing the structures needed to
     * process the documents.
     *
     * @param aContext
     *            The UIMA context.
     *
     * @throws ResourceInitializationException
     *             If an error occurs with some resource.
     *
     * @see org.apache.uima.analysis_component.AnalysisComponent_ImplBase#initialize(org.apache.uima.UimaContext)
     */
	public void initialize() throws ResourceInitializationException {
		
		System.out.println("DBCollectionReader: initialize()...");
		logger.info("initialize()...");
		
		String dbms = (String) getUimaContext().getConfigParameterValue(PARAM_DBMS);
		String server = (String) getUimaContext().getConfigParameterValue(PARAM_SERVER);
		int port = (Integer) getUimaContext().getConfigParameterValue(PARAM_PORT);
		String database = (String) getUimaContext().getConfigParameterValue(PARAM_DATABASE);
		String user = (String) getUimaContext().getConfigParameterValue(PARAM_USER);
		String password = (String) getUimaContext().getConfigParameterValue(PARAM_PASSWORD);
		
		String sql_sentence = (String) getUimaContext().getConfigParameterValue(PARAM_SQL);
		
		this.language = (String) getUimaContext().getConfigParameterValue(PARAM_LANGUAGE);	
		
		if((dbms == null || dbms.length() == 0) || 
				(server == null || server.length() == 0) || 
				(new Integer(port) == null) || 
				(database == null || database.length() == 0) || 
				(user == null || user.length() == 0) || 
				(password == null || password.length() == 0) || 
				(sql_sentence == null || sql_sentence.length() == 0)){
			
			throw new ResourceInitializationException();
		}
		
		logger.info("initialize() - dbms: " + dbms);
		logger.info("initialize() - server: " + server);
		logger.info("initialize() - port: " + port);
		logger.info("initialize() - database: " + database);
		logger.info("initialize() - user: " + user);
		logger.info("initialize() - password: " + password);
		
		logger.info("initialize() - sql_sentence: " + sql_sentence);
		
		if(dbms.equals(MySQL)){
			
			System.out.println("DBCollectionReader: initialize() - Using MySQL as DBMS.");
		
			this.daoFactory = DAOFactory.getDAOFactory(DAOFactory.MYSQL);
			
			Hashtable<String, String> connectionParams = new Hashtable<String, String>();
			connectionParams.put("server", server);
			connectionParams.put("port", String.valueOf(port));
			connectionParams.put("database", database);
			connectionParams.put("user", user);
			connectionParams.put("password", password);
			
			this.documentDAO = this.daoFactory.getDocumentDAO(connectionParams);
			
			this.documentDAO.setSQLSentence(sql_sentence);
			
			try{
				
				this.documents = this.documentDAO.getDocumentsText();				
				this.documentSize = this.documentDAO.getNumberOfDocumentsText();
			}
			catch (DAOException e){
				
				throw new ResourceInitializationException(e);
			}
		}
		
		this.documentCounter = 0;
		
		logger.info("initialize() - Done.");
	}

	public void getNext(CAS aCAS) throws IOException, CollectionException {
		
		JCas jcas;
		
	    try{
	    	
	      jcas = aCAS.getJCas();
	    }
	    catch(CASException e){
	    	
	      throw new CollectionException(e);
	    }
	    
	     // put document in CAS
	    jcas.setDocumentText(documentData);

	    // set language if it was explicitly specified as a configuration parameter
	    if(this.language != null){
	    	
	      jcas.setDocumentLanguage(this.language);
	    }

	    // Also store location of source document in CAS. This information is critical
	    // if CAS Consumers will need to know where the original document contents are located.
	    // For example, the Semantic Search CAS Indexer writes this information into the
	    // search index that it creates, which allows applications that use the search index to
	    // locate the documents that satisfy their semantic queries.
	    SourceDocumentInformation srcDocInfo = new SourceDocumentInformation(jcas);
	    srcDocInfo.setUri("file:///" + this.documentID); // needs to be a valid URI with a known protocol
	    srcDocInfo.setOffsetInSource(0);
	    srcDocInfo.setDocumentSize((int) this.documentData.length());
	    
	    this.documentCounter += 1;
	    
	    srcDocInfo.setLastSegment(true);
	    srcDocInfo.addToIndexes();
	}

	public boolean hasNext() throws IOException, CollectionException {

		
		try{
			
			boolean hasNext = this.documents.next();
			
			if(!hasNext){
				
				this.documentDAO.closeConnection();
			}
			else{
				
				//get document 
			    this.documentData = this.documents.getString("text");			
			    
				 //get document id
				this.documentID = this.documents.getString("id");				
			}

			return hasNext;
		}
		catch(Exception e){
			
			try{
		
				// try to reconnect and continue.
				this.documentDAO.getDocumentsTextFrom(this.documentID);
				boolean hasNext = this.documents.next();
				
				if(!hasNext){
					
					this.documentDAO.closeConnection(); 
					return false;
				}
			    
				//get document 
			    this.documentData = this.documents.getString("text");			
			    
				 //get document id
				this.documentID = this.documents.getString("id");	
				
				return true;
			}
			catch(Exception E){
				
		    	System.err.println("ERROR in reconnect");
				throw new CollectionException(E);
			}
		}
	}

	public Progress[] getProgress(){
		
		return new Progress[] { new ProgressImpl(this.documentCounter, this.documentSize, Progress.ENTITIES) };
	}

	public void close() throws IOException {
		
	}
}
