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

package org.barcelonamedia.uima.reader.DBXMIReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.Hashtable;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.commons.io.IOUtils;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import org.barcelonamedia.uima.reader.DBXMIReader.DAO.DAOException;
import org.barcelonamedia.uima.reader.DBXMIReader.DAO.DAOFactory;
import org.barcelonamedia.uima.reader.DBXMIReader.DAO.XMIDAO;
import org.xml.sax.SAXException;


public class DBXMICollectionReader extends CollectionReader_ImplBase{
	
	/** The logger object. */
	private static final Logger logger = Logger.getLogger(DBXMICollectionReader.class.toString());
	
	// Suported DBMS:  -----------------------------------------
	
	private static final String MySQL = "MySQL";
	
	//----------------------------------------------------------
	
	private static final int DEFAULT_NUM_OF_DOCUMENTS = 2;
	
	
	/** Correponds to a parameter that specifies DBMS to be used.
	 * The value of this variable is 'DBMS' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICollectionReader/desc/DBXMICollectionReader"
	 **/
	private static final String PARAM_DBMS = "DBMS";
	
	/** Correponds to a parameter that specifies the server where DBMS is being hosted.
	 * The value of this variable is 'Server' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICollectionReader/desc/DBXMICollectionReader"
	 **/
	private static final String PARAM_SERVER = "Server";
	
	/** Correponds to a parameter that specifies port to be used to connect to the specified DBMS.
	 * The value of this variable is 'Port' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICollectionReader/desc/DBXMICollectionReader"
	 **/
	private static final String PARAM_PORT = "Port";
	
	/** Correponds to a parameter that specifies the name of the database to be used.
	 * The value of this variable is 'Database' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICollectionReader/desc/DBXMICollectionReader"
	 **/
	private static final String PARAM_DATABASE = "Database";
	
	/** Correponds to a parameter that specifies the username fof the specified database.
	 * The value of this variable is 'User' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICollectionReader/desc/DBXMICollectionReader"
	 **/
	private static final String PARAM_USER = "User";
	
	/** Correponds to a parameter that specifies the password fof the specified database.
	 * The value of this variable is 'Password' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICollectionReader/desc/DBXMICollectionReader"
	 **/
	private static final String PARAM_PASSWORD = "Password";
	
	
	/** Correponds to an optional parameter that contains the language of the documents in the database.
	 * If not specified, the default system encoding will be used.
	 * The value of this variable is 'sql_select' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICollectionReader/desc/DBXMICollectionReader"
	 **/
	private static final String PARAM_SQL = "sql_select";
	
	
	/** Correponds to a parameter that specifies whether XMI is to be decompressed or not before inserting it into CAS.
	 * The value of this variable is 'compression' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICollectionReader/desc/DBXMICollectionReader"
	 **/
	private static final String PARAM_DO_DECOMPRESSION = "compression";
	
	
	/** Correponds to a parameter that specifies whether checking of number of documents to be processed is to be done or not. 
	 * This is useful for some cases in which SQL sentence are quite expensive.
	 * The value of this variable is 'enableDocCounter' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICollectionReader/desc/DBXMICollectionReader"
	 **/
	private static final String PARAM_ENABLE_DOC_COUNTER = "enableDocCounter";
	
	
	/** Name of the configuration parameter that must be set to indicate if the
	 * execution fails if an encountered type is unknown.
	 * The value of this variable is 'mFailOnUnknownType' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICollectionReader/desc/DBXMICollectionReader"
	 **/
	public static final String PARAM_FAILUNKNOWN = "FailOnUnknownType";
	
	
	/** DAO Factory object. */
	private DAOFactory daoFactory;
	
	/** XMI DAO object. */
	private XMIDAO documentDAO;
	
	/** XMI decompression flag **/
	private Boolean do_decompression;
	
	/** Flag which enables/disables number of documents retrieving **/
	private Boolean retrieve_number_of_docs;
	
	private ResultSet documents;
	
	private Boolean mFailOnUnknownType;
	
	private int documentSize;
	
	private int currentIndex;
	
	/** ID of the document being processed **/
	private String documentID;

	/** Documento in process **/
	private InputStream documentData;
	
	
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
	public void initialize() throws ResourceInitializationException{
		
		System.out.println("DBXMICollectionReader: initialize()...");
		logger.info("initialize()...");
		
		String dbms = (String) getUimaContext().getConfigParameterValue(PARAM_DBMS);
		String server = (String) getUimaContext().getConfigParameterValue(PARAM_SERVER);
		int port = (Integer) getUimaContext().getConfigParameterValue(PARAM_PORT);
		String database = (String) getUimaContext().getConfigParameterValue(PARAM_DATABASE);
		String user = (String) getUimaContext().getConfigParameterValue(PARAM_USER);
		String password = (String) getUimaContext().getConfigParameterValue(PARAM_PASSWORD);
		
		String sql_sentence = (String) getUimaContext().getConfigParameterValue(PARAM_SQL);
		
		this.do_decompression = (Boolean) getUimaContext().getConfigParameterValue(PARAM_DO_DECOMPRESSION);
		this.retrieve_number_of_docs = (Boolean) getUimaContext().getConfigParameterValue(PARAM_ENABLE_DOC_COUNTER);
		
		this.mFailOnUnknownType = (Boolean) getUimaContext().getConfigParameterValue(PARAM_FAILUNKNOWN);	
		
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
			
			System.out.println("DBXMICollectionReader: initialize() - Using MySQL as DBMS.");
		
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
				
				if(this.retrieve_number_of_docs){
				
					this.documentSize = this.documentDAO.getNumberOfXMI();
				}
				else{
					
					this.documentSize = DEFAULT_NUM_OF_DOCUMENTS;
				}
				
				this.documents = this.documentDAO.getXMI();				
			}
			catch(DAOException e){
				
				throw new ResourceInitializationException(e);
			}
		}
		
		this.currentIndex = 0;
		
		logger.info("initialize() - Done.");
	}

	public void getNext(CAS aCAS) throws IOException, CollectionException{

	    try{
		    
		    if(this.do_decompression){
			    
			    //Create the decompressor and give it the data to compress
			    Inflater decompressor = new Inflater();
			    byte[] documentDataByteArray = IOUtils.toByteArray(this.documentData);
			    
			    decompressor.setInput(documentDataByteArray);
	
	            //Create an expandable byte array to hold the decompressed data
			    ByteArrayOutputStream bos = new ByteArrayOutputStream(documentDataByteArray.length);
			    
			    //Decompress the data
			    byte[] buf = new byte[1024];
			    
			    while(!decompressor.finished()){
			        
			    	try{
			        
			    		int count = decompressor.inflate(buf);
			            bos.write(buf, 0, count);
			        }
			    	catch(DataFormatException e){
			    		
			    		System.err.println("ERROR in Collection Reader " + e.getClass() + ": " + e.getMessage());
			    		throw new IOException();
			        }
			    }
			    
			    try{
			    	
			        bos.close();
			    }
			    catch(IOException e){
			    	
			    	System.err.println("ERROR in Collection Reader " + e.getClass() + ": " + e.getMessage());
		    		throw new IOException();
			    }
			    
			    //Get the decompressed data
			    byte[] decompressedData = bos.toByteArray();
			    
			    XmiCasDeserializer.deserialize(new ByteArrayInputStream(decompressedData), aCAS, ! this.mFailOnUnknownType);
		    }
		    else{
		    	
		    	XmiCasDeserializer.deserialize(this.documentData, aCAS, ! this.mFailOnUnknownType);
		    }
			
			this.currentIndex += 1;
		}
	    catch(SAXException e){

	    	System.err.println("ERROR in Collection Reader " + e.getClass() + ": " + e.getMessage());
			throw new CollectionException(e);
		}
	}

	public boolean hasNext() throws IOException, CollectionException{

		try{
			
			boolean hasNext = this.documents.next();
			
			if(!hasNext){
				
				this.documentDAO.closeConnection();
			}
			else{
				
				//get document 
			    this.documentData = this.documents.getBlob("xmi").getBinaryStream();
			    
				//get document id
				this.documentID = this.documents.getString("id");				
			}

			return hasNext;
		}
		catch(Exception e){
			
			try{
		
				// try to reconnect and continue.
				this.documentDAO.getXMIFrom(this.documentID);
				boolean hasNext = this.documents.next();
				
				if(!hasNext){
					
					this.documentDAO.closeConnection(); 
					return false;
				}
			    
				//get document 
			    this.documentData = this.documents.getBlob("xmi").getBinaryStream();
			    
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
		
		return new Progress[] { new ProgressImpl(this.currentIndex, this.documentSize, Progress.ENTITIES) };
	}

	public void close() throws IOException{
		
	}
}
