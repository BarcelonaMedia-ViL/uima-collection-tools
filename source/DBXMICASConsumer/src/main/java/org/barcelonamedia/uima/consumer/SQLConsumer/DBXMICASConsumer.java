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


package org.barcelonamedia.uima.consumer.SQLConsumer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CasConsumer_ImplBase;
import org.apache.uima.examples.SourceDocumentInformation;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceProcessException;
import org.apache.uima.util.ProcessTrace;
import org.apache.uima.util.XMLSerializer;
import org.barcelonamedia.uima.consumer.SQLConsumer.DAO.DAOException;
import org.barcelonamedia.uima.consumer.SQLConsumer.DAO.DAOFactory;
import org.barcelonamedia.uima.consumer.SQLConsumer.DAO.XMIDAO;
import org.barcelonamedia.uima.consumer.SQLConsumer.DTO.XMIDTO;

public class DBXMICASConsumer extends CasConsumer_ImplBase{

	/** The logger object. */
	private static final Logger logger = Logger.getLogger(DBXMICASConsumer.class.toString());
	
	// Suported DBMS:  -----------------------------------------
	
	private static final String MySQL = "MySQL";
	
	//----------------------------------------------------------
	
	
	/** Correponds to a parameter that specifies DBMS to be used.
	 * The value of this variable is 'DBMS' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String PARAM_DBMS = "DBMS";
	
	/** Correponds to a parameter that specifies the server where DBMS is being hosted.
	 * The value of this variable is 'Server' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String PARAM_SERVER = "Server";
	
	/** Correponds to a parameter that specifies port to be used to connect to the specified DBMS.
	 * The value of this variable is 'Port' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String PARAM_PORT = "Port";
	
	/** Correponds to a parameter that specifies the name of the database to be used.
	 * The value of this variable is 'Database' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String PARAM_DATABASE = "Database";
	
	/** Correponds to a parameter that specifies the username fof the specified database.
	 * The value of this variable is 'User' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String PARAM_USER = "User";
	
	/** Correponds to a parameter that specifies the password fof the specified database.
	 * The value of this variable is 'Password' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String PARAM_PASSWORD = "Password";
	
	
	/** Correponds to a parameter that specifies the database table to be used.
	 * The value of this variable is 'table' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String PARAM_TABLE = "table";
	
	/** Correponds to a parameter that specifies the field of the specified database table to be used for inserting xmi id
	 * The value of this variable is 'xmi_id_field' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String PARAM_XMI_ID_FIELD = "xmi_id_field";
	
	/** Correponds to a parameter that specifies the field of the specified database table to be used for inserting xmi data
	 * The value of this variable is 'xmi_data_field' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String PARAM_XMI_DATA_FIELD = "xmi_data_field";
	
	/** Correponds to a parameter that specifies whether XMI is to be compressed or not before inserting it into database.
	 * The value of this variable is 'compression' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String PARAM_DO_COMPRESSION = "compression";
	
	
	/** Correponds to a parameter that specifies whether whole document URI or document name is to be used as document ID into the database.
	 * The value of this variable is 'fullURI' which is the name of 
	 * the parameter in the descriptor file that must be set.
	 * @see "/DBXMICASConsumer/desc/DBXMICASConsumer.xml"
	 **/
	private static final String FULL_URI = "fullURI";
	
	
	/** DAO Factory object. */
	private DAOFactory daoFactory;
	
	/** XMI DAO object. */
	private XMIDAO xmiDAO;
	
    /** document number to use as id when no SourceDocumentInformation available */
	private int mDocNum;
	
	/** XMI compression flag **/
	private Boolean do_compression;
	
	/** Full URI flag **/
	private Boolean fullURI;

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
    
		System.out.println("DBXMICASConsumer: initialize()...");
		logger.info("initialize()...");
		
		String dbms = (String) getUimaContext().getConfigParameterValue(PARAM_DBMS);
		String server = (String) getUimaContext().getConfigParameterValue(PARAM_SERVER);
		int port = (Integer) getUimaContext().getConfigParameterValue(PARAM_PORT);
		String database = (String) getUimaContext().getConfigParameterValue(PARAM_DATABASE);
		String user = (String) getUimaContext().getConfigParameterValue(PARAM_USER);
		String password = (String) getUimaContext().getConfigParameterValue(PARAM_PASSWORD);
		
		String table = (String) getUimaContext().getConfigParameterValue(PARAM_TABLE);	
		String xmi_id_field = (String) getUimaContext().getConfigParameterValue(PARAM_XMI_ID_FIELD);	
		String xmi_data_field = (String) getUimaContext().getConfigParameterValue(PARAM_XMI_DATA_FIELD);
		
		this.mDocNum = 0;
		this.do_compression = (Boolean) getUimaContext().getConfigParameterValue(PARAM_DO_COMPRESSION);
		this.fullURI = (Boolean) getUimaContext().getConfigParameterValue(FULL_URI);
		
		if((dbms == null || dbms.length() == 0) || 
				(server == null || server.length() == 0) || 
				(new Integer(port) == null) || 
				(database == null || database.length() == 0) || 
				(user == null || user.length() == 0) || 
				(password == null || password.length() == 0) || 
				(table == null || table.length() == 0) || 
				(xmi_id_field == null || xmi_id_field.length() == 0) || 
				(xmi_data_field == null || xmi_data_field.length() == 0)){
			
			throw new ResourceInitializationException();
		}
		
		logger.info("initialize() - dbms: " + dbms);
		logger.info("initialize() - server: " + server);
		logger.info("initialize() - port: " + port);
		logger.info("initialize() - database: " + database);
		logger.info("initialize() - user: " + user);
		logger.info("initialize() - password: " + password);
		
		logger.info("initialize() - table: " + table);
		logger.info("initialize() - xmi_id_field: " + xmi_id_field);
		logger.info("initialize() - xmi_data_field: " + xmi_data_field);
		
		if(dbms.equals(MySQL)){
			
			System.out.println("DBXMICASConsumer: initialize() - Using MySQL as DBMS.");
		
			this.daoFactory = DAOFactory.getDAOFactory(DAOFactory.MYSQL);
			
			Hashtable<String, String> connectionParams = new Hashtable<String, String>();
			connectionParams.put("server", server);
			connectionParams.put("port", String.valueOf(port));
			connectionParams.put("database", database);
			connectionParams.put("user", user);
			connectionParams.put("password", password);
			
			this.xmiDAO = this.daoFactory.getXMIDAO(connectionParams);
			
			Hashtable<String, String> tableInfo = new Hashtable<String, String>();
			tableInfo.put("table", table);
			tableInfo.put("xmi_id_field", xmi_id_field);
			tableInfo.put("xmi_data_field", xmi_data_field);
			
			this.xmiDAO.setTableInfo(tableInfo);
			
			try{
				
				this.xmiDAO.init();
			}
			catch(DAOException e){

				throw new ResourceInitializationException();
			}
		}
		
		logger.info("initialize() - Done.");
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
	public void processCas(CAS cas) throws ResourceProcessException{
		
		try{

			ByteArrayOutputStream xmi_baos = new ByteArrayOutputStream();
			
			XmiCasSerializer ser = new XmiCasSerializer(cas.getTypeSystem());
		    XMLSerializer xmlSer = new XMLSerializer(xmi_baos, false);
		    
		    ser.serialize(cas, xmlSer.getContentHandler());
		    
		    // Retrieve XMI id
		    String xmi_id = new String();
		    
		    JCas jcas = cas.getJCas();
		    FSIterator it = jcas.getAnnotationIndex(SourceDocumentInformation.type).iterator();
		    
		    if(it.hasNext()){
		    
		    	SourceDocumentInformation sdi = (SourceDocumentInformation) it.next();		  
		    	
		    	if(this.fullURI){
		    	
		    		xmi_id = sdi.getUri().toString();
		    	}
		    	else{
		    		
		    		xmi_id = new File(new URL(sdi.getUri()).getPath()).getName();
		    	}
		    	
		    	if(sdi.getOffsetInSource() > 0 || !sdi.getLastSegment()){
		    		
		    		xmi_id += ("_" + sdi.getOffsetInSource() + "_" + sdi.getDocumentSize());
		        }
		    }

			if(xmi_id.length() == 0){

				xmi_id = "doc" + this.mDocNum++;
			}
			///////////////////////////////////////////////////////////////////////////////////
			
			XMIDTO xmidto = null;
			
			//XMI compression
			if(this.do_compression){
				
				ByteArrayOutputStream compressed_xmi_baos = new ByteArrayOutputStream();
				Deflater deflater = new Deflater();
		
				DeflaterOutputStream deflaterOutputStream  = new DeflaterOutputStream(compressed_xmi_baos, deflater);
				
				deflaterOutputStream.write(xmi_baos.toByteArray());
				deflaterOutputStream.close();
				
				xmidto = new XMIDTO(xmi_id, compressed_xmi_baos.toByteArray());
			}
			else{
				
				xmidto = new XMIDTO(xmi_id, xmi_baos.toByteArray());
			}	    
		    
		    this.xmiDAO.insert(xmidto);
		}
		catch(Exception e){
			
			logger.log(Level.SEVERE, e.getMessage());
			throw new ResourceProcessException(e);
		}
	}
	
	/**
	 * 
	 * 
	 */
	public void collectionProcessComplete(ProcessTrace arg0) throws ResourceProcessException, IOException{
	
		try{
			
			this.xmiDAO.closeConnection();
		}
		catch(DAOException e){

			logger.log(Level.SEVERE, e.getMessage());
			throw new ResourceProcessException(e);
		}
		
		System.out.println("DBXMICASConsumer: collectionProcessComplete()...");
		logger.info("collectionProcessComplete() - Done.");
	}
}
