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

package org.barcelonamedia.uima.reader.solr;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
//import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.examples.SourceDocumentInformation;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

import org.uimafit.component.CasCollectionReader_ImplBase;
import org.uimafit.descriptor.ConfigurationParameter;

public class SolrCollectionReader extends CasCollectionReader_ImplBase{

	/** The logger object. */
	private static final Logger logger = Logger.getLogger(SolrCollectionReader.class.toString());

	@ConfigurationParameter(name="SolrUrl", mandatory=true, defaultValue="http://localhost:8983/solr/",
			description="URL of Solr service")
	private String server;
	@ConfigurationParameter(name="IdField", mandatory=true, defaultValue="id",
			description="Solr field that contains the document ID")
	private String idField;
	@ConfigurationParameter(name="TextField", mandatory=true, defaultValue="text",
			description="Solr field that contains the document text")
	private String textField;
	@ConfigurationParameter(name="Language", mandatory=false,
			description="the language of the document")
	private String language;
	@ConfigurationParameter(name="SolrQuery", mandatory=true, defaultValue="*:*",
			description="the query to select documents")
	private String queryString;

	// current document
	private int documentCounter;

	// list of document ids to retrieve
	// TODO: the whole list of ids is kept in memory, move to fully streaming solution
	private List<String> documentList;

	// total number of documents, should be equivalent to documentList.size()
	private long documentNumber;

	private SolrDao solrDao;

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
	 */
	public void initialize(UimaContext context) throws ResourceInitializationException {

		System.out.println("SolrCollectionReader: initialize()...");
		logger.info("initialize()...");

		try {
			this.solrDao = new SolrDao(server,idField,textField);
		} catch (IOException e) {
			throw new ResourceInitializationException(e);
		}
		this.documentCounter = 0;
		try {
			this.documentNumber = this.solrDao.getDocNum(queryString);
			this.documentList = this.solrDao.getDocIds(queryString);
		} catch (SolrServerException e) {
			throw new ResourceInitializationException(e);
		}
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
		String documentId = this.documentList.get(this.documentCounter);
		String documentText;
		try {
			documentText = this.solrDao.getDocText(documentId);
		} catch (SolrServerException e) {
			throw new CollectionException(e);
		}
		jcas.setDocumentText(documentText);
		// set language if it was explicitly specified as a configuration parameter
		if(this.language != null){
			jcas.setDocumentLanguage(this.language);
		}
		SourceDocumentInformation srcDocInfo = new SourceDocumentInformation(jcas);
		srcDocInfo.setUri("file:///" + documentId); // needs to be a valid URI with a known protocol
		srcDocInfo.setOffsetInSource(0);
		srcDocInfo.setDocumentSize((int) documentText.length());
		this.documentCounter += 1;
		srcDocInfo.setLastSegment(true);
		srcDocInfo.addToIndexes();
	}

	public boolean hasNext() throws IOException, CollectionException {
		return (this.documentCounter<this.documentNumber)? true: false;
	}

	public Progress[] getProgress(){
		return new Progress[] { new ProgressImpl(this.documentCounter, (int) this.documentNumber, Progress.ENTITIES) };
	}

	public void close() throws IOException {
	}
}
