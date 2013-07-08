package org.barcelonamedia.uima.reader.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

/**
 * This class provides an interface to retrieve Solr documents.
 * All document ids must be first retrieved and stored in-memory, 
 * and then each document retrieved individually.
 * 
 * TODO: make this an iterable/iterator
 * 
 * @author jens.grivolla
 *
 */
public class SolrDao {
	
	/** The logger object. */
	private static final Logger logger = Logger.getLogger(SolrCollectionReader.class.toString());

	private static final int PAGE_SIZE = 1000;

	private SolrServer server;
	private String idField;
	private String textField;

	public SolrDao(String server, String idField, String textField) throws IOException {
		this.idField=idField;
		this.textField=textField;
		this.server = new HttpSolrServer(server);
	}

	public List<String> getDocIds(String queryString) throws SolrServerException {
		SolrQuery query = new SolrQuery().setFields(idField).setQuery(queryString).setRows(0);
		long numResults = server.query(query).getResults().getNumFound();
		List<String> docIds = new ArrayList<String>();
		for (long i = 0; i < numResults; i+=PAGE_SIZE) {
			docIds.addAll(getDocIds(queryString,PAGE_SIZE,i));
		}
		return docIds;
	}	

	private List<String> getDocIds(String queryString, int limit, Long offset) throws SolrServerException {
		List<String> docIds = new ArrayList<String>();
		SolrQuery query = new SolrQuery().setFields(idField).setQuery(queryString).setRows(limit).setStart(offset.intValue());
		QueryResponse rsp;
		rsp = server.query(query);
		SolrDocumentList docs = rsp.getResults();
		for (SolrDocument doc : docs) {
			String userId = (String) doc.get(idField);
			docIds.add(userId);
		}
		return docIds;
	}

	public String getDocText(String docId) throws SolrServerException {
		SolrQuery query = new SolrQuery();
		query.setQuery(idField+":"+docId); //TODO: use safer query construction
		query.setFields(textField);
		QueryResponse rsp;
		rsp = server.query(query);
		SolrDocumentList docs = rsp.getResults();
		if (docs.size() == 0) {
			logger.info("No such offer: "+docId);
		}
		if (docs.size() > 1) {
			logger.warning(String.format("getDocDetails: %i entries for id %s", docs.size(),docId));
		}
		SolrDocument doc = docs.get(0);
		return (String) doc.get(textField);
	}

	public long getDocNum(String queryString) throws SolrServerException {
		SolrQuery query = new SolrQuery().setFields(idField).setQuery(queryString).setRows(0);
		long numResults = server.query(query).getResults().getNumFound();
		return numResults;
	}

}
