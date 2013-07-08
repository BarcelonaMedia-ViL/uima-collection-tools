package org.barcelonamedia.uima.reader.solr;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.uima.resource.ResourceInitializationException;
import org.uimafit.factory.CollectionReaderFactory;
import org.xml.sax.SAXException;

public class GetSolrReaderDescriptor {

	/**
	 * @param args
	 * @throws ResourceInitializationException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws ResourceInitializationException, FileNotFoundException, SAXException, IOException {
		CollectionReaderFactory.createDescription(SolrCollectionReader.class).toXML(System.out);
	}

}
