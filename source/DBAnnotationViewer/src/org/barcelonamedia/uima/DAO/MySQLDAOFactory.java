package org.barcelonamedia.uima.DAO;

import java.util.Hashtable;

public class MySQLDAOFactory extends DAOFactory{

	@Override
	public XMIDAO getDocumentDAO(Hashtable<String, String> connectionParams){
		
		return new MySQL_XMIDAO(connectionParams);
	}
}
