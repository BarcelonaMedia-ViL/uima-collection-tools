package org.barcelonamedia.uima.DAO;

import java.io.InputStream;


public interface XMIDAO{

	public void setSQLSentence(String sql_sentence);
	public String[] getXMIList() throws DAOException;
	public InputStream getXMI(String id) throws DAOException;
	public void closeConnection() throws DAOException;
}
