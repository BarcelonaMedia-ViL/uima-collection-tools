package org.barcelonamedia.uima.DAO;

import java.util.Hashtable;


public abstract class DAOFactory{

	// List of DAO types supported by the factory
	public static final int MYSQL = 1;
	//--------------------------------------------
	
	public static DAOFactory getDAOFactory(int whichFactory){
		  
		switch(whichFactory){
			case MYSQL: 
				return new MySQLDAOFactory();
		    default: 
		    	return null;
		}
	}

	public abstract XMIDAO getDocumentDAO(Hashtable<String, String> connectionParams);
}
