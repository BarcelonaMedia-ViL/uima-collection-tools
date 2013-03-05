package org.barcelonamedia.uima.DAO;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;


public class MySQL_XMIDAO extends MySQLDAOSupport implements XMIDAO{

	private static String SQL_SELECT_XMI_TEMPLATE = "SELECT xmi FROM (%s) AS xmi WHERE id = \"%s\"";
	private static String SQL_SELECT_XMI_ID_TEMPLATE = "SELECT id FROM (%s) AS xmi";
	
	private String SQL_SELECT_XMI;
	private String SQL_SELECT_XMI_ID;
	
	private String provided_sql;
	
	private Connection connection;
	private Statement statement;
	private ResultSet resultSet;
	
	
	public MySQL_XMIDAO(Hashtable<String, String> connectionParams){
		
		super(connectionParams);
		
		this.provided_sql = new String();
	}


	public void setSQLSentence(String sql_sentence){
		
		this.provided_sql = sql_sentence;
		
		this.SQL_SELECT_XMI_ID = String.format(SQL_SELECT_XMI_ID_TEMPLATE, this.provided_sql);
	}
	
	public String[] getXMIList() throws DAOException{

		String[] xmi_list = new String[0];
		
		Connection connection = null;
		Statement statement = null;
        ResultSet resultSet = null;
        
        try{
        	
        	connection = getConnection();
            statement = prepareStatement(connection);
            resultSet = statement.executeQuery(this.SQL_SELECT_XMI_ID);
            
            if(resultSet.last()){
            	
            	xmi_list = new String[resultSet.getRow()];
             
            	// not rs.first() because the rs.next() below will move on, missing the first element
            	resultSet.beforeFirst(); 
            }
            
            int index = 0;
            while(resultSet.next()){
            	
            	xmi_list[index] = resultSet.getString("id");
            	index++;
            }
        }
        catch(SQLException e){
        	
            throw new DAOException(e);
        }
        finally{
        	
            try{
				close(connection, statement, resultSet);
			}
            catch(SQLException e){
				
            	throw new DAOException(e);
			}
        }
        
        return xmi_list;
	}
	

	public InputStream getXMI(String id) throws DAOException {

		InputStream xmi = null;
		
		Connection connection = null;
		Statement statement = null;
        ResultSet resultSet = null;
        
        try{
        	
        	this.SQL_SELECT_XMI = String.format(SQL_SELECT_XMI_TEMPLATE, this.provided_sql, id);
        	
        	connection = getConnection();
            statement = prepareStatement(connection);
            System.out.println(this.SQL_SELECT_XMI);
            resultSet = statement.executeQuery(this.SQL_SELECT_XMI);

            if(resultSet.next()){
            	
            	xmi = resultSet.getBlob("xmi").getBinaryStream();
            }
        }
        catch(SQLException e){
        	
            throw new DAOException(e);
        }
        finally{
        	
            try{
				close(connection, statement, resultSet);
			}
            catch(SQLException e){
				
            	throw new DAOException(e);
			}
        }
		
		return xmi;
	}
	
	public void closeConnection() throws DAOException{
		
		try{
			
			close(this.connection, this.statement, this.resultSet);
		}
		catch(SQLException e){
			
			throw new DAOException(e);
		}
	}
}
