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

package org.barcelonamedia.uima.reader.DBXMIReader.DAO;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;


public class MySQL_XMIDAO extends MySQLDAOSupport implements XMIDAO{

	private static String SQL_SELECT_ALL_XMI_TEMPLATE = "SELECT * FROM (%s) AS b ORDER BY id ASC";
	private static String SQL_SELECT_LAST_ID_TEMPLATE = "SELECT * FROM (%s) AS b WHERE id > %s ORDER BY id ASC";
	private static String SQL_SELECT_NUMBER_OF_XMI_TEMPLATE = "SELECT COUNT(*) AS size FROM (%s) t";
	
	private String SQL_SELECT_ALL_XMI;
	private String SQL_SELECT_LAST_ID;
	private String SQL_SELECT_NUMBER_OF_XMI;
	
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
		
		this.SQL_SELECT_ALL_XMI = String.format(SQL_SELECT_ALL_XMI_TEMPLATE, this.provided_sql);
		this.SQL_SELECT_NUMBER_OF_XMI = String.format(SQL_SELECT_NUMBER_OF_XMI_TEMPLATE, this.provided_sql);
	}


	public ResultSet getXMI() throws DAOException{
		
		this.connection = null;
        this.statement = null;
        this.resultSet = null;
        
        try{
        	       	
            this.connection = getConnection();
            this.statement = prepareStatement(connection);
            this.resultSet = statement.executeQuery(this.SQL_SELECT_ALL_XMI);
        }
        catch(SQLException e){
        	
            throw new DAOException(e);
        }
        
        return resultSet;
	}
	
	// This procedure tries to reconnect the server and continue the 
	// processing the query, it does a modification to the query:
	// adds "AND id > Last_id " and order by id asc
	public ResultSet getXMIFrom(String lastID) throws DAOException{
        
        try{
        	
        	this.SQL_SELECT_LAST_ID = String.format(SQL_SELECT_LAST_ID_TEMPLATE, this.provided_sql, lastID);
        	
        	close(this.connection, this.statement, this.resultSet);
        	
            this.connection = getConnection();
            this.statement = prepareStatement(connection);
            
            this.resultSet = statement.executeQuery(this.SQL_SELECT_LAST_ID);
        }
        catch (SQLException e){
        	
            throw new DAOException(e);
        }
        
        return resultSet;
	}


	public void closeConnection() throws DAOException{
		
		try{
			
			close(this.connection, this.statement, this.resultSet);
		}
		catch(SQLException e){
			
			throw new DAOException(e);
		}
	}


	public int getNumberOfXMI() throws DAOException{

		int size = 0;
		
		Connection connection = null;
		Statement statement = null;
        ResultSet resultSet = null;
        
        try{
        	
        	connection = getConnection();
            statement = prepareStatement(connection);
            resultSet = statement.executeQuery(this.SQL_SELECT_NUMBER_OF_XMI);
            
            if(resultSet.next()){
            	
            	size = resultSet.getInt("size");
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
        
        return size;
	}
}
