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

package org.barcelonamedia.uima.consumer.SQLConsumer.DAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

import org.barcelonamedia.uima.consumer.SQLConsumer.DTO.XMIDTO;


public class MySQL_XMIDAO extends MySQLDAOSupport implements XMIDAO{

	private static String SQL_INSERT_TEMPLATE	 = "INSERT INTO %s (%s, %s) VALUES (?, ?)";
	
	private String SQL_INSERT;
	
	private Connection connection;
	private PreparedStatement preparedStatement;
	
	
	public MySQL_XMIDAO(Hashtable<String, String> connectionParams){
		
		super(connectionParams);
	}
	

	@Override
	public void init() throws DAOException{
		
		try{
			
			this.connection = getConnection();
			this.preparedStatement = this.connection.prepareStatement(this.SQL_INSERT);
		} 
		catch(SQLException e){
			
			throw new DAOException(e);
		}
	}
	
	@Override
	public void insert(XMIDTO xmidto) throws DAOException{
		
        ResultSet generatedKeys = null;
        
        Object[] values = {
        		
        		xmidto.getId(),
        		xmidto.getXmi_data(),
	    };
        
        try{
        	
        	if(!connectionIsValid(this.connection)){
        		
        		close(this.preparedStatement);
    			close(this.connection);
    			
        		this.init();
        	}
        	
        	setValues(this.preparedStatement, values);
            int affectedRows = this.preparedStatement.executeUpdate();
             
            if(affectedRows == 0){
             	
            	throw new DAOException("Creating XMI failed, no rows affected.");
            }
  
        } 
        catch(SQLException e){
        	
            throw new DAOException(e);
        }
        finally{
        	
            try{
            	
				close(generatedKeys);
			} 
            catch(SQLException e){
            	
				throw new DAOException(e);
			}
        }
	}

	@Override
	public void setTableInfo(Hashtable<String, String> tableInfo){
		
		this.SQL_INSERT = String.format(SQL_INSERT_TEMPLATE, tableInfo.get("table"),
								tableInfo.get("xmi_id_field"),
								tableInfo.get("xmi_data_field"));
	}

	@Override
	public void closeConnection() throws DAOException {
		
		try{
			
			close(this.preparedStatement);
			close(this.connection);
		} 
		catch(SQLException e){

			throw new DAOException(e);
		}
	}
}
