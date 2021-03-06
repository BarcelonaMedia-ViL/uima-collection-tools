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

package org.barcelonamedia.uima.consumer.SQLAnnotationsConsumer.DAO;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;


public class MySQLDAOSupport{
	
	private static final String connectionURL_Pattern = "jdbc:mysql://%s:%s/%s?user=%s&password=%s";
	private static String connectionURL;
	
	public MySQLDAOSupport(Hashtable<String, String> connectionParams){
		
		connectionURL = String.format(connectionURL_Pattern, connectionParams.get("server"),
								connectionParams.get("port"),
								connectionParams.get("database"),
								connectionParams.get("user"),
								connectionParams.get("password"));
	}

	public static Connection getConnection() throws SQLException{

		try{
			Class.forName("com.mysql.jdbc.Driver");
			
			Connection connection = DriverManager.getConnection(connectionURL);
			
			if(connection == null){

				throw new SQLException("No database connection");
			}
			else{
				
				return connection;
			}
		}
		catch(ClassNotFoundException e){
			
			System.err.print(e.getMessage());
		}
		
		return null;
	}
	
	public static PreparedStatement prepareStatement(Connection connection, String sql, boolean returnGeneratedKeys, Object... values) throws SQLException{
    
		PreparedStatement preparedStatement = connection.prepareStatement(sql, returnGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
		setValues(preparedStatement, values);
		
	    return preparedStatement;
	}
	
	public static void setValues(PreparedStatement preparedStatement, Object... values) throws SQLException{
		
		for(int i = 0; i < values.length; i++){
			
			preparedStatement.setObject(i + 1, values[i]);
		}
    }
	
	public static boolean connectionIsValid(Connection connection) throws SQLException{
    	
		Statement stmt = null;
		ResultSet rs = null;
		
		try{
			
		   stmt = connection.createStatement();		   
		   rs = stmt.executeQuery("SELECT 1");
		   
		   if(rs.next()){

			   return true;
		   }
		   else{

			   return false;
		   }
		}
		catch(SQLException e){
		
		   return false;
		}
		finally{
			
		   if(stmt != null) stmt.close();
		   if(rs != null) rs.close();
		}
    }
	
    public static void close(Connection connection) throws SQLException{
    	
        if(connection != null){
        	
        	connection.close();
        }
    }

    public static void close(Statement statement) throws SQLException{
    	
        if (statement != null){

        	statement.close();
        }
    }

    public static void close(ResultSet resultSet) throws SQLException{
    	
        if(resultSet != null){
        	
        	resultSet.close();
        }
    }

    public static void close(Connection connection, Statement statement) throws SQLException{
    	
        close(statement);
        close(connection);
    }

    public static void close(Connection connection, Statement statement, ResultSet resultSet) throws SQLException{
    	
        close(resultSet);
        close(statement);
        close(connection);
    }
}
