package org.barcelonamedia.uima.DAO;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;


public class MySQLDAOSupport{
	
	private static String connectionURL = "jdbc:mysql://%s:%s/%s?user=%s&password=%s";
	
	public MySQLDAOSupport(Hashtable<String, String> connectionParams){
		
		connectionURL = String.format(connectionURL, connectionParams.get("server"),
								connectionParams.get("port"),
								connectionParams.get("database"),
								connectionParams.get("user"),
								connectionParams.get("password"));
	}

	public static Connection getConnection() throws SQLException {

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
	
	public static Statement prepareStatement(Connection connection) throws SQLException{
    
		Statement statement = connection.createStatement();
		
	    return statement;
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
