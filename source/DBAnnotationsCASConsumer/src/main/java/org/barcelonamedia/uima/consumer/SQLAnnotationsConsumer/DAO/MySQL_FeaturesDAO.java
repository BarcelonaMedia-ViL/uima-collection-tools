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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;

import org.barcelonamedia.uima.consumer.SQLAnnotationsConsumer.DTO.FeaturesDTO;
import org.barcelonamedia.uima.consumer.SQLAnnotationsConsumer.DAO.DAOException;
import org.barcelonamedia.uima.consumer.utils.CollectionUtils;


public class MySQL_FeaturesDAO extends MySQLDAOSupport implements FeaturesDAO{

	private static String SQL_INSERT_TEMPLATE = "INSERT INTO %s (%s, %s) VALUES (%s)";
	private static String SQL_CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (%s)";
	private static String SQL_CREATE_TABLE_IFNOTEXISTS_TEMPLATE = "CREATE TABLE IF NOT EXISTS %s (%s)";

	private String SQL_INSERT;
	private String SQL_CREATE_TABLE;

	private Connection connection;
	private PreparedStatement preparedStatement;


	public MySQL_FeaturesDAO(Hashtable<String, String> connectionParams){

		super(connectionParams);
	}

	public void init() throws DAOException{

		try{

			this.connection = getConnection();			
			this.preparedStatement = this.connection.prepareStatement(SQL_INSERT);
		}
		catch(SQLException e){

			throw new DAOException(e);
		}
	}

	public void insert(FeaturesDTO featuresdto) throws DAOException{

		ResultSet generatedKeys = null;

		try{

			if(!connectionIsValid(this.connection)){

				close(this.preparedStatement);
				close(this.connection);

				this.init();
			}

			setValues(this.preparedStatement, (Object[])featuresdto.getFeatures()); 

			int affectedRows = this.preparedStatement.executeUpdate();

			if (affectedRows == 0) {
				throw new DAOException("Creating features entry failed, no rows affected.");
			}
		}
		catch (SQLException e){
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

	public void setTableInfo(Hashtable<String, String> tableInfo){

		ArrayList<String> prepared_fields = new ArrayList<String>();

		String[] features = tableInfo.get("features").split(","); 

		//Adds one more atribute due to doc_id
		prepared_fields.add("?");

		for(int i=0; i<features.length; i++){

			prepared_fields.add("?");
		}

		this.SQL_INSERT = String.format(SQL_INSERT_TEMPLATE, tableInfo.get("table"), tableInfo.get("doc_id"), tableInfo.get("features"), CollectionUtils.join(prepared_fields, ","));
	}

	/*public boolean tableExists(String table) throws DAOException{

		Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        boolean table_exists = false;

        try{

        	connection = getConnection();

        	DatabaseMetaData dbm = connection.getMetaData();
        	resultSet = dbm.getTables(null, null, table, null);

        	if(resultSet.next()){

        		table_exists = true;
        	}
        } 
        catch (SQLException e){

            throw new DAOException(e);
        }
        finally{

            try{

				close(connection, preparedStatement, resultSet);
			} 
            catch(SQLException e){
				throw new DAOException(e);
			}
        }

		return table_exists;
	}*/


	public void createTable(Hashtable<String, String> tableInfo) throws DAOException {
		createTable(tableInfo, false);
	}

	public void createTable(Hashtable<String, String> tableInfo, Boolean useExistingTable) throws DAOException{

		Connection connection = null;
		Statement statement = null;

		try{

			String table_fields = new String();
			String doc_id = tableInfo.get("doc_id");
			String[] features = tableInfo.get("features").split(",");

			table_fields += "id bigint(20) NOT NULL AUTO_INCREMENT,";
			table_fields += doc_id + " TEXT NOT NULL,";

			for(int i = 0; i<features.length; i++){

				table_fields += features[i] + " MEDIUMTEXT,";
			}

			table_fields += "PRIMARY KEY (id)";

			String template = useExistingTable? SQL_CREATE_TABLE_IFNOTEXISTS_TEMPLATE : SQL_CREATE_TABLE_TEMPLATE;
			this.SQL_CREATE_TABLE = String.format(template, tableInfo.get("table"), table_fields);

			connection = getConnection();
			statement = connection.createStatement();
			
			statement.executeUpdate(this.SQL_CREATE_TABLE);
		}
		catch(SQLException e){

			System.out.println("MySQL_FeaturesDAO: catch(): " + e.getMessage());
			throw new DAOException(e);
		}
		finally{

			try{

				close(connection, statement);
			}
			catch(SQLException e){

				System.out.println("MySQL_FeaturesDAO: finally catch(): " + e.getMessage());
				throw new DAOException(e);
			}
		}
	}

	public void closeConnection() throws DAOException{

		try{

			close(this.preparedStatement);
			close(this.connection);
		} 
		catch(SQLException e){

			throw new DAOException(e);
		}
	}

}
