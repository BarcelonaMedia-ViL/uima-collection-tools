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

import java.util.Hashtable;

import org.barcelonamedia.uima.consumer.SQLAnnotationsConsumer.DTO.FeaturesDTO;
import org.barcelonamedia.uima.consumer.SQLAnnotationsConsumer.DAO.DAOException;


public interface FeaturesDAO{

	public void init() throws DAOException;
	public void createTable(Hashtable<String, String> tableInfo) throws DAOException;
	public void createTable(Hashtable<String, String> tableInfo, Boolean useExistingTable) throws DAOException;
	public void setTableInfo(Hashtable<String, String> tableInfo);
	public void insert(FeaturesDTO featuresdto) throws DAOException;
	//public boolean tableExists(String table) throws DAOException;
	public void closeConnection() throws DAOException;
}
