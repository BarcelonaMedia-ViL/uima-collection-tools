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


package org.barcelonamedia.uima.consumer.SQLConsumer.DTO;


public class XMIDTO{

	private String id;
	private byte[] xmi_data;
	
	
	public XMIDTO() {
		super();
	}
	
	public XMIDTO(String id, byte[] xmi_data) {
		super();
		this.id = id;
		this.xmi_data = xmi_data;
	}
	
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public byte[] getXmi_data() {
		return xmi_data;
	}
	public void setXmi_data(byte[] xmi_data) {
		this.xmi_data = xmi_data;
	}
}
