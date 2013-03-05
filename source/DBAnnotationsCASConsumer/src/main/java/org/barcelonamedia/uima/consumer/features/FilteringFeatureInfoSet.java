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

package org.barcelonamedia.uima.consumer.features;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class FilteringFeatureInfoSet{

	/**
	 * UIMA Feature qualified name.
	 */
	private String qualifiedName;
	
	/**
	 * Regular expression defining Feature's possible value(s)
	 */
	private String regex;
	
	private Pattern pattern;
	private Matcher matcher;
	

	public FilteringFeatureInfoSet(){

	}

	public FilteringFeatureInfoSet(String qualifiedName, String regex){

		this.qualifiedName = qualifiedName;
		this.regex = regex;
		
		this.initRegex();
	}
	

	public String getQualifiedName() {
		return qualifiedName;
	}

	public void setQualifiedName(String qualifiedName) {
		this.qualifiedName = qualifiedName;
	}

	public String getRegex() {
		return regex;
	}

	public void setRegex(String regex) {
		this.regex = regex;		
		this.initRegex();
	}
	
	public Matcher getMatcher() {
		return matcher;
	}

	private void initRegex(){

		this.pattern = Pattern.compile(this.regex);
		this.matcher = this.pattern.matcher("");
	}
}
