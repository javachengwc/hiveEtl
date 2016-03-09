package com.ultrapower.bigdata.tools;


import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class ParseArgs {
	public static final Logger log = Logger.getLogger(ParseArgs.class);

	private Map<String,String> map = null;
	
	public ParseArgs(String[] args) {
		map = new HashMap<String, String>() ;
		
		if (args.length == 0) {
			return ;
		}
		int i = 0;
		while(i < args.length){
			log.info("参数个数" + ":" + args.length);
			log.info("参数" + i + ":" + args[i]);
			String par = args[i].trim();
			if (par.startsWith("-")) {
				String key = par.substring(1).trim();
				i++ ;
				String value = null;
				if (args.length>i) {
					value = args[i].trim();
					if (value.startsWith("\"") || value.startsWith("\'")) {
						value = value.substring(1,value.length() - 1).trim();
					}
				}
				map.put(key, value);
				i++ ;
			}else {
				i++ ;
			}
			
			
			
		}
		
		
	}
	public Map<String, String> getMap() {
		return map;
	}
	
	
	
	
}
