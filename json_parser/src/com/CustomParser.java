package com;

import java.util.*;

import org.json.simple.parser.*;
import org.json.simple.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class CustomParser extends EvalFunc<String>{
	
	public static HashMap recursive_json(String parentKey, HashMap<String, String> outMap, JSONObject jsonInput) throws NullPointerException{
		Iterator<?> objectKeys = jsonInput.keySet().iterator();
		while (objectKeys.hasNext()){
			String key = (String)objectKeys.next();
			String resultKey; 
			if (parentKey == ""){
				 resultKey =  key;
			}else{
				resultKey = parentKey + "." + key;
			}
			if (!(jsonInput.get(key) instanceof JSONObject)){
				outMap.put(resultKey, jsonInput.get(key).toString());
			}else {
				recursive_json(resultKey, outMap, (JSONObject)jsonInput.get(key));
			}
		}
		
		//System.out.println(outMap);
		return outMap;
		
	}

	public static JSONObject jsonStringToFields(String log) throws ParseException{
		JSONParser parser = new JSONParser();
	    Object parsed = parser.parse(log);
	    JSONObject parsedJson = (JSONObject)parsed;
	    return parsedJson;
	   }
	
	public static String returnKeyValues(HashMap oMap, String[] reKeys){
		String result = "";
		Iterator allKeys = oMap.keySet().iterator();
		for (int i = 0; i < reKeys.length; i++) {
	         if (oMap.get(reKeys[i]) != null){
	        	 result = result + "," + oMap.get(reKeys[i]).toString().replace(",", "");
	         }else {
	        	 result = result + "," + "NA";
	         }
	      }
		return result;
		
		
	}
	
/*
	public static void main(String [] args) throws IOException, ParseException{
		
		BufferedReader br =  new BufferedReader(new InputStreamReader(System.in));
		
		String input = br.readLine();
		String[] reqKeys =  {"ts","e.f.v.rn","e.f.v.ri","e.f.p.cp","e.vi","e.ec","e.tp","e.as"};

		jsonStringToFields(input);
		HashMap<String,String> output = new HashMap<String,String>();
		HashMap<String,String> outString = recursive_json("", output, jsonStringToFields(input));
		System.out.println(outString);
		String resultString = returnKeyValues(outString,reqKeys);
		//return resultString;
		System.out.println(resultString.substring(1));
	
	
}
*/
	
	@Override
	public String exec(Tuple arg0) throws IOException, NullPointerException{
		// TODO Auto-generated method stub
		String input = (String) arg0.get(0);
		String[] reqKeys =  {"ts","e.f.v.rn","e.f.v.ri","e.f.p.cp","e.vi","e.ec","e.tp","e.as"};
		//return input;
		
		HashMap<String,String> output = new HashMap<String,String>();
		HashMap<String, String> outString = new HashMap<String,String>();
		try {
			outString = recursive_json("", output, jsonStringToFields(input));
		} catch (NullPointerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String resultString = returnKeyValues(outString,reqKeys);
		return resultString.substring(1); 
		
	}


	
}
