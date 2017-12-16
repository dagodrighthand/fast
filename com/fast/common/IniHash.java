package com.fast.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Hashtable;


public class IniHash {

	public static Hashtable<String, String> iniHash;
	//private String IniFileName;
	
	/*public IniHash(String iIniFileName){
		this.IniFileName = iIniFileName;
		hashINI();
	}*/
	public static String getHashValue(String Section,String Key, String Default)
	{  
		/*if(isEmpty()) *///hashINI();
		if(isEmpty()) return "";
		String hashKey=Section+"."+Key;
		hashKey = hashKey.toUpperCase();
		String Value=(String)iniHash.get(hashKey);
		if (Value==null) 
			Value = Default;
		return Value;
	}
	
	public static void hashINI(String iIniFileName)
	{
		String Section="",Key="",Value="";
		if(iniHash==null) iniHash=new Hashtable<String, String>();
		if(!isEmpty()) iniHash.clear();
		try
		{
			BufferedReader bufReader = new BufferedReader(new FileReader(iIniFileName));
			String strLine="";
			int firstLeftSquareBrackets=0,firstRightSquareBrackets=0;
			while( (strLine=bufReader.readLine())!=null)
			{
				strLine=strLine.trim();
				firstLeftSquareBrackets=strLine.indexOf("[");
				firstRightSquareBrackets=strLine.indexOf("]");   
				if(firstLeftSquareBrackets>=0 && firstRightSquareBrackets>=0 && firstLeftSquareBrackets<firstRightSquareBrackets)
				{
					
					Section=strLine.substring(firstLeftSquareBrackets+1,firstRightSquareBrackets);
				}
				else
				{
					int index=0;
					index=strLine.indexOf("=");
					if(index == -1){ continue;}
					Key=strLine.substring(0,index).trim();
					Value=strLine.substring(index+1).trim();   
					String hashKey="";
					hashKey=Section+"."+Key;
					hashKey = hashKey.toUpperCase();
					iniHash.put(hashKey,Value);
				}
			}
			bufReader.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/*public void reload(){
		hashINI();
	}*/
	public static  boolean isEmpty()
	{
		if(iniHash==null) return true;
		try { return iniHash.isEmpty(); }
		catch(Exception e) { 
			return true; 
		}
	}
	
	public static void main(String[] args) {
		hashINI("E:/gongdaoping/workspace/dispatcherthreadmain_new/SHMMCC_MR_ZTE.ini");
		
	}
	
}
