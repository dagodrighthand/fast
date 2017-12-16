package com.fast.common;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.map.LinkedMap;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*String sFileName = "/";
		if(sFileName.equalsIgnoreCase("/")){
			System.out.println(111);
		}
		
		Date sThreadDate_start = new Date();
		try {
			Thread.sleep(13*1000L);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Date scurrentDate = new Date();
		
		double vmin = (double)((scurrentDate.getTime() - sThreadDate_start.getTime())/(1000));
		System.out.println(vmin);
		if(vmin>10){
			System.out.println(vmin+"___");
		}*/
		String str = "";
		String ddf ="D:/MR_RAW_DAY2/TEST/DOWNLOADFAILNBID.DAT";
		System.out.println(ddf.substring(0, ddf.lastIndexOf("/")));
		
		List lst = getLogTEMPFileName("D:/MR_RAW_DAY2/TEST/DOWNLOADFAILNBID.DAT","TEMP");
		
		System.out.println(lst.size());
		
		String str2 ="MR.LteScEarfcn MR.LteScPci MR.LteScRSRP MR.LteScRSRQ MR.LteSceNBRxTxTimeDiff MR.LteScTadv MR.LteScPUSCHPRBNum";
		
		String[] slValues = str2.split(" ");
		LinkedMap ds2;
		TreeMap<String,Integer> tmap = new TreeMap<String,Integer>();
		Integer i = 0;
		for(String ss :slValues){
			tmap.put(ss,i);
			System.out.println(ss);
			i++;
		}
		System.out.println("------------");
		Set<Entry<String, Integer>> keset = tmap.entrySet();
		for(Iterator<Map.Entry<String,Integer>> iterator = keset.iterator();iterator.hasNext();)
		{
			Map.Entry<String,Integer> entry = (Map.Entry<String, Integer>) iterator.next();
			String xmlfilepath = entry.getKey();
			System.out.println(xmlfilepath+" "+entry.getValue());
		}
		
		Pattern pattern_mrbak = Pattern.compile("[^\\d]+");
		Matcher matcher = pattern_mrbak.matcher("225107844-38400:2:38");
		//System.out.println(matcher.groupCount());
		
		int matchcounts = 0;
		while(matcher.find()){
			matchcounts++;
		}
		String[] strs2 = new String[matchcounts];
		matcher = pattern_mrbak.matcher("225107844-38400:2:38");
		int matchNum = 0;
		while(matcher.find()){
			strs2[matchNum] = matcher.group();
			matchNum ++;
		}
		
		for(String sr:strs2){
			System.out.println(sr);
		}
		
		
		int index="".indexOf("="); 
		
		
	}
	static List<String> getLogTEMPFileName(String strLogFileNamePath, String iFilter){
    	
		   List<String> strNamesList = new ArrayList<String>();
	    	//String[] strs = strLogFileNamePath.split("/");
	    	String filePath = strLogFileNamePath.substring(0, strLogFileNamePath.lastIndexOf("/"));
	    	String fileName_simple = strLogFileNamePath.substring(strLogFileNamePath.lastIndexOf("/")+1);
	    	Pattern pattern_mrbak = Pattern.compile("^"+fileName_simple);
	    	
	    	File BAKDIRs = new File(filePath);
			File[] BAKDIR = BAKDIRs.listFiles();
			
			for(File bkmrbak:BAKDIR){
				
				if(!bkmrbak.isFile())continue;
				String dirName = bkmrbak.getName();
				System.out.println(fileName_simple+" "+dirName);
				String fileName = bkmrbak.getPath();
				Matcher matcher = pattern_mrbak.matcher(dirName);
				
				if(matcher.find() && fileName_simple.contains(iFilter)){
					strNamesList.add(fileName);
				}
			}
	    	return strNamesList;
	    }
}