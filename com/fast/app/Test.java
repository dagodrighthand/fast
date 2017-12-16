package com.fast.app;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fast.common.Utils;
import com.fast.reptile.FtpCollecter;

public class Test {
	static DateFormat dfMRLog8 = new SimpleDateFormat("yyyyMMdd");
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		int date_between_day;
		try {
			date_between_day = Utils.daysBetween(dfMRLog8.parse("20171101"),dfMRLog8.parse("20171130"));
			System.out.println(date_between_day);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Pattern pattern_filter= Pattern.compile("11".toUpperCase());
		Matcher matcher = pattern_filter.matcher("DASFSDFD".toUpperCase());
    	if (matcher.find()){
    		System.out.println(112);
    	}
    	Map<Test,String> futureMaps = new ConcurrentHashMap<Test,String>();
    	Test t1 = new Test();
    	futureMaps.put(t1, "");
    	t1 = new Test();
    	futureMaps.put(t1, "");
    	t1 = new Test();
    	futureMaps.put(t1, "");
    	
    	System.out.println(futureMaps.size());
	}

}
