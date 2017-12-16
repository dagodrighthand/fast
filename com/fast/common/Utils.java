package com.fast.common;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

	public static String extractDate10fromStr(String instr,String idateFormat_terminate) {
		//yyyymmddhh24
		String reg = "20\\d{2}(((0[13578]|1[02])([0-2]\\d|3[01]))|((0[469]|11)([0-2]\\d|30))|(02([01]\\d|2[0-8])))[0-9]{2}";
		
		String vinstr = instr.replace(idateFormat_terminate,"");
		Pattern pattern = Pattern.compile(reg);
		Matcher matcher = pattern.matcher(vinstr);
		String returnStr = "";
		while (matcher.find()) {
			returnStr = matcher.group();
		}
		return returnStr;
	}
	
	public static String extractHour24fromStr(String instr,String idateFormat_terminate) {
		//hh24
		String reg = "20\\d{2}(((0[13578]|1[02])([0-2]\\d|3[01]))|((0[469]|11)([0-2]\\d|30))|(02([01]\\d|2[0-8])))[0-9]{2}";
		String vinstr = instr.replace(idateFormat_terminate,"");
		Pattern pattern = Pattern.compile(reg);
		Matcher matcher = pattern.matcher(vinstr);
		String returnStr = "";
		while (matcher.find()) {
			returnStr = matcher.group();
		}
		
		if("".equalsIgnoreCase(returnStr)){
			return "";
		}
		
		String str11 = "";
		
		try{
			str11 = returnStr.substring(8, 10);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return str11;
	}
	
	public static String extractDDfromStr(String instr) {
		String reg = "20\\d{2}(((0[13578]|1[02])([0-2]\\d|3[01]))|((0[469]|11)([0-2]\\d|30))|(02([01]\\d|2[0-8])))[0-9]{2}";
		Pattern pattern = Pattern.compile(reg);
		Matcher matcher = pattern.matcher(instr);
		String returnStr = "";
		while (matcher.find()) {
			returnStr = matcher.group();
		}
		
		if("".equalsIgnoreCase(returnStr)){
			return "";
		}
		
		
		String str11 = "";
		
		try{
			str11 = returnStr.substring(6, 8);
		}catch(Exception e){
			//System.out.println(instr);
			e.printStackTrace();
		}
		
		return str11;
	}
	
	public static String extractDate8fromStr(String instr,String idateFormat_terminate) {
		String reg = "20\\d{2}(((0[13578]|1[02])([0-2]\\d|3[01]))|((0[469]|11)([0-2]\\d|30))|(02([01]\\d|2[0-8])))[0-9]{2}";
		String vinstr = instr.replace(idateFormat_terminate,"");
		Pattern pattern = Pattern.compile(reg);
		Matcher matcher = pattern.matcher(vinstr);
		String returnStr = "";
		while (matcher.find()) {
			returnStr = matcher.group();
		}
		if("".equalsIgnoreCase(returnStr)){
			return "";
		}
		
		
		String str11 = "";
		
		try{
			str11 = returnStr.substring(0, 8);
		}catch(Exception e){
			//System.out.println(instr);
			e.printStackTrace();
		}
		
		return str11;
	}
	
	public static String extractEnbidfromFileName(String iFileName,String idateFormat_terminate) {
		//LTE_MRGZ_HUAWEI_136.192.44.197_201711030315_201711030330_001
		String vDate10 = extractDate10fromStr(iFileName,idateFormat_terminate);
		String returnStr = "";
		Pattern pattern_mr_xml = Pattern.compile("_[0-9]{4,10}_"+vDate10); 
		Matcher matcher = pattern_mr_xml.matcher(iFileName); 
		while (matcher.find()) {
			returnStr = matcher.group();
		}
		
		returnStr = returnStr.replace("_"+vDate10, "").replace("_", "");
		return returnStr;
	}
	
	public static boolean IsDDBySiteLocation(String ifilename,String iSiteLocation,HashMap<String,String> iSendDayMap){
		
		if(iSendDayMap == null){ return false;}
		
		String vSendDaysMR = iSendDayMap.get(iSiteLocation);
		
		if(vSendDaysMR == null){ return false;}
		
		String vdd = Utils.extractDDfromStr(ifilename);
		
		if(vdd != null){
			if(vSendDaysMR.contains(vdd)){
				return true;
			}
		}
		return false;
	}
	
	public static void main(String[] args) {
		
		/*System.out.println (extractEnbidfromFileName("TD-LTE_MRE_NSN_OMC_234324_644566_20160426100000.xml.gz"));
		
		List<String> jss = new ArrayList<String>();
		jss.add("D:\\MR_RAW_DAY2\\GANZHOU\\NOKIA\\RAW");
		jss.add("D:\\MR_RAW_DAY2\\GANZHOU\\HUAWEI\\RAW");
		jss.add("D:\\MR_RAW_DAY2\\JIUJIANG\\ZTE\\RAW");
		List<String> lll = GetPathExistsFile(jss);
		
		for(String ds :lll){
			System.out.println(ds);
		}
		
		String sd = " TD-LTE_MRS_ZTE_OMC1_917471_20160518100000.zip";
		String str99 = "NOKLTE01_MR201609182315.tar.gz";
		if("".equalsIgnoreCase(extractEnbidfromFileName(str99))){
			
			System.out.println ("123");
		}
		System.out.println (extractDate8fromStr(sd));
		
		
		
		System.out.println (common.Utils.extractEnbidfromFileName(str99)+"000");
		*/
		String dd = "127.0.0.1:21:/oamdata/tce/lte/mr/{yyyymmdd}/,c:/MR_RAW_DAY20170223/TEMP/ALCATEL/RAW/MR1//FDD-LTE_MRE_ZTE_OMC1_74245_20170223010000.zip";
		//System.out.println(getlinuxPath(dd,"1"));
		
		String dds = "FDD-LTE_MRS_ZTE_OMC1_8511_20170802201500.zip";
		System.out.println(extractEnbidfromFileName("LTE_MRGZ_HUAWEI_136.192.44.197_201711030315_201711030330_001.tar.gz",""));
		
		//"/export/home/omc/var/fileint/TSNBI/LTESpecial/20171103/LTE_MRGZ_HUAWEI_136.192.44.197_201711030315_201711030330_001.tar.gz"
		
		
	}
	
	public static String extractCityfrominobid(String ifilename,ConcurrentHashMap<String,String> ithreadeNBIDLISTSBYCITY){
		String vinboid = extractEnbidfromFileName(ifilename,"");
		String vcity = "";
		if(!"".equalsIgnoreCase(vinboid)){
			if(ithreadeNBIDLISTSBYCITY != null)
			vcity = ithreadeNBIDLISTSBYCITY.get(vinboid);
			if(vcity == null){
				vcity = "";
			}
		}
		return vcity;
	}
	
	public static List<String> GetPathExistsFile_temp(List<String> filedirlist){
		
		List<String> reLists = new ArrayList<String>();
		//RAW
		for(String strfiledir : filedirlist){
			//System.out.println(strfiledir);
			File root_mr  = new File(strfiledir);
			if(!root_mr.exists())continue;
			File[] files_mr = root_mr.listFiles();
			for (File file1 : files_mr) {//MR1
				if(file1.isDirectory()){
					File[] files_mr2 = file1.listFiles();
					for (File file2 : files_mr2){
						if(file2.isFile()){
							reLists.add(file2.getPath().replace("\\", "/"));
						}
					}
					//}
				}
			}
		}
		return reLists;
	}
	public static void GetPathByRecursivecommonMain(Map<String,Long> allSet,Set<String> iDirset){
		for(String s:iDirset){
			System.out.println("读取目录:"+s);
			GetPathByRecursivecommon(allSet,s);
		}
	}
	
	
	public static void GetPathByRecursivecommon(Map<String,Long> allSet,String iDir){
		// System.out.println("GetPathByRecursivecommon:"+iDir);
		if(iDir.contains("{")){
			String xmlfilepath = iDir;
			String vrootdir = xmlfilepath.substring(0,xmlfilepath.indexOf("{"));
			String vhomedir = xmlfilepath.substring(xmlfilepath.indexOf("{")+1,xmlfilepath.indexOf("}"));
			Pattern pattern_mrbak = Pattern.compile(vhomedir+"\\d{8}");
			
			File f = new File(vrootdir);
			
			if(!f.exists() || f.isFile()){return;}
			
			File[] fs = f.listFiles();
			for(File f1:fs){
				
				if(f1.isFile()) continue;
				Matcher matcher = pattern_mrbak.matcher(f1.getName());
				if(!matcher.find()){
					continue;
				}
				String fname = f1.getName().replace("$", "\\$");
				String sd = xmlfilepath.replaceAll("\\{.+\\}", fname);
				File existfile = new File(sd);
				if(existfile.exists()){
					GetPathByRecursive(allSet,sd);
				}
			}
		
			
		}
		else{
			GetPathByRecursive(allSet,iDir+"/");
		}
		
	
	}
	public static void GetPathByRecursive(Map<String,Long> allSet,String iDir){
		// System.out.println("GetPathByRecursive:"+iDir);
			File f = new File(iDir);
			
			if(!f.exists()){return;}
			
			if(f.isDirectory()){
				File[] f2 = f.listFiles();
				
				for(File f3:f2){
					if(f3.isDirectory()){
						GetPathByRecursive(allSet,f3.getPath());
					}
					else if(f3.isFile()){
						allSet.put(f3.getPath().replace("\\", "/"),f3.length());
					}
				}
			}
	}
	public static void getMRFile(Map<String,String> mapfiles,String MrPath){
		File files = new File(MrPath);
		
		if(!files.exists()){return;}
		
		File[] fs = files.listFiles();
		
		for(File f:fs){
			if(f.isFile()){
				mapfiles.put(f.getAbsolutePath(),"");
			}
			else if(f.isDirectory()){
				getMRFile(mapfiles,f.getAbsolutePath());
			}
		}
	}
	
	public static void getFolder(Map<String,String> imapfiles,String iMrPath){
		File files = new File(iMrPath);
		
		if(!files.exists()){return;}
		
		File[] fs = files.listFiles();
		
		for(File f:fs){
			if(f.isDirectory()){
				String vpath = f.getAbsolutePath();
				vpath = vpath.replace("\\", "/");
				imapfiles.put(iMrPath,"");
				getMRFile(imapfiles,f.getAbsolutePath());
			}
		}
	}
	
	public static String leftPad(String str,int length,char ch){  
        char[] chs = new char[length];  
        Arrays.fill(chs, ch);//把数组chs填充成ch  
        char[] src = str.toCharArray();//把字符串转换成字符数�?  
        System.arraycopy(src, 0, chs,  
                length-src.length,src.length);  
        //从src�?0位置�?始复制到chs中从length-src.length到src.lengtth  
        //右填�?  
        return new String(chs);  

    }  
	
	public static String convertIdentity(String iip,String iport){
		
		String[] vips = iip.split("\\.");
		String gvip = "";
		for(int i = 0; i < vips.length; i++){
			String fd = vips[i];
			gvip = gvip+Utils.leftPad(fd,3,'0');
		}
		gvip = gvip + Utils.leftPad(iport,5,'0');
		return gvip;
	}

	public static String getlinuxPath(String ifilePath,String ifileType){
		//�?有文件或文件夹路�? 统一成linux路径
		//�?有文件夹的路�? 末端 不带反斜�?
		Pattern pattern_filePath = Pattern.compile("[/\\\\]{1,}");
		Matcher matcher = pattern_filePath.matcher(ifilePath);
		String vlinuxfilePath = matcher.replaceAll("/");

		if(ifileType.equalsIgnoreCase("0")){//文件�?
			if(!vlinuxfilePath.endsWith("/")){
				vlinuxfilePath = vlinuxfilePath + "/";
			}
		}
		vlinuxfilePath = vlinuxfilePath.replace("//", "/");
		return vlinuxfilePath;
	}
	public static int daysBetween(Date date1,Date date2)  
	{  
	    Calendar cal = Calendar.getInstance();  
	    cal.setTime(date1);  
	    long time1 = cal.getTimeInMillis();              
	    cal.setTime(date2);  
	    long time2 = cal.getTimeInMillis();       
	    long between_days=(time2-time1)/(1000*3600*24);  
	   return Integer.parseInt(String.valueOf(between_days));         
	}  
	
}
