package com.fast.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.fast.bean.*;
import com.fast.common.*;
import com.fast.dao.DBUtils;
public class MRServerBeanInit {
	
	public static List<MRServerBean> MRServerBeanList = new ArrayList<MRServerBean>();
	public static Map<String,MRServerBean> MRServerBeanMap = new HashMap<String,MRServerBean>();
	private static String sMonitor_RANGE;
	
    public static List<MRServerBean> getMRServerBeanList() {
		return MRServerBeanList;
	}
    
    
	public static String getsMonitor_RANGE() {
		return sMonitor_RANGE;
	}


	public static Map<String, MRServerBean> getMRServerBeanMap() {
		return MRServerBeanMap;
	}

	public static void initMRServerBean(String iIniFileName){
		MRServerBeanList.clear();
		MRServerBeanMap.clear();
		IniHash.hashINI(iIniFileName);
		//System.out.println("iniHash:"+iniHash.size());
    	int idxMRServer = 0;
    	MRServerBean mrsb;
    	String sMRServer;
    	String sIP, sUser, sPwd,sPassiveMode, sSrcPath, sSrcPath_Base, sLocalPath
    			, sServerIPPort,sFTPType,sFilterRegExp;
    	int sPort,sFolderLevel;
    	
    	String sDBIP = IniHash.getHashValue("Common", "DBIP", "");
    	String sDBPORT = IniHash.getHashValue("Common", "DBPORT", "");
    	String sDBNAME = IniHash.getHashValue("Common", "DBNAME", "");
    	String sDBUSER = IniHash.getHashValue("Common", "DBUSER", "");
    	String sDBPASSWORD = IniHash.getHashValue("Common", "DBPASSWORD", "");

    	//初始化公有参数
    	sMonitor_RANGE = IniHash.getHashValue("Common", "MONITOR_RANGE", "");
    	
    	//初始化数据库配置
    	DBUtils.connParaset(sDBIP,sDBPORT,sDBNAME,sDBUSER,sDBPASSWORD);
		
		while(idxMRServer<=5000){
			sMRServer = IniHash.getHashValue("MRServer", String.valueOf(idxMRServer+1), "");
			if (sMRServer.length() == 0) {
				idxMRServer++;
				continue;
			}
			sIP = IniHash.getHashValue(sMRServer, "IP", "");
			sPort = Integer.valueOf(IniHash.getHashValue(sMRServer, "Port", "21"));
			sServerIPPort = sIP+":"+String.valueOf(sPort);
			sUser = IniHash.getHashValue(sMRServer, "User", "");
			sPwd = IniHash.getHashValue(sMRServer, "Pwd", "");
			sSrcPath = IniHash.getHashValue(sMRServer, "SrcPath", "");
			sLocalPath = IniHash.getHashValue(sMRServer, "LocalPath", "");
			sFTPType = IniHash.getHashValue(sMRServer, "FTPType", "");
			sSrcPath_Base = IniHash.getHashValue(sMRServer, "SrcPath", "");
			sSrcPath_Base = Utils.getlinuxPath(sSrcPath_Base,"0");
			sPassiveMode = IniHash.getHashValue(sMRServer, "SPassiveMode", "TRUE");
			sFilterRegExp = IniHash.getHashValue(sMRServer, "FilterRegExp", "");
			//大包标识，若为大包，读取文件列表时需要解压缩
			//sIsBigZip = IniHash.getHashValue(sMRServer, "IsBigZip", "0");
			sFolderLevel = Integer.valueOf(IniHash.getHashValue(sMRServer, "FolderLevel", "0"));
			mrsb = new MRServerBean(sMRServer);
			mrsb.setsFilterRegExp(sFilterRegExp);
			mrsb.setsFTPType(sFTPType);
			mrsb.setsIP(sIP);
			mrsb.setsLocalPath(sLocalPath);
			mrsb.setsPassiveMode(sPassiveMode);
			mrsb.setsPort(sPort);
			mrsb.setsPwd(sPwd);
			mrsb.setsServerIPPort(sServerIPPort);
			mrsb.setsSrcPath(sSrcPath);
			mrsb.setsSrcPath_Base(sSrcPath_Base);
			mrsb.setsUser(sUser);
			//mrsb.setsIsBigZip(sIsBigZip);
			mrsb.setsFolderLevel(sFolderLevel);
			mrsb.setsMonitor_RANGE(sMonitor_RANGE);
			MRServerBeanList.add(mrsb);
			MRServerBeanMap.put(sServerIPPort, mrsb);
			idxMRServer ++;
		}
    }
}
