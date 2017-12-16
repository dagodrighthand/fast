package com.fast.reptile;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import com.fast.bean.MRServerBean;
import com.fast.common.Utils;
import com.fast.dao.FTPCollect_DAO;
import com.fast.dao.FTPCollect_DAO_Factory;
import com.fast.dao.FTPMONITOR_DAO;
import com.fast.dao.FTPMONITOR_DAO_Factory;
import com.fast.ftp.FtpDownload;
import com.fast.pool.ftp.PoolManger;
import com.jcraft.jsch.SftpException;

public class FtpCollecter   implements Callable<String>{
	private String sround;
	private MRServerBean dm;
	private FtpDownload ftpDlFiles;
	private Map<String,Set<String>> handleObjMap;
	private static volatile Integer Collect_FileSum = 0;
	private Date sThreadDate_start;
	private Thread current_Thread;
	private int current_downloadFiles;
	private String isDone;
	
	public static void reSetCollect_FileSum(){
		Collect_FileSum = 0;
	}
	
	public String getIsDone() {
		return isDone;
	}
	public int getCurrent_downloadFiles() {
		return current_downloadFiles;
	}
	public Map<String,String> enbidMap;
	
	public Thread getCurrent_Thread() {
		return current_Thread;
	}
	public void setEnbidMap(Map<String, String> enbidMap) {
		this.enbidMap = enbidMap;
	}
	public Date getsThreadDate_start() {
		return sThreadDate_start;
	}
	
	private synchronized void setCollect_FileCount(int icollect_FileSum){
		int before = 0;
		if(Collect_FileSum == null){
			Collect_FileSum = new Integer(icollect_FileSum);
		}
		else{
			before = Collect_FileSum.intValue();
			Collect_FileSum = Collect_FileSum.intValue() + icollect_FileSum;
		}
		//System.out.println("改之前的:"+before+" \t 改之后的:"+Collect_FileSum.intValue());
	}
	
	public static Integer getCollect_FileSum() {
		return Collect_FileSum;
	}

	@Override
	public String call() throws InterruptedException {
		
		sThreadDate_start = new Date();
		current_Thread = Thread.currentThread();
		System.out.println("[FtpCollecter]线程 "+Thread.currentThread().getName()+" 处理开始,"+dm.MRServerPro+","+dm.sIP+",分配待处理的文件数:"+handleObjMap.size());
		//获取ftp对象
		PoolManger.setFTPClientInfo(dm.sIP,dm.sPort,dm.sUser,dm.sPwd,dm.sFTPType,dm.sPassiveMode,300);
		ftpDlFiles = PoolManger.getFTPClientTimeout(dm.sIP,dm.sPort);
		HashMap<String,Integer> ftpFilesMap = new HashMap<String,Integer>();
		
		Map<String,Integer> failmaps = new ConcurrentHashMap<String,Integer>();
		//handleMap.put(filePathBase+","+fileName+","+isTar,"");
		Set<Entry<String,Set<String>>> handleSet = handleObjMap.entrySet();
		for(Iterator<Map.Entry<String,Set<String>>> iterator = handleSet.iterator();iterator.hasNext();)
		{
			Map.Entry<String,Set<String>> entry = (Map.Entry<String, Set<String>>) iterator.next();
			String vKey = entry.getKey();
			failmaps.put(vKey,0);
			//System.out.println("vKey:"+vKey);
		}
		
		//成功落地的文件数
	
		//--------------
		while(true){
			Set<Entry<String,Integer>> lhmpset = failmaps.entrySet();
			for(Iterator<Map.Entry<String,Integer>> iterator = lhmpset.iterator();iterator.hasNext();)
			{
				Map.Entry<String,Integer> entry = (Map.Entry<String, Integer>) iterator.next();
				String recordsStr = entry.getKey();
				String[] records = recordsStr.split(",");
				String filePathBase = records[0];
				String isTar = records[1];
				String remoteFileName1 = filePathBase.substring(filePathBase.lastIndexOf("/")+1);
				String remoteFileDir =null;
				try{
					remoteFileDir = filePathBase.substring(0,filePathBase.lastIndexOf("/"));
				}catch(Exception e){
					//System.out.println("filePathBase:"+recordsStr);
					e.printStackTrace();
				}
				int execCount = entry.getValue().intValue();
				
				String sDate = Utils.extractDate8fromStr(remoteFileName1, "");
				String vcurrlocalpath = dm.sLocalPath.replace("{RAW_DAY}", "RAW_DAY"+sDate);
				
				//不等于-1表示未执行或未成功
				if(execCount != -1){
					
					String issucess = null;
					Set<String> fileNameSet = null;
					if("1".equalsIgnoreCase(isTar)){
						try {
							fileNameSet = handleObjMap.get(recordsStr);
							issucess = ftpDlFiles.downloadFile(remoteFileName1,fileNameSet,remoteFileDir,vcurrlocalpath,enbidMap);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (SftpException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						if("-1".equalsIgnoreCase(issucess)){
							for(String fn:fileNameSet){
								ftpFilesMap.put(filePathBase+","+fn,-1);
							}
							
							failmaps.remove(recordsStr);
							failmaps.put(recordsStr, -1);
							current_downloadFiles = current_downloadFiles + fileNameSet.size();
						}
						/*else if("-2".equalsIgnoreCase(issucess)){
							for(String fn:fileNameSet){
								ftpFilesMap.put(filePathBase+","+fn,-2);
							}
							
							failmaps.remove(recordsStr);
							failmaps.put(recordsStr, -1);
						}*/
						else {
							int failCount = failmaps.get(recordsStr).intValue();
							
							if(failCount > 4){
								failmaps.remove(recordsStr);
								failmaps.put(recordsStr, -1);
								
								for(String fn:fileNameSet){
									ftpFilesMap.put(filePathBase+","+fn, failCount);
								}
							}
							else{
								failmaps.remove(recordsStr);
								failmaps.put(recordsStr, failCount + 1);
							}
							
						}
					}
					else{
						String enbid = Utils.extractEnbidfromFileName(remoteFileName1, "");
						String city = enbidMap.get(enbid);
						if(city == null || "".equalsIgnoreCase(city)){city = "TEMP";}
						vcurrlocalpath = vcurrlocalpath.replace("{CITY}", city);
						
					    issucess = ftpDlFiles.downloadFile(remoteFileName1,vcurrlocalpath, remoteFileDir);
					    
					    if("-1".equalsIgnoreCase(issucess)){
					    	ftpFilesMap.put(filePathBase+","+remoteFileName1,-1);
							
							failmaps.remove(recordsStr);
							failmaps.put(recordsStr, -1);
							current_downloadFiles ++;
						}
						/*else if("-2".equalsIgnoreCase(issucess)){
							ftpFilesMap.put(filePathBase+","+remoteFileName1,-2);
							
							failmaps.remove(recordsStr);
							failmaps.put(recordsStr, -1);
						}*/
						else {
							int failCount = failmaps.get(recordsStr).intValue();
							
							if(failCount > 4){
								failmaps.remove(recordsStr);
								failmaps.put(recordsStr, -1);
								
								ftpFilesMap.put(filePathBase+","+remoteFileName1,failCount);
							}
							else{
								failmaps.remove(recordsStr);
								failmaps.put(recordsStr, failCount + 1);
							}
							
						}
					}
					if(!"-1".equalsIgnoreCase(issucess) && !"-2".equalsIgnoreCase(issucess)){
						PoolManger.returnFTPClient(dm.sIP,dm.sPort, ftpDlFiles);
						ftpDlFiles = PoolManger.getFTPClientTimeout(dm.sIP,dm.sPort);
					}
				}
			}
			
			lhmpset = failmaps.entrySet();
			int exitCode = 1;
			for(Iterator<Map.Entry<String,Integer>> iterator = lhmpset.iterator();iterator.hasNext();)
			{
				Map.Entry<String,Integer> entry = (Map.Entry<String, Integer>) iterator.next();
				int execCount = entry.getValue().intValue();
				if(execCount != -1){
					exitCode = 0;
				}
			}
			if(exitCode == 1){
				break;
			}
		}
		
		if(ftpFilesMap.size() != 0){
			FTPCollect_DAO ftpcdao = FTPCollect_DAO_Factory.getFTPCollect_DAO();
			ftpcdao.insertBatch(sround,dm.sIP,dm.sPort,ftpFilesMap);
		}
		//----------
		System.out.println("[FtpCollecter]线程 "+Thread.currentThread().getName()+" 处理完成,"+dm.MRServerPro+","+dm.sIP+",分配待处理的文件数:"+handleObjMap.size()+",成功采集个数:"+current_downloadFiles);
		//FTPMONITOR_DAO ftpmdao = FTPMONITOR_DAO_Factory.getFTPMONITOR_DAO();
		//ftpmdao.insertBatch(dm.sIP,dm.sPort, ftpFilesMap);
		
		//返回ftp对象
		PoolManger.returnFTPClient(dm.sIP,dm.sPort, ftpDlFiles);
		
		//加入到成功落地文件总数中
		setCollect_FileCount(current_downloadFiles);
		
		isDone = "1";
		return null;
	}
	public MRServerBean getDm() {
		return dm;
	}
	public void setDm(MRServerBean dm) {
		this.dm = dm;
	}
	
	public Map<String, Set<String>> getHandleObjMap() {
		return handleObjMap;
	}
	public void setHandleObjMap(Map<String, Set<String>> handleObjMap) {
		this.handleObjMap = handleObjMap;
	}
	public void setSround(String sround) {
		this.sround = sround;
	}

}
