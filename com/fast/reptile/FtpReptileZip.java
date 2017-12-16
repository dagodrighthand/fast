package com.fast.reptile;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.Set;

import com.fast.bean.MRServerBean;
import com.fast.common.Utils;
import com.fast.dao.DBUtils;
import com.fast.dao.FTPMONITOR_DAO;
import com.fast.dao.FTPMONITOR_DAO_Factory;
import com.fast.ftp.FtpDownload;
import com.fast.pool.ftp.PoolManger;

public class FtpReptileZip   implements Callable<String>{
	private MRServerBean dm;
	private FtpDownload ftpDlFiles;
	private Set<String> handleObjSet;
	private Date sThreadDate_start;
	private Thread current_Thread;
	private String isDone;
	
	
	public String getIsDone() {
		return isDone;
	}


	public Date getsThreadDate_start() {
		return sThreadDate_start;
	}
	
	
	public Thread getCurrent_Thread() {
		return current_Thread;
	}


	@Override
	public String call() {
		sThreadDate_start = new Date();
		current_Thread = Thread.currentThread();
		Pattern pattern_filter= Pattern.compile(dm.sFilterRegExp.toUpperCase());
		//获取ftp对象
		PoolManger.setFTPClientInfo(dm.sIP,dm.sPort,dm.sUser,dm.sPwd,dm.sFTPType,dm.sPassiveMode,300);
		ftpDlFiles = PoolManger.getFTPClientTimeout(dm.sIP,dm.sPort);
		HashMap<String,Long> ftpFilesMap = new HashMap<String,Long>();
		int failCount = 0;
		for(String remoteDirectory:handleObjSet){
			try {
				String zipFileName = remoteDirectory.substring(remoteDirectory.lastIndexOf("/")+1);
				String remoteDownLoadPath = remoteDirectory.substring(0,remoteDirectory.lastIndexOf("/"));
				
				if(zipFileName.toLowerCase().endsWith(".tar.gz") || zipFileName.toLowerCase().endsWith(".zip")){
					while(true){
						boolean exec_Status = ftpDlFiles.readBigZipMap(ftpFilesMap, zipFileName, remoteDownLoadPath,pattern_filter);
						if(exec_Status == true){
							break;
						}
						else{
							failCount ++;
							PoolManger.returnFTPClient(dm.sIP,dm.sPort, ftpDlFiles);
							ftpDlFiles = PoolManger.getFTPClientTimeout(dm.sIP,dm.sPort);
						}
						
						if(failCount > 10){
							break;
						}
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//IP 端口号 文件绝对路径 文件大小 创建时间 更新时间(可多次更新) 更新次数
		/*Set<Entry<String,Long>> ftpSet = ftpFilesMap.entrySet();
		for(Iterator<Map.Entry<String,Long>> iterator = ftpSet.iterator();iterator.hasNext();)
		{
			Map.Entry<String,Long> entry = (Map.Entry<String, Long>) iterator.next();
			String vfileNamePath = entry.getKey();
			Long fileSize = entry.getValue();
			
			System.out.println(dm.MRServerPro+"\t"+dm.sIP+"\t"+fileSize.intValue()+"\t"+ vfileNamePath);
		}*/
		System.out.println("[FtpReptileZip爬虫]"+dm.MRServerPro+","+dm.sIP+",已收集到的文件数:"+ftpFilesMap.size());
		FTPMONITOR_DAO ftpmdao = FTPMONITOR_DAO_Factory.getFTPMONITOR_DAO();
		ftpmdao.insertBatch(dm.sIP,dm.sPort,ftpFilesMap);
		ftpmdao.insertMonitorLog("[FtpReptileZip爬虫]"+dm.MRServerPro+","+dm.sIP+",已收集到的文件数:"+ftpFilesMap.size());
		//返回ftp对象
		PoolManger.returnFTPClient(dm.sIP,dm.sPort,ftpDlFiles);
		isDone = "1";
		return null;
	}
	public MRServerBean getDm() {
		return dm;
	}
	public void setDm(MRServerBean dm) {
		this.dm = dm;
	}
	public Set<String> getHandleObjSet() {
		return handleObjSet;
	}
	public void setHandleObjSet(Set<String> handleObjSet) {
		this.handleObjSet = handleObjSet;
	}
}
