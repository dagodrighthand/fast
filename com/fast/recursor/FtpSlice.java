package com.fast.recursor;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fast.bean.MRServerBean;
import com.fast.common.Utils;
import com.fast.dao.FTPMONITOR_DAO;
import com.fast.dao.FTPMONITOR_DAO_Factory;
import com.fast.ftp.FtpDownload;
import com.fast.pool.ftp.PoolManger;
import com.fast.reptile.FtpReptileZip;

public class FtpSlice  implements Callable<String>{
	public MRServerBean dm;
	private FtpDownload ftpDlFiles;
	private Thread current_Thread;
	public static ThreadPoolExecutor reptile_threadPool;
	private Date sThreadDate_start;
	private String isDone;
	DateFormat dfMRLog8 = new SimpleDateFormat("yyyyMMdd");
	DateFormat dfMRLog10 = new SimpleDateFormat("yyyy-MM-dd");
	
	private HashSet<String> ftpZipFilesSet = new HashSet<String>();
	private static Map<FtpReptileZip,Future<String>> futureMaps= new ConcurrentHashMap<FtpReptileZip,Future<String>>();
	public Date getsThreadDate_start() {
		return sThreadDate_start;
	}
	public String getIsDone() {
		return isDone;
	}
	public static Map<FtpReptileZip, Future<String>> getFutureMaps() {
		return futureMaps;
	}
	
	public static void setFutureMapsNulls() {
		futureMaps.clear();
	}

	public Thread getCurrent_Thread() {
		return current_Thread;
	}

	//连上Ftp 切片,按目录或者大�?
	//确认大包条件:不带基站�? 指定文件字符 带有tar.gz zip 后缀
	@Override
	public String call() {
		sThreadDate_start = new Date();
		current_Thread = Thread.currentThread();
		Pattern pattern_filter= Pattern.compile(dm.sFilterRegExp.toUpperCase());
		
		Calendar calendar_Sys = Calendar.getInstance();
		calendar_Sys.setTime(new Date());
		
		dfMRLog8 = new SimpleDateFormat("yyyyMMdd");
		//获取ftp对象
		PoolManger.setFTPClientInfo(dm.sIP,dm.sPort,dm.sUser,dm.sPwd,dm.sFTPType,dm.sPassiveMode,300);
		ftpDlFiles = PoolManger.getFTPClientTimeout(dm.sIP,dm.sPort);
		
		//过滤日期范围 监控指定区间的数�?
		String[] Dates = dm.sMonitor_RANGE.split("-");
		String startDate = Dates[0];
		String endDate = Dates[1];
		
		int date_between_day = 0;
		try {
			date_between_day = Utils.daysBetween(dfMRLog8.parse(startDate),dfMRLog8.parse(endDate));
		} catch (Exception e) {
			/*System.out.println(startDate+"\t"+endDate);
			return;*/
			e.printStackTrace();
		}
		Calendar calLogEnd = Calendar.getInstance();
		try {
			calLogEnd.setTime(dfMRLog8.parse(endDate));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//源路径中含有yyyymmdd�?
		String vremoteDirectory = "";
		//倒着日期统计
		for(int i = 1; i <= date_between_day+1; i++){
			ftpZipFilesSet.clear();
			//时间晚于当前日期 则�??出循�?
			if(calLogEnd.getTime().getTime() > calendar_Sys.getTime().getTime()) {
				calLogEnd.add(Calendar.DAY_OF_YEAR, -1);
				continue;
			}
			String vworkdate = dfMRLog8.format(calLogEnd.getTime());
			String vworkdate10 = dfMRLog10.format(calLogEnd.getTime());
			//转换 sSrcPath
			if(dm.sSrcPath.contains("{yyyy")){
				if(dm.sSrcPath.contains("{yyyymmdd}")){
					vremoteDirectory = dm.sSrcPath.replace("{yyyymmdd}", vworkdate);
				}
				else if(dm.sSrcPath.contains("{yyyy-mm-dd}")){
					vremoteDirectory = dm.sSrcPath.replace("{yyyy-mm-dd}", vworkdate10);
				}
			}
			else{
				vremoteDirectory = dm.sSrcPath;
			}
			//System.out.println(vremoteDirectory);
			HashMap<String,Long> FilePathMap = new HashMap<String,Long>();
			//递归获得�?有文�?
			try {
				ftpDlFiles.ListFiles_Recursive(FilePathMap,vremoteDirectory,vworkdate);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			if(FilePathMap.size() == 0 ){
				calLogEnd.add(Calendar.DAY_OF_YEAR, -1);
				System.out.println("[FtpSlice警告]请核�?:"+dm.MRServerPro+","+dm.sIP+","+vworkdate+" 的SrcPath递归后无文件!");
				continue;
			}
			else{
				//System.out.println(dm.MRServerPro+"\t"+dm.sIP+"\t"+vworkdate+"\t"+FilePathMap.size());
				System.out.println("[FtpSlice统计信息]:"+vworkdate+","+dm.sIP+","+vremoteDirectory+",递归后文件数:"+FilePathMap.size());
			}
			
			//非大包集�?
			HashMap<String,Long> ftpNoZipFilesMap = new HashMap<String,Long>();
			//大包集合
			
			Set<Entry<String,Long>> ftpSet = FilePathMap.entrySet();
			for(Iterator<Map.Entry<String,Long>> iterator = ftpSet.iterator();iterator.hasNext();)
			{	
				Map.Entry<String,Long> entry = (Map.Entry<String, Long>) iterator.next();
				String filePath = entry.getKey();
				Long fileSize = entry.getValue();
				String FileName = filePath.substring(filePath.lastIndexOf("/")+1);
				String FileDir = filePath.substring(0,filePath.lastIndexOf("/"));
				
				//�?查包是否为大�?
        		String vEnbid = Utils.extractEnbidfromFileName(FileName,"");
        		if("".equalsIgnoreCase(vEnbid)){
        			//不带基站号的大包就解压缩
        			if(FileName.toLowerCase().endsWith(".tar.gz") || FileName.toLowerCase().endsWith(".zip")){
        				ftpZipFilesSet.add(filePath);
        			}
        			else{
        			//不带基站号的非大�? 加入map,供后续merge到DB
        				//�? FilterRegExp 过滤
        				Matcher matcher = pattern_filter.matcher(FileName.toUpperCase());
                    	if (!matcher.find()){
                    		continue;
                    	}
        				ftpNoZipFilesMap.put(filePath,fileSize);
        			}
        		}
        		else{
        			//加入map,供后续merge到DB
        			//�? FilterRegExp 过滤
    				Matcher matcher = pattern_filter.matcher(FileName.toUpperCase());
                	if (!matcher.find()){
                		continue;
                	}
        			ftpNoZipFilesMap.put(filePath,fileSize);
        		}
			}
			
			System.out.println("[FtpSlice统计信息]:"+vworkdate+","+dm.sIP+","+vremoteDirectory+",待解压的文件�?:"+ftpZipFilesSet.size()+",非待解压的文件数(过滤FilterRegExp "+dm.sFilterRegExp+"):"+ftpNoZipFilesMap.size());
			
			//1. 更新DB 状�??
			FTPMONITOR_DAO ftpmdao = FTPMONITOR_DAO_Factory.getFTPMONITOR_DAO();
			
			//ftp 若在DB中存�? 则更新update_count
			if(ftpZipFilesSet.size() != 0){
				ftpmdao.updateStatus(vworkdate,dm.sIP,dm.sPort,ftpZipFilesSet);
			}
			//merge到DB
			Set<String> stdb = null;
			if(ftpNoZipFilesMap.size() != 0){
				ftpmdao.insertBatch(dm.sIP,dm.sPort,ftpNoZipFilesMap);
			}
			
			//将不带基站号的大包加入线程池读取压缩文件
			//获取DB中的ZIP列表
			try {
				stdb = ftpmdao.queryByIpDate(dm.sIP,dm.sPort,vworkdate,"1");
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			//ftp 若不在DB�?,则读取解压缩信息
			//过滤已经读取过的大包 此处为了不重复对大包读取,提高效率
			for(String fpath:stdb){
				ftpZipFilesSet.remove(fpath);
			}
			
			System.out.println("[FtpSlice统计信息]:"+vworkdate+","+dm.sIP+","+vremoteDirectory+",待解压的文件�?(经过DB过滤�?):"+ftpZipFilesSet.size());
			ftpmdao.insertMonitorLog("[FtpSlice统计信息]:"+vworkdate+","+dm.sIP+","+vremoteDirectory+",待解压的文件�?(经过DB过滤�?):"+ftpZipFilesSet.size());
			if(ftpZipFilesSet.size() == 0){
				calLogEnd.add(Calendar.DAY_OF_YEAR, -1);
				continue;
			}
			//切片大包到爬虫器
			Set<String> hsfs = null;
			int objcount = 0;
			int objCount_Limit = 20;
			
			for(String fs:ftpZipFilesSet){
				if(hsfs == null){
					hsfs = new HashSet<String>();
				}
				hsfs.add(fs);
				objcount ++;
				
				if(objcount > objCount_Limit){
					FtpReptileZip freptile = new FtpReptileZip();
					freptile.setHandleObjSet(hsfs);
					freptile.setDm(dm);
					objcount = 0;
					//加入到线程池
					System.out.println("[FtpSlice信息]任务已加入爬虫线程池 "+vworkdate+","+dm.sIP+","+vremoteDirectory+ ", 处理的大包数:"+hsfs.size());
					ftpmdao.insertMonitorLog("[FtpSlice信息]任务已加入爬虫线程池 "+vworkdate+","+dm.sIP+","+vremoteDirectory+ ", 处理的大包数:"+hsfs.size());
					Future<String> future = reptile_threadPool.submit(freptile);
					futureMaps.put(freptile, future);
					hsfs = null;
					while(reptile_threadPool.getQueue().size() > 200){
						try {
							ftpmdao.insertMonitorLog("[FtpSlice信息]"+"等待线程队列处理,[Process FtpSlice] ThreadActiveCount:"+reptile_threadPool.getActiveCount()+" QueueThread:"+reptile_threadPool.getQueue().size());
							//future.get();
							Thread.sleep(30*1000L);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
			
			if(objcount > 0){
				FtpReptileZip freptile = new FtpReptileZip();
				freptile.setHandleObjSet(hsfs);
				freptile.setDm(dm);
				objcount = 0;
				//加入到线程池
				System.out.println("[FtpSlice信息]任务已加入爬虫线程池 "+vworkdate+","+dm.sIP+","+vremoteDirectory+ ", 处理的大包数:"+hsfs.size());
				ftpmdao.insertMonitorLog("[FtpSlice信息]任务已加入爬虫线程池 "+vworkdate+","+dm.sIP+","+vremoteDirectory+ ", 处理的大包数:"+hsfs.size());
				Future<String> future = reptile_threadPool.submit(freptile);
				futureMaps.put(freptile, future);
				
				while(reptile_threadPool.getQueue().size() > 200){
					try {
						ftpmdao.insertMonitorLog("[FtpSlice信息]"+"等待线程队列处理,[Process FtpSlice] ThreadActiveCount:"+reptile_threadPool.getActiveCount()+" QueueThread:"+reptile_threadPool.getQueue().size());
						//future.get();
						Thread.sleep(30*1000L);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
			calLogEnd.add(Calendar.DAY_OF_YEAR, - 1);
		}
		
		//返回ftp对象
		PoolManger.returnFTPClient(dm.sIP,dm.sPort, ftpDlFiles);
		isDone = "1";
		return null;
	}
	public void setMsb(MRServerBean msb) {
		this.dm = msb;
	}
	
	public static void setReptile_threadPool(ThreadPoolExecutor reptile_threadPool) {
		FtpSlice.reptile_threadPool = reptile_threadPool;
	}
	
	public static void main(String[] args) throws ParseException {
		/*String startDate = "20171001";
		String endDate = "20171111";
		int date_between_day = Utils.daysBetween(dfMRLog.parse(startDate),dfMRLog.parse(endDate));
		Calendar calLogstart = Calendar.getInstance();
		calLogstart.setTime(dfMRLog.parse(startDate));
		
		for(int i = 1; i <= date_between_day+1; i++){
			String vworkdate = dfMRLog.format(calLogstart.getTime());
			System.out.println(vworkdate);
			calLogstart.add(Calendar.DAY_OF_YEAR, 1);
		}*/
	}
	
	
}
