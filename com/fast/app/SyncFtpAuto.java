package com.fast.app;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.fast.bean.MRServerBean;
import com.fast.bean.MRServerBeanInit;
import com.fast.dao.DBUtils;
import com.fast.dao.FTPMONITOR_DAO;
import com.fast.dao.FTPMONITOR_DAO_Factory;
import com.fast.pool.ftp.PoolManger;
import com.fast.recursor.FtpSlice;
import com.fast.reptile.FtpReptileZip;
import com.fast.scheduler.SyncFtpAutoScheduler;

public class SyncFtpAuto {
	public static ThreadPoolExecutor threadPool_Main;
	public static ThreadPoolExecutor threadPool_Reptile;
	public static String IniFileName;
	public static String thread_run_flag = "FALSE";
	private static Map<FtpSlice,Future<String>> syncFtpfutureMaps;
	private static FTPMONITOR_DAO ftpmdao;
	public static void main(String[] args)
	{
		IniFileName = args[0];
		SyncFtpAutoScheduler.init();
		//func_main();
	}
	public static void func_main() {
		threadPool_Main = new ThreadPoolExecutor(40, 100, 0, TimeUnit.MILLISECONDS,
	            new ArrayBlockingQueue<Runnable>(2000));
		threadPool_Reptile = new ThreadPoolExecutor(40, 100, 0, TimeUnit.MILLISECONDS,
	            new ArrayBlockingQueue<Runnable>(2000));
		MRServerBeanInit.initMRServerBean(IniFileName);
		syncFtpfutureMaps = new ConcurrentHashMap<FtpSlice,Future<String>>();
		//测试DB是否连通
		Connection conn = null;
		try {
			conn = DBUtils.openConnection();
			
		} catch (SQLException e1) {
			System.out.println("[SyncFtpAuto错误]DB 连接失败! 请检查连接信息!");
			e1.printStackTrace();
			thread_run_flag = "FALSE";
			return;
		}
		finally{
			try{
				DBUtils.closeConnection(conn);
			}
			catch(Exception e){}
		}
		Long begin = new Date().getTime();
		List<MRServerBean> MRServerBeanList = MRServerBeanInit.getMRServerBeanList();

		//记录监控信息
		ftpmdao = FTPMONITOR_DAO_Factory.getFTPMONITOR_DAO();
		ftpmdao.insertMonitorStart("[SyncFtpAuto信息]开启新轮次监控,配置文件读取成功!",MRServerBeanList.size(),MRServerBeanInit.getsMonitor_RANGE());
		ftpmdao.insertMonitorLog("[SyncFtpAuto信息]开启新轮次监控,配置文件读取成功!");
		FtpSlice.setReptile_threadPool(threadPool_Reptile);
		System.out.println("[SyncFtpAuto信息]"+new Date()+" 共要分析的FTP数量为: "+MRServerBeanList.size());
		
		//初始化FtpSlice futureMaps
		FtpSlice.setFutureMapsNulls();
		
		for(MRServerBean msb: MRServerBeanList){
			FtpSlice fslice = new FtpSlice();
			fslice.setMsb(msb);
			Future<String> future = threadPool_Main.submit(fslice);
			syncFtpfutureMaps.put(fslice, future);
			
			while(threadPool_Main.getQueue().size() > 100){
				try {
					ftpmdao.insertMonitorLog("[SyncFtpAuto信息]"+"等待线程队列处理,[Process FtpSlice] ThreadActiveCount:"+threadPool_Main.getActiveCount()+" QueueThread:"+threadPool_Main.getQueue().size());
					//future.get();
					getSyncFtpFutureStatus();
					Thread.sleep(30*1000L);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		threadPool_Main.shutdown();
				
		while(true){
			if((threadPool_Main.getActiveCount() !=0 || threadPool_Main.getQueue().size() !=0)){
				System.out.println("[SyncFtpAuto信息]"+"******[Process FtpSlice] ThreadActiveCount:"+threadPool_Main.getActiveCount()+" QueueThread:"+threadPool_Main.getQueue().size());
				try {
					Long end = new Date().getTime();
					ftpmdao.insertMonitorStatus("[SyncFtpAuto信息]"+"正在处理FtpSlice,已耗时:"+ (end - begin) / 1000 + " s",MRServerBeanInit.getsMonitor_RANGE());
					ftpmdao.insertMonitorLog("[SyncFtpAuto信息]"+"正在处理FtpSlice,已耗时:"+ (end - begin) / 1000 + " s");
					Thread.sleep(60*1000);
					/*if(threadPool_Reptile.getActiveCount() !=0){
						break;
					}*/
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				//定时迭代future状态
				getSyncFtpFutureStatus();
			}else
			{
				break;
			}
		}
		syncFtpfutureMaps.clear();
		threadPool_Reptile.shutdown();
		
		while(true){
			if((threadPool_Reptile.getActiveCount() !=0 || threadPool_Reptile.getQueue().size() !=0)){
				System.out.println("[Reptile信息]"+"******[Process Reptile] ThreadActiveCount:"+threadPool_Reptile.getActiveCount()+" QueueThread:"+threadPool_Reptile.getQueue().size());
				ftpmdao.insertMonitorLog("[Reptile信息]"+"******[Process Reptile] ThreadActiveCount:"+threadPool_Reptile.getActiveCount()+" QueueThread:"+threadPool_Reptile.getQueue().size());
				try {
					Thread.sleep(30*1000);
					Long end = new Date().getTime();
					ftpmdao.insertMonitorStatus("[Reptile信息]"+"爬虫正在递归FTP文件系统,已耗时:"+ (end - begin) / 1000 + " s",MRServerBeanInit.getsMonitor_RANGE());
					ftpmdao.insertMonitorLog("[Reptile信息]"+"爬虫正在递归FTP文件系统,已耗时:"+ (end - begin) / 1000 + " s");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				//定时迭代future状态
				getReptileFutureStatus();
			}
			else{
				break;
			}
		}
		FtpSlice.setFutureMapsNulls();
		
		PoolManger.destroyAllObject();
		Long end = new Date().getTime();
		System.out.println("[SyncFtpAuto信息]"+new Date()+" ***本轮次信息搜索成功完成***");
		ftpmdao.insertMonitorStatus("[SyncFtpAuto信息]"+"本轮次完成收集FTP文件系统,耗时:" + (end - begin) / 1000 + " s",MRServerBeanInit.getsMonitor_RANGE());
		ftpmdao.insertMonitorLog("[SyncFtpAuto信息]"+"本轮次完成收集FTP文件系统,耗时:" + (end - begin) / 1000 + " s");
		thread_run_flag = "FALSE";
	}
	
	public static void getSyncFtpFutureStatus(){
		Set<Entry<FtpSlice, Future<String>>> set = syncFtpfutureMaps.entrySet();
		long currentDateLong = new Date().getTime();
		
		for(Entry<FtpSlice, Future<String>> ef : set){
			FtpSlice ftpslice = ef.getKey();
			Future<String>  future = ef.getValue();
			if(ftpslice.getsThreadDate_start() == null)continue;
			double vmin = (double)((currentDateLong - ftpslice.getsThreadDate_start().getTime())/(60*1000));
			ftpmdao.insertMonitorLog("[SyncFtpAuto信息]IP:" + ftpslice.dm.getsIP()
					+" 线程:"+ftpslice.getCurrent_Thread().getName()+" 已运行分钟:"+vmin);
			//超时2hour则结束任务
			if(vmin > 60 && !future.isDone()){
				boolean cancelStatus = future.cancel(true);
				boolean interrupStatus = ftpslice.getCurrent_Thread().interrupted();
				ftpmdao.insertMonitorLog("[SyncFtpAuto信息]"+ftpslice.dm.getsIP()+" 线程:"+ftpslice.getCurrent_Thread().getName()
						+",递归搜索文件时:"+"超过60min,任务cancel状态:"+cancelStatus
						+",任务interrupte状态:"+interrupStatus);
			}
			//清理过期的future
			if(future.isDone() && "1".equalsIgnoreCase(ftpslice.getIsDone())){
				syncFtpfutureMaps.remove(ftpslice);
			}
		}
	}
	
	public static void getReptileFutureStatus(){
		Set<Entry<FtpReptileZip, Future<String>>> set = FtpSlice.getFutureMaps().entrySet();
		long currentDateLong = new Date().getTime();
		
		for(Entry<FtpReptileZip, Future<String>> ef : set){
			FtpReptileZip ftprep = ef.getKey();
			Future<String>  future = ef.getValue();
			if(ftprep.getsThreadDate_start() == null)continue;
			double vmin = (double)((currentDateLong - ftprep.getsThreadDate_start().getTime())/(60*1000));
			ftpmdao.insertMonitorLog("[Reptile信息]IP:" + ftprep.getDm().getsIP()+" 线程:"+ftprep.getCurrent_Thread().getName()+" 已运行分钟:"+vmin);
			//超时2hour则结束任务
			if(vmin > 80 && !future.isDone()){
				boolean cancelStatus = future.cancel(true);
				boolean interrupStatus = ftprep.getCurrent_Thread().interrupted();
				ftpmdao.insertMonitorLog("[Reptile信息]"+ftprep.getDm().getsIP()
						+",线程:"+ftprep.getCurrent_Thread().getName()+",处理文件数:"+ftprep.getHandleObjSet().size()
						+"超过80min,任务cancel状态:"+cancelStatus
						+",任务interrupte状态:"+interrupStatus);
			}
			//清理过期的future
			if(future.isDone()&& "1".equalsIgnoreCase(ftprep.getIsDone())){
				FtpSlice.getFutureMaps().remove(ftprep);
			}
		}
	}
}
