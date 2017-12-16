package com.fast.app;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.fast.bean.Collect_CustomBean;
import com.fast.bean.MRServerBean;
import com.fast.bean.MRServerBeanInit;
import com.fast.dao.DBUtils;
import com.fast.dao.FTPCollect_DAO;
import com.fast.dao.FTPCollect_DAO_Factory;
import com.fast.pool.ftp.PoolManger;
import com.fast.recursor.FtpSlice;
import com.fast.reptile.FtpCollecter;
import com.fast.reptile.FtpReptileZip;
import com.fast.scheduler.CollectCustomListnerScheduler;

public class CollectCustomListener {
	public static ThreadPoolExecutor threadPool_Main;
	
	public static String IniFileName;
	public static String thread_run_flag = "FALSE";
	public static Map<String,String> enbidMap;
	private static String logInfo = "";
	private static Map<FtpCollecter,Future<String>> futureMaps;
	
	public static void main(String[] args)
	{
		IniFileName = args[0];
		CollectCustomListnerScheduler.init();
		//func_main();
	}
	
	public static void func_main() {
		threadPool_Main = new ThreadPoolExecutor(50, 100, 5, TimeUnit.MINUTES,
	            new ArrayBlockingQueue<Runnable>(200));
		futureMaps = new ConcurrentHashMap<FtpCollecter,Future<String>>();
		FtpCollecter.reSetCollect_FileSum();
		//初始化ini配置
		MRServerBeanInit.initMRServerBean(IniFileName);
		//测试DB是否连通
		Connection conn = null;
		try {
			conn = DBUtils.openConnection();
		} catch (SQLException e1) {
			System.out.println(new Date()+"DB 连接失败! 请检查连接信息!");
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
		
		Map<String,MRServerBean> MRServerBeanMap = MRServerBeanInit.getMRServerBeanMap();
		
		System.out.println(new Date()+" 源FTP共有: "+MRServerBeanMap.size());
		
		FTPCollect_DAO ftpdao = FTPCollect_DAO_Factory.getFTPCollect_DAO();
		
		//获取任务Bean
		Collect_CustomBean customBean = null;
		try {
			customBean = ftpdao.CreateBeanCollectCustom();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if(customBean == null){
			System.out.println(new Date()+"当前在DB中未获取到current_status为9的任务!");
			thread_run_flag = "FALSE";
			return;
		}
		else{
			System.out.println(new Date()+"当前在DB中已获取到任务,sround 为"+customBean.getSround());
			ftpdao.updateInfo(customBean.getSround(),"开始初始化任务","1");
		}
		//获取待采集测试
		Map<String,Set<String>> queryByCollectMap = null;
		try {
			queryByCollectMap = ftpdao.queryByCollectCustom(customBean);
		} catch (Exception e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		if(queryByCollectMap.size() == 0){
			ftpdao.updateInfo(customBean.getSround(),"根据您的CUSTOM配置,没有搜索到文件","4");
			System.out.println(new Date()+"SROUND:"+customBean.getSround()+",根据您的CUSTOM配置,没有搜索到文件");
			thread_run_flag = "FALSE";
			if("1".equalsIgnoreCase(customBean.getIs_loop())){
				ftpdao.updateStatus(customBean.getSround(),"等待下次循环调用!","9");
			}
			return;
		}
		else{
			ftpdao.updateInfo(customBean.getSround(),"根据 sround 为"+customBean.getSround()+"的 任务,搜索到 "+queryByCollectMap.size()+"个文件待处理","1");
			System.out.println(new Date()+"根据 sround 为"+customBean.getSround()+"的 任务,搜索到 "+queryByCollectMap.size()+"个文件待处理");
		}
		
		//公参处理
		try {
			enbidMap = ftpdao.initEnbidListSiteInfo();
			
			if(enbidMap.size() != 0){
			  ftpdao.updateInfo(customBean.getSround(),"SROUND:"+customBean.getSround()+"数据库中读取公参表基站数:"+enbidMap.size(),"1");
			  System.out.println(new Date()+"SROUND:"+customBean.getSround()+"数据库中读取公参表基站数:"+enbidMap.size());
			}
			else{
				ftpdao.updateInfo(customBean.getSround(),"SROUND:"+customBean.getSround()+"数据库中读取公参表无基站数,将终止!","4");
				System.out.println(new Date()+"SROUND:"+customBean.getSround()+"数据库中读取公参表无基站数,将终止!");
				thread_run_flag = "FALSE";
				if("1".equalsIgnoreCase(customBean.getIs_loop())){
					ftpdao.updateStatus(customBean.getSround(),"等待下次循环调用!","9");
				}
				return;
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		ftpdao.updateStatusForStart(customBean.getSround(),"正在获取源FTP共有:"+MRServerBeanMap.size());
		
		int objcount = 0;
		String sround = null;
		String ip = null;
		String port = null;
		String filePathBase = null;
		String pre_FilePathBase = null;
		//String fileName = null;
		String isTar = null;
		String pre_IsTar = null;
		String current_ipport = null;
		String pre_Ipport = null;

		String current_FileNameBase = null;
		String pre_FileNameBase = null;
		int filesReadyCount = 0;
		try {
			ftpdao.updateInfo(customBean.getSround(),"当前获得待处理的原始包个数为:"+queryByCollectMap.size(),"1");
			
			Map<String,Set<String>> handleMap = null;
			Set<String> FileNameSet = null;
			Set<String> pre_FileNameSet = null;
			Set<Entry<String,Set<String>>> fileRecordSet = queryByCollectMap.entrySet();
			for(Iterator<Map.Entry<String,Set<String>>> iterator = fileRecordSet.iterator();iterator.hasNext();)
			{
				Map.Entry<String,Set<String>> entry = (Map.Entry<String, Set<String>>) iterator.next();
				String sobj = entry.getKey();
				FileNameSet = entry.getValue();
				filesReadyCount = filesReadyCount + FileNameSet.size();
			}
			fileRecordSet = queryByCollectMap.entrySet();
			
			for(Iterator<Map.Entry<String,Set<String>>> iterator = fileRecordSet.iterator();iterator.hasNext();)
			{
				Map.Entry<String,Set<String>> entry = (Map.Entry<String, Set<String>>) iterator.next();
				String sobj = entry.getKey();
				FileNameSet = entry.getValue();
				//filesReadyCount = filesReadyCount + FileNameSet.size();
				
				String[] fields = sobj.split(",");
				sround = fields[0];
				ip = fields[1];
				port = fields[2];
				filePathBase = fields[3];
				isTar = fields[4];
				
				MRServerBean dm = MRServerBeanMap.get(ip+":"+port);
				if(dm == null){
					ftpdao.updateInfo(customBean.getSround(),"[error] 服务器 "+ip+":"+port + " 在ini中不存在! 退出该服务器采集","4");
					System.out.println(new Date()+"SROUND:"+customBean.getSround()+"[error] 服务器 "+ip+":"+port + " 在ini中不存在! 退出该服务器采集" );
					continue;
				}
				//System.out.println(sround+","+ip+","+port+","+filePathBase+","+isTar);
				
				current_ipport = ip + ":" + port;
				current_FileNameBase = filePathBase;
				
				//System.out.println("current_ipport:"+current_ipport+" sround:"+sround);

				if(handleMap == null){
					handleMap = new HashMap<String,Set<String>>();
				}
				
				if(current_ipport.equalsIgnoreCase(pre_Ipport) || pre_Ipport == null){
					//handleMap.put(filePathBase+","+fileName+","+isTar,"");
					
					handleMap.put(filePathBase+","+isTar,FileNameSet);
					//System.out.println(filePathBase+","+isTar);
					//一个大包抵5000个小包
					if("1".equalsIgnoreCase(isTar)){
						objcount = objcount + 1000;
					}
					else{
						objcount ++;
					}
					
					//构建Set
					if(objcount > 3000){
						FtpCollecter ftpcollars = new FtpCollecter();
						ftpcollars.setHandleObjMap(handleMap);
						ftpcollars.setEnbidMap(enbidMap);
						ftpcollars.setDm(dm);
						ftpcollars.setSround(sround);
						//加入到线程池
						Future<String> future = threadPool_Main.submit(ftpcollars);
						futureMaps.put(ftpcollars, future);
						
						logInfo = "[SROUND<"+customBean.getSround()+">正在分配线程]"+dm.MRServerPro+"\t"+ dm.sIP+" 分配采集文件数: "
								+handleMap.size()+",累计总任务数:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
						ftpdao.updateInfo(customBean.getSround(),logInfo,"3");
						System.out.println(logInfo);
						
						handleMap = null;
						objcount = 0;
						
						while(threadPool_Main.getQueue().size() > 100){
							try {
								logInfo = "[SROUND<"+customBean.getSround()+">等待线程队列处理] ThreadActiveCount:"
										+threadPool_Main.getActiveCount()+" QueueThread:"+threadPool_Main.getQueue().size()
										+",累计总任务数:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
								ftpdao.updateStatusForEnd(customBean.getSround(),filesReadyCount,FtpCollecter.getCollect_FileSum(),logInfo,"0");
								getFutureStatus();
								System.out.println(logInfo);
								//future.get();
								Thread.sleep(30*1000L);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
				else{
					//System.out.println("开始分配线程");
					//handleMap.put(pre_FilePathBase+","+pre_IsTar,pre_FileNameSet);
					FtpCollecter ftpcollars = new FtpCollecter();
					ftpcollars.setHandleObjMap(handleMap);
					ftpcollars.setEnbidMap(enbidMap);
					dm = MRServerBeanMap.get(pre_Ipport);
					ftpcollars.setDm(dm);
					ftpcollars.setSround(sround);
					//加入到线程池
					Future<String> future = threadPool_Main.submit(ftpcollars);
					futureMaps.put(ftpcollars, future);
					
					logInfo = "[SROUND<"+customBean.getSround()+">正在分配线程]"+dm.MRServerPro+"\t"+ dm.sIP+" 分配采集文件数: "+handleMap.size()
							+",累计总任务数:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
					ftpdao.updateInfo(customBean.getSround(),logInfo,"3");
					System.out.println(logInfo);
					
					handleMap = new HashMap<String,Set<String>>();
					handleMap.put(filePathBase+","+isTar,FileNameSet);
					objcount = 1;
					
					while(threadPool_Main.getQueue().size() > 100){
						try {
							logInfo = "[SROUND<"+customBean.getSround()+">等待线程队列处理] ThreadActiveCount:"+threadPool_Main.getActiveCount()+" QueueThread:"+threadPool_Main.getQueue().size()+",累计总任务数:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
							ftpdao.updateStatusForEnd(customBean.getSround(),filesReadyCount,FtpCollecter.getCollect_FileSum(),logInfo,"0");
							//future.get();
							getFutureStatus();
							System.out.println(logInfo);
							Thread.sleep(30*1000L);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				
				pre_Ipport = fields[1] + ":" + fields[2];
				pre_FileNameBase = fields[3];
				pre_IsTar = fields[4];
				pre_FileNameSet = FileNameSet;
			}
			
			if(objcount > 0){
				FtpCollecter ftpcollars = new FtpCollecter();
				ftpcollars.setHandleObjMap(handleMap);
				ftpcollars.setEnbidMap(enbidMap);
				MRServerBean dm = MRServerBeanMap.get(ip+":"+port);
				ftpcollars.setDm(dm);
				ftpcollars.setSround(sround);
				//加入到线程池
				//System.out.println("FtpSlice:"+dm.MRServerPro+"\t"+ dm.sIP+" 待采集文件数: "+handleMap.size());
				Future<String> future = threadPool_Main.submit(ftpcollars);
				futureMaps.put(ftpcollars, future);
				
				while(threadPool_Main.getQueue().size() > 100){
					try {
						logInfo = "[SROUND<"+customBean.getSround()+">等待线程队列处理] ThreadActiveCount:"+threadPool_Main.getActiveCount()+" QueueThread:"+threadPool_Main.getQueue().size()
								+",累计总任务数:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
						ftpdao.updateStatusForEnd(customBean.getSround(),filesReadyCount,FtpCollecter.getCollect_FileSum(),logInfo,"0");
						//future.get();
						getFutureStatus();
						System.out.println(logInfo);
						Thread.sleep(30*1000L);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		while(true){
			if((threadPool_Main.getActiveCount() !=0 || threadPool_Main.getQueue().size() !=0)){
				logInfo = new Date()+"SROUND:"+customBean.getSround()
				+" ******[Process CollectCustomListener] ThreadActiveCount:"
				+threadPool_Main.getActiveCount()+" QueueThread:"+threadPool_Main.getQueue().size()
				+",累计总任务数:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
				System.out.println(logInfo);
				try {
					ftpdao.updateStatusForEnd(customBean.getSround(),filesReadyCount,FtpCollecter.getCollect_FileSum()
							,logInfo,"0");
					getFutureStatus();
					Thread.sleep(30*1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}else
			{
				PoolManger.destroyAllObject();
				ftpdao.updateStatusForEnd(customBean.getSround(),filesReadyCount,FtpCollecter.getCollect_FileSum(),"***本轮次用户定制化采集成功完成***","1");
				System.out.println(new Date()+"SROUND:"+customBean.getSround()+"***本轮次用户定制化采集成功完成***预计采集个数:"+filesReadyCount+",完成采集"+FtpCollecter.getCollect_FileSum());
				break;
			}
		}
		futureMaps.clear();
		threadPool_Main.shutdown();
		thread_run_flag = "FALSE";
		if("1".equalsIgnoreCase(customBean.getIs_loop())){
			ftpdao.updateStatus(customBean.getSround(),"等待下次循环调用!","9");
		}
	}

	public static void getFutureStatus(){
		Set<Entry<FtpCollecter, Future<String>>> set = futureMaps.entrySet();
		long currentDateLong = new Date().getTime();
		
		for(Entry<FtpCollecter, Future<String>> ef : set){
			FtpCollecter ftpcollecter = ef.getKey();
			Future<String>  future = ef.getValue();
			if(ftpcollecter.getsThreadDate_start() == null)continue;
			double vmin = (double)((currentDateLong - ftpcollecter.getsThreadDate_start().getTime())/(60*1000));
			logInfo = "[CollectCustomListener信息]"
			+ftpcollecter.getDm().getsIP()+" 线程:"+ftpcollecter.getCurrent_Thread().getName()+" 已运行分钟:"+vmin
			+",状态:("+ftpcollecter.getCurrent_downloadFiles()+":"+ftpcollecter.getHandleObjMap().size()+")"+",累计总任务数:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
			
			System.out.println(logInfo);
			//超时2hour则结束任务
			if(vmin > 120 && !future.isDone()){
				boolean cancelStatus = future.cancel(true);
				boolean interrupStatus = ftpcollecter.getCurrent_Thread().interrupted();
				logInfo = "[CollectCustomListener信息]"+ftpcollecter.getDm().getsIP()+" 线程:"
				+ftpcollecter.getCurrent_Thread().getName()+",递归搜索文件时:"+"超过 "+vmin+" 分钟,任务cancel状态:"+cancelStatus
				+",任务interrupte状态:"+interrupStatus
				+",状态:("+ftpcollecter.getCurrent_downloadFiles()+":"+ftpcollecter.getHandleObjMap().size()+")"
				+",累计总任务数:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
				
				System.out.println(logInfo);
			}
			
			//清理已完成的future,当future.cancel(true)时,future.isDone()为TRUE,但 threadPool_Main.getActiveCount() 不释放!!!
			if(future.isDone() && "1".equalsIgnoreCase(ftpcollecter.getIsDone())){
				futureMaps.remove(ftpcollecter);
			}
		}
	}
}
