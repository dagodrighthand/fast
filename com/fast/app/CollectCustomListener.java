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
		//��ʼ��ini����
		MRServerBeanInit.initMRServerBean(IniFileName);
		//����DB�Ƿ���ͨ
		Connection conn = null;
		try {
			conn = DBUtils.openConnection();
		} catch (SQLException e1) {
			System.out.println(new Date()+"DB ����ʧ��! ����������Ϣ!");
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
		
		System.out.println(new Date()+" ԴFTP����: "+MRServerBeanMap.size());
		
		FTPCollect_DAO ftpdao = FTPCollect_DAO_Factory.getFTPCollect_DAO();
		
		//��ȡ����Bean
		Collect_CustomBean customBean = null;
		try {
			customBean = ftpdao.CreateBeanCollectCustom();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if(customBean == null){
			System.out.println(new Date()+"��ǰ��DB��δ��ȡ��current_statusΪ9������!");
			thread_run_flag = "FALSE";
			return;
		}
		else{
			System.out.println(new Date()+"��ǰ��DB���ѻ�ȡ������,sround Ϊ"+customBean.getSround());
			ftpdao.updateInfo(customBean.getSround(),"��ʼ��ʼ������","1");
		}
		//��ȡ���ɼ�����
		Map<String,Set<String>> queryByCollectMap = null;
		try {
			queryByCollectMap = ftpdao.queryByCollectCustom(customBean);
		} catch (Exception e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		if(queryByCollectMap.size() == 0){
			ftpdao.updateInfo(customBean.getSround(),"��������CUSTOM����,û���������ļ�","4");
			System.out.println(new Date()+"SROUND:"+customBean.getSround()+",��������CUSTOM����,û���������ļ�");
			thread_run_flag = "FALSE";
			if("1".equalsIgnoreCase(customBean.getIs_loop())){
				ftpdao.updateStatus(customBean.getSround(),"�ȴ��´�ѭ������!","9");
			}
			return;
		}
		else{
			ftpdao.updateInfo(customBean.getSround(),"���� sround Ϊ"+customBean.getSround()+"�� ����,������ "+queryByCollectMap.size()+"���ļ�������","1");
			System.out.println(new Date()+"���� sround Ϊ"+customBean.getSround()+"�� ����,������ "+queryByCollectMap.size()+"���ļ�������");
		}
		
		//���δ���
		try {
			enbidMap = ftpdao.initEnbidListSiteInfo();
			
			if(enbidMap.size() != 0){
			  ftpdao.updateInfo(customBean.getSround(),"SROUND:"+customBean.getSround()+"���ݿ��ж�ȡ���α��վ��:"+enbidMap.size(),"1");
			  System.out.println(new Date()+"SROUND:"+customBean.getSround()+"���ݿ��ж�ȡ���α��վ��:"+enbidMap.size());
			}
			else{
				ftpdao.updateInfo(customBean.getSround(),"SROUND:"+customBean.getSround()+"���ݿ��ж�ȡ���α��޻�վ��,����ֹ!","4");
				System.out.println(new Date()+"SROUND:"+customBean.getSround()+"���ݿ��ж�ȡ���α��޻�վ��,����ֹ!");
				thread_run_flag = "FALSE";
				if("1".equalsIgnoreCase(customBean.getIs_loop())){
					ftpdao.updateStatus(customBean.getSround(),"�ȴ��´�ѭ������!","9");
				}
				return;
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		ftpdao.updateStatusForStart(customBean.getSround(),"���ڻ�ȡԴFTP����:"+MRServerBeanMap.size());
		
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
			ftpdao.updateInfo(customBean.getSround(),"��ǰ��ô������ԭʼ������Ϊ:"+queryByCollectMap.size(),"1");
			
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
					ftpdao.updateInfo(customBean.getSround(),"[error] ������ "+ip+":"+port + " ��ini�в�����! �˳��÷������ɼ�","4");
					System.out.println(new Date()+"SROUND:"+customBean.getSround()+"[error] ������ "+ip+":"+port + " ��ini�в�����! �˳��÷������ɼ�" );
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
					//һ�������5000��С��
					if("1".equalsIgnoreCase(isTar)){
						objcount = objcount + 1000;
					}
					else{
						objcount ++;
					}
					
					//����Set
					if(objcount > 3000){
						FtpCollecter ftpcollars = new FtpCollecter();
						ftpcollars.setHandleObjMap(handleMap);
						ftpcollars.setEnbidMap(enbidMap);
						ftpcollars.setDm(dm);
						ftpcollars.setSround(sround);
						//���뵽�̳߳�
						Future<String> future = threadPool_Main.submit(ftpcollars);
						futureMaps.put(ftpcollars, future);
						
						logInfo = "[SROUND<"+customBean.getSround()+">���ڷ����߳�]"+dm.MRServerPro+"\t"+ dm.sIP+" ����ɼ��ļ���: "
								+handleMap.size()+",�ۼ���������:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
						ftpdao.updateInfo(customBean.getSround(),logInfo,"3");
						System.out.println(logInfo);
						
						handleMap = null;
						objcount = 0;
						
						while(threadPool_Main.getQueue().size() > 100){
							try {
								logInfo = "[SROUND<"+customBean.getSround()+">�ȴ��̶߳��д���] ThreadActiveCount:"
										+threadPool_Main.getActiveCount()+" QueueThread:"+threadPool_Main.getQueue().size()
										+",�ۼ���������:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
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
					//System.out.println("��ʼ�����߳�");
					//handleMap.put(pre_FilePathBase+","+pre_IsTar,pre_FileNameSet);
					FtpCollecter ftpcollars = new FtpCollecter();
					ftpcollars.setHandleObjMap(handleMap);
					ftpcollars.setEnbidMap(enbidMap);
					dm = MRServerBeanMap.get(pre_Ipport);
					ftpcollars.setDm(dm);
					ftpcollars.setSround(sround);
					//���뵽�̳߳�
					Future<String> future = threadPool_Main.submit(ftpcollars);
					futureMaps.put(ftpcollars, future);
					
					logInfo = "[SROUND<"+customBean.getSround()+">���ڷ����߳�]"+dm.MRServerPro+"\t"+ dm.sIP+" ����ɼ��ļ���: "+handleMap.size()
							+",�ۼ���������:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
					ftpdao.updateInfo(customBean.getSround(),logInfo,"3");
					System.out.println(logInfo);
					
					handleMap = new HashMap<String,Set<String>>();
					handleMap.put(filePathBase+","+isTar,FileNameSet);
					objcount = 1;
					
					while(threadPool_Main.getQueue().size() > 100){
						try {
							logInfo = "[SROUND<"+customBean.getSround()+">�ȴ��̶߳��д���] ThreadActiveCount:"+threadPool_Main.getActiveCount()+" QueueThread:"+threadPool_Main.getQueue().size()+",�ۼ���������:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
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
				//���뵽�̳߳�
				//System.out.println("FtpSlice:"+dm.MRServerPro+"\t"+ dm.sIP+" ���ɼ��ļ���: "+handleMap.size());
				Future<String> future = threadPool_Main.submit(ftpcollars);
				futureMaps.put(ftpcollars, future);
				
				while(threadPool_Main.getQueue().size() > 100){
					try {
						logInfo = "[SROUND<"+customBean.getSround()+">�ȴ��̶߳��д���] ThreadActiveCount:"+threadPool_Main.getActiveCount()+" QueueThread:"+threadPool_Main.getQueue().size()
								+",�ۼ���������:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
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
				+",�ۼ���������:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
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
				ftpdao.updateStatusForEnd(customBean.getSround(),filesReadyCount,FtpCollecter.getCollect_FileSum(),"***���ִ��û����ƻ��ɼ��ɹ����***","1");
				System.out.println(new Date()+"SROUND:"+customBean.getSround()+"***���ִ��û����ƻ��ɼ��ɹ����***Ԥ�Ʋɼ�����:"+filesReadyCount+",��ɲɼ�"+FtpCollecter.getCollect_FileSum());
				break;
			}
		}
		futureMaps.clear();
		threadPool_Main.shutdown();
		thread_run_flag = "FALSE";
		if("1".equalsIgnoreCase(customBean.getIs_loop())){
			ftpdao.updateStatus(customBean.getSround(),"�ȴ��´�ѭ������!","9");
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
			logInfo = "[CollectCustomListener��Ϣ]"
			+ftpcollecter.getDm().getsIP()+" �߳�:"+ftpcollecter.getCurrent_Thread().getName()+" �����з���:"+vmin
			+",״̬:("+ftpcollecter.getCurrent_downloadFiles()+":"+ftpcollecter.getHandleObjMap().size()+")"+",�ۼ���������:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
			
			System.out.println(logInfo);
			//��ʱ2hour���������
			if(vmin > 120 && !future.isDone()){
				boolean cancelStatus = future.cancel(true);
				boolean interrupStatus = ftpcollecter.getCurrent_Thread().interrupted();
				logInfo = "[CollectCustomListener��Ϣ]"+ftpcollecter.getDm().getsIP()+" �߳�:"
				+ftpcollecter.getCurrent_Thread().getName()+",�ݹ������ļ�ʱ:"+"���� "+vmin+" ����,����cancel״̬:"+cancelStatus
				+",����interrupte״̬:"+interrupStatus
				+",״̬:("+ftpcollecter.getCurrent_downloadFiles()+":"+ftpcollecter.getHandleObjMap().size()+")"
				+",�ۼ���������:"+futureMaps.size()+","+threadPool_Main.getTaskCount();
				
				System.out.println(logInfo);
			}
			
			//��������ɵ�future,��future.cancel(true)ʱ,future.isDone()ΪTRUE,�� threadPool_Main.getActiveCount() ���ͷ�!!!
			if(future.isDone() && "1".equalsIgnoreCase(ftpcollecter.getIsDone())){
				futureMaps.remove(ftpcollecter);
			}
		}
	}
}
