package com.fast.pool.ftp;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

import com.fast.common.*;
import com.fast.ftp.*;

public class PoolManger {
	/*
	 * FTPClientConfig config = new FTPClientConfig();
		config.setHost("127.0.0.1");
		config.setPort(21);
		config.setUsername("fast");
		config.setPassword("fast");
		config.setEncoding("utf-8");
		config.setPassiveMode("false");
		config.setClientTimeout(30 * 1000);
	 */
	private static ConcurrentHashMap<String,FTPPool> FTPPool_Map = new ConcurrentHashMap<String,FTPPool>();
	
	public static FtpDownload getFTPClient(String iip,int iport){
		//identity IP+PORT XXX.XXX.XXX.XXX:XXXXX
		String videntity = Utils.convertIdentity(iip,String.valueOf(iport));
		FtpDownload ftpclient = null;
		Set<Entry<String,FTPPool>> lhmpset = FTPPool_Map.entrySet();
		for(Iterator<Map.Entry<String,FTPPool>> iterator = lhmpset.iterator();iterator.hasNext();)
		{
			Map.Entry<String,FTPPool> entry = (Map.Entry<String, FTPPool>) iterator.next();
			
			if(entry.getKey().equalsIgnoreCase(videntity)){
				ftpclient = entry.getValue().getResource();
			}
		}
		return ftpclient;
	}
	public static FtpDownload getFTPClientTimeout(String iip,int iport){
		int iReFTPLogin = 1;
		
		FtpDownload fdlf = getFTPClient(iip,iport);
		
		//ReLogin 10 times
		while (fdlf == null) {
			try{
				iReFTPLogin ++;
				fdlf = getFTPClient(iip,iport);
			}catch(Exception e){
				
			}
			
			if(fdlf != null){
				break;
			}
			
			if (iReFTPLogin >= 3){
				break;
			}
			try {
				Thread.sleep(1000L*3);
			} catch (Exception e) {
				//e.printStackTrace();
			}
			
		}
		
		return fdlf;
	}
	public static void returnFTPClient(String iip,int iport,FtpDownload iftpclient){
		//identity IP+PORT XXX.XXX.XXX.XXX:XXXXX
		String videntity = Utils.convertIdentity(iip,String.valueOf(iport));
		FtpDownload ftpclient = null;
		Set<Entry<String,FTPPool>> lhmpset = FTPPool_Map.entrySet();
		FTPPool ftppool = null;
		for(Iterator<Map.Entry<String,FTPPool>> iterator = lhmpset.iterator();iterator.hasNext();)
		{
			Map.Entry<String,FTPPool> entry = (Map.Entry<String, FTPPool>) iterator.next();
			
			if(entry.getKey().equalsIgnoreCase(videntity)){
				ftppool = entry.getValue();
				try{
				ftppool.returnResource(iftpclient);
				}catch(Exception e){
					
				}
			}
		}
	}
	
	public static void destroyAllObject(){
		Set<Entry<String,FTPPool>> lhmpset = FTPPool_Map.entrySet();
		FTPPool ftppool = null;
		for(Iterator<Map.Entry<String,FTPPool>> iterator = lhmpset.iterator();iterator.hasNext();)
		{
			Map.Entry<String,FTPPool> entry = (Map.Entry<String, FTPPool>) iterator.next();
			
			ftppool = entry.getValue();
			ftppool.destroy();
		}
		FTPPool_Map.clear();
		
	}
	public static void setFTPClientInfo(String iIp, int iPort, String iUser, String iPassword, String iFtpType,String iPassiveMode,int idataTimeoutSecond){
		
		//�ж��Ƿ����?
		String videntity = Utils.convertIdentity(iIp,String.valueOf(iPort));
		//FtpDownload ftpclient = null;
		Set<Entry<String,FTPPool>> lhmpset = FTPPool_Map.entrySet();
		for(Iterator<Map.Entry<String,FTPPool>> iterator = lhmpset.iterator();iterator.hasNext();)
		{
			Map.Entry<String,FTPPool> entry = (Map.Entry<String, FTPPool>) iterator.next();
			
			if(entry.getKey().equalsIgnoreCase(videntity)){
				//System.out.println("setFTPClientInfo �Ѿ�����");
				return;
			}
		}

		//System.out.println("setFTPClientInfo NEW ����");
		//System.out.println("�½����Ӷ���!");
		//identity IP+PORT XXX.XXX.XXX.XXX:XXXXX
		GenericObjectPool.Config config = new Config();
		//��������
		config.maxActive=50;
		//config.maxWait = 1L;
		config.testWhileIdle = false;
		config.timeBetweenEvictionRunsMillis=30000L;
		//config.maxIdle = 2;
		//�ӳ���ȡ����ﵽ���ʱ,���������¶���.
		//config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_GROW;
		config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
		//��Ϊ��ʱȡ����ȴ�����������?.
		config.maxWait=6000*1000;
		//ȡ������ʱ��֤(�˴����ó���֤ftp�Ƿ�������״̬).
		config.testOnBorrow=true;
		//���ض���ʱ��֤(�˴����ó���֤ftp�Ƿ�������״̬).
		config.testOnReturn=true;
		
		FTPPool pool = new FTPPool(config,iIp,iPort,iUser,iPassword,iFtpType,iPassiveMode,idataTimeoutSecond);
		//FtpDownload ftpClient = null;
		/*for(int i=1;i<=15;i++){
			ftpClient = pool.getResource();
			pool.returnResource(ftpClient);
		}
		System.out.println(common.Utils.convertIdentity(iIp,String.valueOf(iPort)));*/
		FTPPool_Map.put(Utils.convertIdentity(iIp,String.valueOf(iPort)), pool);
	}
	
	public static void main(String[] args){
		
		//setFTPClientInfo("127.0.0.1",22,"fast","fast","SFTP","TRUE");
		/*FtpDownload ftpDlFiles = new FtpDownload("127.0.0.1",21,"fast","fast");
		ftpDlFiles.ftpLogin();*/
		
		/*SFtpDownload ftpDlFiles = new SFtpDownload("127.0.0.1",22,"fast","fast");
		ftpDlFiles.ftpLogin();
		ftpDlFiles.ftpLogin();
		ftpDlFiles.ftpLogin();*/
		//ftpDlFiles.ftpLogOut();
		
		FtpDownload ftpClient = getFTPClient("127.0.0.1",22);
		/*ftpClient = getFTPClient("127.0.0.1",22);
		ftpClient = getFTPClient("127.0.0.1",22);
		ftpClient = getFTPClient("127.0.0.1",22);
		ftpClient = getFTPClient("127.0.0.1",22);
		//returnFTPClient("127.0.0.1",22, fg);
		 HashSet<String> allFileDirList = new HashSet<String>();
	 		int vFolderLevelFact = 0;
	 		String vremoteDirectory = "/";
	 		
			 ftpClient.getLoadFiledir(allFileDirList, "/oamdata/", 1, vFolderLevelFact);
			 vFolderLevelFact = 0;
			 
			 ftpClient.getLoadFiledir(allFileDirList, "/oamdata/tce", 1, vFolderLevelFact);
			 vFolderLevelFact = 0;
			 
			 ftpClient.getLoadFiledir(allFileDirList, "/oamdata/tce/mr/", 1, vFolderLevelFact);
			 vFolderLevelFact = 0;
			 
			 ftpClient.getLoadFiledir(allFileDirList, "/oamdata/", 1, vFolderLevelFact);
			 vFolderLevelFact = 0;
			 
			 ftpClient.getLoadFiledir(allFileDirList, "/oamdata/", 1, vFolderLevelFact);
			 vFolderLevelFact = 0;*/
		try {
			//Thread.sleep(1000L*1);
		//	destroyAllObject();
			System.out.println("2222222");
			List<Long> filesizelist = new ArrayList<Long>();
			ftpClient.getDirectorySize("/oamdata12", filesizelist);
			
			System.out.println(filesizelist.get(0));
			//Thread.sleep(1000L*60*2);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
