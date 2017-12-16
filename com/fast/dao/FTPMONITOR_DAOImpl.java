package com.fast.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import com.fast.common.Utils;

public class FTPMONITOR_DAOImpl implements FTPMONITOR_DAO {

	public synchronized static String getSysRandom(){
		Random rd = new Random();
		String currtime = String.valueOf(System.currentTimeMillis());
		DateFormat dfErrLog = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		Thread th = Thread.currentThread();
		String SerialNum = dfErrLog.format(new Date())+rd.nextLong()+currtime+th.getId();
		return SerialNum;
	}
	public Set<String> queryByIpDate(String ip,int port,String sDate,String isFindTar) throws Exception{
		Connection conn = null;  
	    PreparedStatement pst = null;
	    DateFormat dfMRLog = new SimpleDateFormat("yyyyMMdd");
	    Set<String> hs = new HashSet<String>();
	    Date dt = null;
		try {
			dt = dfMRLog.parse(sDate);
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	     
	    try {  
	        conn = DBUtils.openConnection();
	        String sql = null;
			if("1".equalsIgnoreCase(isFindTar)){
				sql = "select FILE_PATH_BASE  from FAST_FTP_MONITOR_DETAIL partition(P_{PARTDATA}) "
						+ " where ip = ? and port=? and is_tar=? and sdate=?";
				sql = sql.replace("{PARTDATA}", sDate);
				pst = conn.prepareStatement(sql);
		        pst.setString(1, ip);
		        pst.setInt(2, port);
		        pst.setString(3, isFindTar);
				pst.setDate(4, new java.sql.Date(dt.getTime()));
			}
			else{
				sql = "select FILE_PATH_BASE  from FAST_FTP_MONITOR_DETAIL partition(P_{PARTDATA}) "
						+ " where ip = ? and port=? and sdate=?";
				sql = sql.replace("{PARTDATA}", sDate);
				pst = conn.prepareStatement(sql);
		        pst.setString(1, ip);
		        pst.setInt(2, port);
				pst.setDate(3, new java.sql.Date(dt.getTime()));
			}
	        
	        ResultSet rs = pst.executeQuery();  
	        while(rs.next()){  
	        	hs.add(rs.getString("FILE_PATH_BASE"));
	        }  
	    } catch (Exception e) {  
	        e.printStackTrace();  
	        throw new Exception("数据访问异常");  
	    }finally{  
	        DBUtils.closeConnection(conn);  
	    }  
	    return hs;
		
		
	}
	
	@Override
	public void insertBatch(String ip,int port,HashMap<String, Long> ftpFilesMap) {
		
		PreparedStatement pst = null;
		Connection conn = null;
		int insertBath = 0;
		int insertCount = 0;
		String fileDate = null;
		DateFormat dfMRLog = new SimpleDateFormat("yyyyMMdd");
		try {
			conn = DBUtils.openConnection();
			Long begin = new Date().getTime();
			String sql = "insert into FAST_FTP_SESSION_PRESERVE"
					+ "(sdate, ip, port, enbid, mr_type,FILE_NAME, file_path_base, file_path, file_length,is_tar,shour) "
					+ " values(?,?,?,?,?,?,?,?,?,?,?)";
			conn.setAutoCommit(false);
			pst = conn.prepareStatement(sql);
			String fileName = null;
			Set<Entry<String,Long>> ftpSet = ftpFilesMap.entrySet();
			for(Iterator<Map.Entry<String,Long>> iterator = ftpSet.iterator();iterator.hasNext();)
			{	insertBath ++;
				insertCount ++;
				Map.Entry<String,Long> entry = (Map.Entry<String, Long>) iterator.next();
				String vfileNamePaths = entry.getKey();
				Long fileSize = entry.getValue();
				String file_PathBase = null;
				String vfileNamePath = null;
				String is_Tar = null;
				if(vfileNamePaths.contains(";")){
					String[] vfileNamePathList = vfileNamePaths.split(";");
					file_PathBase = vfileNamePathList[0];
					vfileNamePath = vfileNamePathList[1];
					is_Tar = "1";
				}
				else{
					file_PathBase = vfileNamePaths;
					vfileNamePath = vfileNamePaths;
				}
				
				fileDate = Utils.extractDate8fromStr(vfileNamePath,"");
				vfileNamePath = Utils.getlinuxPath(vfileNamePath,"1");
				
				//文件名
				fileName = vfileNamePath.substring(vfileNamePath.lastIndexOf("/")+1);
				
				//enbid
				String enbid = Utils.extractEnbidfromFileName(fileName,"");
				//shour
				String shour = Utils.extractHour24fromStr(fileName,"");
				
				//mr_type
				String mr_Type = null;
				if(fileName.toUpperCase().contains("_MRO_")){
					mr_Type = "MRO";
				}else if(fileName.toUpperCase().contains("_MRE_")){
					mr_Type = "MRE";
				}else if(fileName.toUpperCase().contains("_MRS_")){
					mr_Type = "MRS";
				}
				Date dt = dfMRLog.parse(fileDate);
				pst.setDate(1, new java.sql.Date(dt.getTime()));
				pst.setString(2, ip);
				pst.setInt(3, port);
				pst.setString(4, enbid);
				pst.setString(5, mr_Type);
				pst.setString(6, fileName);
				pst.setString(7, file_PathBase);
				pst.setString(8, vfileNamePath);
				pst.setInt(9, fileSize.intValue());
				pst.setString(10, is_Tar);
				pst.setString(11, shour);
				
				pst.addBatch();
				
				if(insertBath > 50000){
					pst.executeBatch();
					insertBath = 0;
				}
			}
			
			if(insertBath >0 ){
				pst.executeBatch();
			}
			//merge
			
			sql = 
				"merge into FAST_FTP_MONITOR_DETAIL partition(P_{PARTDATA}) a\n" +
				"using(select * from FAST_FTP_SESSION_PRESERVE)b\n" + 
				"on(a.sdate = to_date(?,'yyyymmdd')\n"
				+ " and a.sdate = b.sdate \n" + 
				"and a.ip = b.ip\n" + 
				"and a.port=b.port\n" + 
				"and a.file_path = b.file_path\n" + 
				")\n" + 
				"when matched then update set update_date = sysdate,\n" + 
				"                             update_count = nvl(update_count,0) + 1\n" + 
				"when not matched then insert (SDATE,SHOUR,IP,PORT,enbid, mr_type, file_name, file_path_base,file_path,file_length,is_tar)\n" + 
				"values(b.sdate,b.shour,b.ip,b.port,b.enbid,b.mr_type,b.file_name,b.file_path_base,b.file_path,b.file_length,b.is_tar)";
			sql = sql.replace("{PARTDATA}",fileDate);
			
			pst = conn.prepareStatement(sql);
			pst.setString(1, fileDate);
			pst.executeUpdate();
			conn.commit();
			pst.close();
			
			Long end = new Date().getTime();
			System.out.println("程序与DB Merge记录数:"+insertCount+"\t 耗时 : " + (end - begin) / 1000 + " s");
		}catch(Exception e){
			e.printStackTrace();
		}finally{  
	        DBUtils.closeConnection(conn);  
	    }
	}
	public void updateStatus(String fileDate,String ip,int port,Set<String> ftpFilesSet){
		PreparedStatement pst = null;
		Connection conn = null;
		int insertBath = 0;
		int insertCount = 0;
		DateFormat dfMRLog = new SimpleDateFormat("yyyyMMdd");
		//String sdate = "";
		try {
			conn = DBUtils.openConnection();
			Long begin = new Date().getTime();
			String sql = "insert into FAST_FTP_SESSION_PRESERVE"
					+ "(sdate, ip, port, file_path ) "
					+ " values(?,?,?,?)";
			conn.setAutoCommit(false);
			pst = conn.prepareStatement(sql);
			for(String filePath:ftpFilesSet)
			{	insertBath ++;
				insertCount ++;
				String vfileNamePath = filePath;
				vfileNamePath = Utils.getlinuxPath(vfileNamePath,"1");
				
				Date dt = dfMRLog.parse(fileDate);
				pst.setDate(1, new java.sql.Date(dt.getTime()));
				pst.setString(2, ip);
				pst.setInt(3, port);
				pst.setString(4, vfileNamePath);
				
				pst.addBatch();
				
				if(insertBath > 500000){
					pst.executeBatch();
					insertBath = 0;
				}
			}
			
			if(insertBath >0 ){
				pst.executeBatch();
			}
			//merge
			
			sql = 
				"merge into FAST_FTP_MONITOR_DETAIL partition(P_{PARTDATA}) a\n" +
				"using(select * from FAST_FTP_SESSION_PRESERVE)b\n" + 
				"on( a.sdate = to_date(?,'yyyymmdd')\n" 
				+ "and a.sdate = b.sdate \n" + 
				"and a.ip = b.ip\n" + 
				"and a.port=b.port\n" + 
				"and a.file_path_base = b.file_path\n" + 
				")\n" + 
				"when matched then update set update_date = sysdate,\n" + 
				"                             update_count = nvl(update_count,0) + 1\n";
			sql = sql.replace("{PARTDATA}", fileDate);
			pst = conn.prepareStatement(sql);
			pst.setString(1, fileDate);
			pst.executeUpdate();
			conn.commit();
			pst.close();
			
			Long end = new Date().getTime();
			System.out.println("BIGZIP 程序与DB Merge记录数:"+insertCount+"\t 耗时 : " + (end - begin) / 1000 + " s");
		}catch(Exception e){
			e.printStackTrace();
		}finally{  
	        DBUtils.closeConnection(conn);  
	    }
	}
 
	@Override
	public void mergeBatch() {
		// TODO Auto-generated method stub
		
	}
	public void insertMonitorStart(String current_info,int monitor_servers,String monitor_range) {
		
		PreparedStatement pst = null;
		Connection conn = null;
		int insertCount = 0;
		try {
			conn = DBUtils.openConnection();
			Long begin = new Date().getTime();
			
			String sql = 
				"merge into fast_ftp_monitor_status a\n" +
				"using(select ? current_info, ? monitor_servers, ? monitor_range from dual)b\n" + 
				"on(a.monitor_range = b.monitor_range)\n" + 
				"when matched then update set a.threadmain_heartbeat = sysdate,\n" + 
				"  current_info = b.current_info,\n" + 
				"  monitor_servers = b.monitor_servers,\n" + 
				"  monitor_count = a.monitor_count + 1\n" + 
				"when not matched then insert (threadmain_heartbeat, current_info, monitor_servers, monitor_range, monitor_count)\n" + 
				"  values(sysdate,b.current_info,b.monitor_servers,b.monitor_range,1)";

			pst = conn.prepareStatement(sql);
			pst.setString(1, current_info);
			pst.setInt(2, monitor_servers);
			pst.setString(3, monitor_range);
			pst.executeUpdate();
			pst.close();
			
			Long end = new Date().getTime();
			System.out.println("程序与DB Merge记录数:"+insertCount+"\t 耗时 : " + (end - begin) / 1000 + " s");
		}catch(Exception e){
			e.printStackTrace();
		}finally{  
	        DBUtils.closeConnection(conn);  
	    }
	}

	public void insertMonitorStatus(String current_info,String monitor_range) {
		
		PreparedStatement pst = null;
		Connection conn = null;
		int insertCount = 0;
		try {
			conn = DBUtils.openConnection();
			Long begin = new Date().getTime();
			
			String sql = 
				"merge into fast_ftp_monitor_status a\n" +
				"using(select ? current_info, ? monitor_range from dual)b\n" + 
				"on(a.monitor_range = b.monitor_range)\n" + 
				"when matched then update set a.threadmain_heartbeat = sysdate,\n" + 
				"  current_info = b.current_info\n" + 
				"when not matched then insert (threadmain_heartbeat, current_info, monitor_range, monitor_count)\n" + 
				"  values(sysdate,b.current_info,b.monitor_range,1)";

			pst = conn.prepareStatement(sql);
			pst.setString(1, current_info);
			pst.setString(2, monitor_range);
			pst.executeUpdate();
			pst.close();
			
			Long end = new Date().getTime();
			System.out.println("程序与DB Merge记录数:"+insertCount+"\t 耗时 : " + (end - begin) / 1000 + " s");
		}catch(Exception e){
			e.printStackTrace();
		}finally{  
	        DBUtils.closeConnection(conn);  
	    }
	}
	public void insertMonitorLog(String current_info) {
		PreparedStatement pst = null;
		Connection conn = null;
		try {
			conn = DBUtils.openConnection();
			//Long begin = new Date().getTime();
			String sql = "insert into FAST_FTP_MONITOR_LOG"
					+ "(id,current_info) "
					+ " select SEQ_FAST_FTP_MONITOR_LOG.Nextval,? from dual";
			//conn.setAutoCommit(false);
			pst = conn.prepareStatement(sql);
			pst.setString(1, current_info);
			pst.executeUpdate();
			//conn.commit();
			pst.close();
			
			//Long end = new Date().getTime();
			//System.out.println("程序与DB Merge日志录入:"+" 耗时 : " + (end - begin) / 1000 + " s");
		}catch(Exception e){
			e.printStackTrace();
		}finally{  
	        DBUtils.closeConnection(conn);  
	    }
	}
}
