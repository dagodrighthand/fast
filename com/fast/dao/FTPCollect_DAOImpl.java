package com.fast.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.collections.map.LinkedMap;

import com.fast.bean.Collect_CustomBean;
import com.fast.common.Utils;

public class FTPCollect_DAOImpl implements FTPCollect_DAO{
	

	 public Collect_CustomBean CreateBeanCollectCustom() throws Exception{
		    Connection conn = null;  
		    PreparedStatement pst = null;
		    Collect_CustomBean cc = null;
		    
		    String sql =
						"  select SROUND,START_DATE,HOUR_LIST,ENBID_GROUP,IS_LOOP,MR_TYPE,FILE_FAILCOUNT "
						+ " from(select SROUND,START_DATE,HOUR_LIST,ENBID_GROUP,IS_LOOP,MR_TYPE,FILE_FAILCOUNT "
						+ ",row_number() over(order by update_date nulls first,start_date)rn "
						+ "  from FAST_FTP_COLLECT_CUSTOM where open_flag = 1 "
						+ " and (current_status = '9' /*or IS_LOOP = '1'*/))where rn =1 ";
		    try {
		        conn = DBUtils.openConnection();
		        pst = conn.prepareStatement(sql);
		        
		        String sround = null;
		        String start_date = null;
		        String hour_list = null;
		        String enbid_group = null;
		        String mr_type = null;
		        int file_failcount = 0;
		        String is_loop = null;
		        ResultSet rs = pst.executeQuery();
		        while(rs.next()){
		        	sround = rs.getString("SROUND");
		        	start_date = rs.getString("START_DATE");
		        	hour_list = rs.getString("HOUR_LIST");
		        	enbid_group = rs.getString("ENBID_GROUP");
		        	is_loop = rs.getString("IS_LOOP");
		        	mr_type = rs.getString("MR_TYPE");
		        	file_failcount = rs.getInt("FILE_FAILCOUNT");
		        	if(file_failcount == 0){file_failcount = 100;}
		        	cc = new Collect_CustomBean();
		        	cc.setSround(sround);
		        	cc.setStart_date(start_date);
		        	cc.setHour_list(hour_list);
		        	cc.setEnbid_group(enbid_group);
		        	cc.setIs_loop(is_loop);
		        	cc.setMr_type(mr_type);
		        	cc.setFail_count(file_failcount);
		        }
		    } catch (Exception e) {
		        e.printStackTrace();
		        throw new Exception("数据访问异常");
		    }finally{
		        DBUtils.closeConnection(conn);
		    }
		    return cc;
	 }
	
	 public Map<String,Set<String>> queryByCollectCustom(Collect_CustomBean customBean) throws Exception{

		  Connection conn = null;  
		    PreparedStatement pst = null;
		    Map<String,Set<String>> queryByCustomMap = new LinkedHashMap<String,Set<String>>();

			DateFormat dfMRLog = new SimpleDateFormat("yyyyMMdd");
		    Date dt = dfMRLog.parse(customBean.getStart_date());
		    
			String sql =
			"select /*+ parallel(a 8) */distinct ? SROUND,IP,PORT,FILE_PATH_BASE,FILE_NAME,nvl(is_tar,'0')IS_TAR\n" +
			"    from FAST_FTP_MONITOR_DETAIL partition(P_{PARTDATA}) a,(select distinct enbid,enbid_group from FAST_FTP_COLLECT_ENBID_CFG) b\n" + 
			"                where   a.enbid = b.enbid\n" + 
			"  				 and ','||trim(?)||',' like '%,'||b.enbid_group ||',%' \n" + 
			"                and regexp_like(a.mr_type,nvl(replace(?,',','|'),a.mr_type))\n" + 
			"                and a.sdate = ?\n" + 
			"                and not exists(select 1 from FAST_FTP_COLLECT_CUSTOM_DETAIL partition(P_{PARTDATA}) c\n" + 
			"                   where c.sround = ? "
			+ "				      and c.sdate =? "
			+ "				      and a.ip = c.ip "
			+ " 				  and a.file_name = c.file_name\n"
			+ "					  and (c.fail_count = -1 or c.fail_count > ?)" + 
			"                 )\n" + 
			"                and ? like '%'||a.shour||'%' " + 
			"                order by ip,PORT,FILE_PATH_BASE";
			sql = sql.replace("{PARTDATA}", customBean.getStart_date());

		    try {
		        conn = DBUtils.openConnection();
		        pst = conn.prepareStatement(sql);
		        pst.setString(1, customBean.getSround());
		        pst.setString(2, customBean.getEnbid_group());
		        pst.setString(3, customBean.getMr_type());
		        pst.setDate(4, new java.sql.Date(dt.getTime()));
		        pst.setString(5, customBean.getSround());
		        pst.setDate(6, new java.sql.Date(dt.getTime()));
		        pst.setInt(7, customBean.getFail_count());
		        pst.setString(8, customBean.getHour_list());
		        
		        String sround = null;
		        String ip = null;
		        int port = 0;
		        String file_path_base = null;
		        String file_name = null;
		        String is_tar = null;
		        
		        String pre_sround = null;
		        String pre_ip = null;
		        int pre_port = 0;
		        String pre_file_path_base = null;
		        String pre_is_tar = null;
		        
		        ResultSet rs = pst.executeQuery();
		        Set<String> singleBigSet = null;
		        int count = 0;
		        while(rs.next()){
		        	sround = rs.getString("SROUND");
		        	ip = rs.getString("IP");
		        	port = rs.getInt("PORT");
        			file_path_base = rs.getString("FILE_PATH_BASE");
        		    file_name = rs.getString("FILE_NAME");
        		    is_tar = rs.getString("IS_TAR");
        		    
        		    if(ip.equalsIgnoreCase(pre_ip) && port == pre_port){
        		    	
        		    	if(!file_path_base.equalsIgnoreCase(pre_file_path_base))
        		    	{
        		    		queryByCustomMap.put(pre_sround+","+pre_ip+","+pre_port+","+pre_file_path_base+","+pre_is_tar, singleBigSet);
        		    		singleBigSet = new HashSet<String>();
            		    	count = 0;
        		    	}
        		    }
        		    else{
        		    	if(pre_sround != null){
        		    		queryByCustomMap.put(pre_sround+","+pre_ip+","+pre_port+","+pre_file_path_base+","+pre_is_tar, singleBigSet);
        		    	}
        		    	singleBigSet = new HashSet<String>();
        		    	count = 0;
        		    }
        		    singleBigSet.add(file_name);
        		    count ++;
        		    pre_is_tar = is_tar;
        		    pre_sround = sround;
        		    pre_ip = ip;
        		    pre_port = port;
        		    pre_file_path_base = file_path_base;
		        }
		        
		        if(count > 0){
		        	queryByCustomMap.put(sround+","+ip+","+port+","+file_path_base+","+is_tar, singleBigSet);
		        }
		    } catch (Exception e) {
		        e.printStackTrace();
		        throw new Exception("数据访问异常");
		    }finally{
		        DBUtils.closeConnection(conn);
		    }
		    return queryByCustomMap;

	  }
	public void updateStatusForStart(String sRound,String current_Info){
		PreparedStatement pst = null;
		Connection conn = null;
		//String sdate = "";
		try {
			conn = DBUtils.openConnection();

			String sql =
				"update FAST_FTP_COLLECT_custom t\n" +
				"set current_status = '1'\n" + 
				",collect_date_start = sysdate\n" + 
				",collect_date_end = null \n" + 
				",estimate_filecount = null \n" + 
				",collect_filecount = null \n" + 
				",update_date = sysdate\n" + 
				",CURRENT_INFO = ? \n" + 
				"where sround = ?";
				
			pst = conn.prepareStatement(sql);
			pst.setString(1, current_Info);
			pst.setString(2, sRound);
			pst.executeUpdate();
			//conn.commit();
			pst.close();
		}catch(Exception e){
			e.printStackTrace();
		}finally{  
	        DBUtils.closeConnection(conn);  
	    }
	}
	public void updateStatusForEnd(String sRound,int estimate_filecount,int collect_filecount,String current_info,String isEnd){
		PreparedStatement pst = null;
		Connection conn = null;
		DateFormat dfMRLog = new SimpleDateFormat("yyyyMMdd");
		//String sdate = "";
		try {
			conn = DBUtils.openConnection();
			if("1".equalsIgnoreCase(isEnd)){
				String sql = 
					"update FAST_FTP_COLLECT_custom t\n" +
					"set current_status = '4'\n" + 
					",collect_date_end = sysdate\n" + 
					",ESTIMATE_FILECOUNT = ? \n"+
					",collect_filecount = ? \n"+
					",current_info = ? \n"+
					",update_date = sysdate\n" + 
					"where sround = ?";
					
				pst = conn.prepareStatement(sql);
				pst.setInt(1, estimate_filecount);
				pst.setInt(2, collect_filecount);
				pst.setString(3, current_info);
				pst.setString(4, sRound);
			}
			else{
				String sql = 
						"update FAST_FTP_COLLECT_custom t\n" +
						"set current_status = '3'\n" + 
						",ESTIMATE_FILECOUNT = ? \n"+
						",collect_filecount = ? \n"+
						",update_date = sysdate\n" + 
						",current_info = ? \n"+
						"where sround = ?";
						
					pst = conn.prepareStatement(sql);
					pst.setInt(1, estimate_filecount);
					pst.setInt(2, collect_filecount);
					pst.setString(3, current_info);
					pst.setString(4, sRound);
			}
			pst.executeUpdate();
			//conn.commit();
			pst.close();
		}catch(Exception e){
			e.printStackTrace();
		}finally{  
	        DBUtils.closeConnection(conn);  
	    }
	}
	public void updateStatus(String sRound,String current_info,String current_status){
		PreparedStatement pst = null;
		Connection conn = null;
		try {
			conn = DBUtils.openConnection();
			String sql = 
				"update FAST_FTP_COLLECT_custom t\n" +
				"set current_status = ? \n" + 
				",current_info = ? \n"+
				",update_date = sysdate\n" + 
				"where sround = ?";
				
			pst = conn.prepareStatement(sql);
			pst.setString(1, current_status);
			pst.setString(2, current_info);
			pst.setString(3, sRound);
			pst.executeUpdate();
			//conn.commit();
			pst.close();
		}catch(Exception e){
			e.printStackTrace();
		}finally{  
	        DBUtils.closeConnection(conn);  
	    }
	}
	public void updateInfo(String sRound,String info,String current_status){
		PreparedStatement pst = null;
		Connection conn = null;
		//String sdate = "";
		try {
			conn = DBUtils.openConnection();
			String sql = 
					"update FAST_FTP_COLLECT_custom t\n" +
					"set current_info = ? \n"+
					",update_date = sysdate\n" + 
					",current_status = ? \n"+
					"where sround = ?";
					
			pst = conn.prepareStatement(sql);
			pst.setString(1, info);
			pst.setString(2, current_status);
			pst.setString(3, sRound);
			pst.executeUpdate();
			//conn.commit();
			pst.close();
		}catch(Exception e){
			e.printStackTrace();
		}finally{  
	        DBUtils.closeConnection(conn);  
	    }
	}
	public void insertBatch(String sround,String ip,int port,HashMap<String, Integer> ftpFilesMap) {
		
		PreparedStatement pst = null;
		Connection conn = null;
		int insertBath = 0;
		int insertCount = 0;
		String fileDate = null;
		DateFormat dfMRLog = new SimpleDateFormat("yyyyMMdd");
		try {
			conn = DBUtils.openConnection();
			Long begin = new Date().getTime();
			String sql = "insert into FAST_FTP_COLLECT_PRESERVE \n"
						+"(sdate, ip, port, enbid, mr_type, file_name, file_path_base,FAIL_COUNT,sround,shour)"
						+ "values(?,?,?,?,?,?,?,?,?,?)";
			conn.setAutoCommit(false);
			pst = conn.prepareStatement(sql);
			Set<Entry<String,Integer>> ftpSet = ftpFilesMap.entrySet();
			for(Iterator<Map.Entry<String,Integer>> iterator = ftpSet.iterator();iterator.hasNext();)
			{	insertBath ++;
				insertCount ++;
				Map.Entry<String,Integer> entry = (Map.Entry<String, Integer>) iterator.next();
				String vfileNamePaths = entry.getKey();
				int failCount = entry.getValue().intValue();
				String file_PathBase = null;
				String fileName = null;
				if(vfileNamePaths.contains(",")){
					String[] vfileNamePathList = vfileNamePaths.split(",");
					file_PathBase = vfileNamePathList[0];
					fileName = vfileNamePathList[1];
				}
				
				fileDate = Utils.extractDate8fromStr(fileName,"");
				file_PathBase = Utils.getlinuxPath(file_PathBase,"1");
			
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
				pst.setInt(8, failCount);
				pst.setString(9, sround);
				pst.setString(10, shour);
				
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
					"merge into FAST_FTP_COLLECT_CUSTOM_DETAIL partition(P_{PARTDATA}) a\n" +
					"using(select * from FAST_FTP_COLLECT_PRESERVE)b\n" + 
					"on(a.sdate = to_date(?,'yyyymmdd')\n"
					+ " and a.sdate = b.sdate \n" + 
					"and a.ip = b.ip\n" + 
					"and a.port=b.port\n" + 
					"and a.FILE_NAME = b.FILE_NAME\n" + 
					"and a.FILE_PATH_BASE = b.FILE_PATH_BASE\n" +
					")\n" + 
					"when matched then update set FAIL_COUNT = case when b.FAIL_COUNT = -1 then -1 else FAIL_COUNT + 1 end\n"+
					"when not matched then insert (sdate, ip, port, enbid, mr_type, file_name, file_path_base,FAIL_COUNT,sround,shour)\n" + 
					"values(b.sdate, b.ip, b.port, b.enbid, b.mr_type, b.file_name, b.file_path_base,b.FAIL_COUNT,b.sround,b.shour)";
				sql = sql.replace("{PARTDATA}",fileDate);
				
				pst = conn.prepareStatement(sql);
				pst.setString(1, fileDate);
				pst.executeUpdate();
				conn.commit();
				pst.close();
			
			Long end = new Date().getTime();
			//System.out.println("入采集日志:"+insertCount+"\t 耗时 : " + (end - begin) / 1000 + " s");
		}catch(Exception e){
			e.printStackTrace();
		}finally{  
	        DBUtils.closeConnection(conn);  
	    }
	}
	
	 public Map<String,String> initEnbidListSiteInfo() throws Exception{

		  Connection conn = null;  
		    PreparedStatement pst = null;
		    Map<String,String> enbidMap = new HashMap<String,String>();
		    String sql = " select distinct CITY,ENBID from CFG_SITEINFO_TDLTE ";
		    try {
		        conn = DBUtils.openConnection();
		        pst = conn.prepareStatement(sql);
		        ResultSet rs = pst.executeQuery();
		        
		        String city = null;
		        String enbid = null;
		        while(rs.next()){
		        	city = rs.getString("CITY");
		        	enbid = rs.getString("ENBID");
		        	enbidMap.put(enbid, city);
		        }
		    } catch (Exception e) {
		        e.printStackTrace();
		        throw new Exception("数据访问异常");
		    }finally{
		        DBUtils.closeConnection(conn);
		    }
		    return enbidMap;

	  }
}
