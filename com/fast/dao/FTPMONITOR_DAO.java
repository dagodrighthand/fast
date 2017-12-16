package com.fast.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public interface FTPMONITOR_DAO {

	public void insertBatch(String ip,int port,HashMap<String,Long> ftpFilesMap);
	public Set<String> queryByIpDate(String ip,int port,String sDate,String isFindTar) throws Exception;
	public void updateStatus(String fileDate,String ip,int port,Set<String> ftpFilesSet);
	public void mergeBatch();
	public void insertMonitorStart(String current_info,int monitor_servers,String monitor_range);
	public void insertMonitorStatus(String current_info,String monitor_range);
	public void insertMonitorLog(String current_info);
}
