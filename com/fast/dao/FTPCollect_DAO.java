package com.fast.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fast.bean.Collect_CustomBean;

public interface FTPCollect_DAO {
	public Collect_CustomBean CreateBeanCollectCustom() throws Exception;
	public Map<String,Set<String>> queryByCollectCustom(Collect_CustomBean customBean) throws Exception;
	public void updateStatusForStart(String sRound,String current_Info);
	public void updateStatusForEnd(String sRound,int estimate_filecount,int collect_filecount,String current_info,String isEnd);
	public void updateInfo(String sRound,String info,String current_status);
	public void insertBatch(String sround,String ip,int port,HashMap<String, Integer> ftpFilesMap);
	public Map<String,String> initEnbidListSiteInfo() throws Exception;
	public void updateStatus(String sRound,String current_info,String current_status);
}
