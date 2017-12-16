package com.fast.dao;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DBUtils {
	private volatile static String driver;
	private static String url;
	private static String sip;
	private static String sport;
	public static String sid;
	public static String user;
	public static String password;
	/*public static String user1 = "fastx";
	public static String password1 = "F@st*123";
	private static String user2 = "c##fastx";
	private static String password2 = "F@st*123";*/
	
	public static void connLoadDriver(){
		//Properties props = new Properties();
		try {
			/*props.load(DBUtils.class.getClassLoader().getResourceAsStream(
			"db2.properties"));*/
			driver = "oracle.jdbc.OracleDriver";
			url = "jdbc:oracle:thin:@"+sip+":"+sport+":"+sid;
			
			Class.forName(driver);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	public static Connection openConnection() throws SQLException{
		
		/*if(driver == null){
			connLoadDriver();
		}
		if(url == null){
			connLoadDriver();
		}*/
		connLoadDriver();
		Connection con = null;
		
		con = DriverManager.getConnection(url,user,password);
		return con;
	}
	
	public static void closeConnection(Connection con){
		if(con != null){
			try {
				con.close();
			} catch (SQLException e) {
			}
			
		}
		
	}
	public static void main(String[] args)throws Exception {
		String srg =  "127.0.0.225:56035:fast";
		String[] vargs = srg.split(":");
		
		if(vargs.length != 3){
			System.out.println("参数:"+srg+" 格式不对!"+"\n"
					+ "用法：110.92.190.6:1521:fast");
			return;
		}

		/*boolean issetok = connParaset(vargs[0],vargs[1],vargs[2]);
		if(!issetok){
			return;
		}
		Connection con = openConnection();
		if(con == null){
			return;
		}
		System.out.println(con);*/
	}
	
	public static boolean connParaset(String iip,String iport,String isid,String iuser,String ipassword){
		sip = iip;
		sport = iport;
		sid = isid;
		user = iuser;
		password = ipassword;
		if(!isIpv(sip)){
			System.out.println("IP地址:"+iip+" 格式不正确");
			return false;
		}
		try{
			int vport = Integer.valueOf(sport);
		}catch(Exception e){
			System.out.println("端口号:"+sport+" 无效");
			return false;
		}
		return true;
	}
	
	public static boolean isIpv(String ipAddress) {  
		  
		//正则校验IP合法性  
		Pattern pattern = Pattern.compile("^((\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5]|[*])\\.){3}(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5]|[*])$");  
		if(!pattern.matcher(ipAddress).find()){  
		    return false; 
		}
		else{
			return true;
		}
  
    }  
}