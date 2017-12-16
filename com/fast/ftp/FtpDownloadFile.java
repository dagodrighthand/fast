package com.fast.ftp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.springframework.util.FileCopyUtils;

import com.fast.common.*;

//import org.apache.log4j.Logger;  
/*
 * update by: gdp
 * update date:20150818
 * version:1.0
 * 
 */
public class FtpDownloadFile extends FtpDownload  implements Runnable{
	private static DateFormat dfLog = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private FTPClient ftpClient;
    private String strIp;
    private int intPort;
    private String user;
    private String password;
    public Date sThreadDate_start  =  new Date();
    public ThreadPoolExecutor threadPool_FtpLogin;
    public boolean boolean_login = false;
    public Thread sThreadCurrent;
    public String PassiveMode;
    public static int vxx = 0;
    //private int FileCnt;

//    private static Logger logger = Logger.getLogger(Ftp.class.getName());  
  
    /* * 
     * Ftp构造函数 
     */  
    public FtpDownloadFile(String strIp, int intPort, String user, String Password,String PassiveMode,int idataTimeoutSecond) {  
        //this.FileCnt = 0;
    	this.strIp = strIp;  
        this.intPort = intPort;  
        this.user = user;  
        this.password = Password;  
        this.PassiveMode = PassiveMode;
        super.dataTimeoutSecond = idataTimeoutSecond;
        this.ftpClient = new FTPClient();

    }
    public boolean ftpLoginTimeout() {
    	threadPool_FtpLogin = new ThreadPoolExecutor(1, 100000, 0, TimeUnit.MILLISECONDS,
	            new ArrayBlockingQueue<Runnable>(10));
    	threadPool_FtpLogin.execute(this);
    	threadPool_FtpLogin.shutdown();
    	
    	while(true){
			if(loggin_done_flg.equalsIgnoreCase("0")){
				Date scurrentDate = new Date();
				double vsecond = (double)((scurrentDate.getTime() - sThreadDate_start.getTime())/(1000));
				try{
					if(vsecond > 10){
						sThreadCurrent.interrupt();
						ftpLogOut();
						break;
					}
					Thread.sleep(3*1000);
				}catch(Exception e){
					e.printStackTrace();
					break;
				}
			}
			else{
	    		break;
	    	}
		}
    	return boolean_login;
    }
    public void run(){
    	sThreadDate_start = new Date();
    	sThreadCurrent = Thread.currentThread();
    	boolean_login = ftpLogin();
    	if(boolean_login == true){
    		loggin_done_flg = "1";
    	}
    }
    /** 
     * @return 判断是否登入成功 
     * */  
    private long keepAliveTimeout = 10;//等待发送，超时时间 300 set timeout to 5 minutes
    private int controlKeepAliveReplyTimeout = 10;//等待应答时间
    
    public boolean ftpLogin() {
    	//vxx ++;
    	//System.out.println("第"+vxx+" 次请求,loggin_done_flg:"+loggin_done_flg+" objectid:"+this.toString());
    	//if(loggin_done_flg.equalsIgnoreCase("1")){return true;}
        boolean isLogin = false;  
        FTPClientConfig ftpClientConfig = new FTPClientConfig();
        ftpClientConfig.setServerTimeZoneId(TimeZone.getDefault().getID());
        this.ftpClient.setControlEncoding("UTF-8");
        this.ftpClient.configure(ftpClientConfig);
        
        try {
            if (this.intPort > 0) {  
                this.ftpClient.connect(this.strIp, this.intPort);
            } else {  
                this.ftpClient.connect(this.strIp);  
            } 
            
            // FTP服务器连接回答  
            int reply = this.ftpClient.getReplyCode();  
            if (!FTPReply.isPositiveCompletion(reply)) {  
                this.ftpClient.disconnect();  
                System.out.println("FTP 服务器:Ip:"+strIp+",Port:"+intPort+" login fail! 请确认该连接问题");
                return isLogin;  
            }  
            
           /* ftpClient.setDataTimeout(6000000);
            ftpClient.setConnectTimeout(6000000);
            ftpClient.setControlKeepAliveTimeout(keepAliveTimeout);  
            ftpClient.setControlKeepAliveReplyTimeout(controlKeepAliveReplyTimeout);  */

            if(this.ftpClient.login(this.user, this.password) == false){
            	System.out.println("FTP 服务器:Ip:"+strIp+",Port:"+intPort+" login fail! 请确认该连接问题");
            	return isLogin;
            };  
            
            this.ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE); 
            this.ftpClient.setUseEPSVwithIPv4(true);
            // 设置传输协议  
            if(PassiveMode.equalsIgnoreCase("TRUE")){
	            this.ftpClient.sendCommand("PASV");
	            this.ftpClient.enterLocalPassiveMode();
            }
            
            this.ftpClient.setFileTransferMode(FTPClient.STREAM_TRANSFER_MODE);
            
            //FTPClientConfig conf = new FTPClientConfig("UNIX");     
            //this.ftpClient.configure(conf);
            //this.ftpClient.sendCommand("OPTS UTF8 ON");
            //this.ftpClient.setStrictMultilineParsing(true);
//            logger.info("恭喜" + this.user + "成功登陆FTP服务器");
            isLogin = true;
        	loggin_done_flg = "1";
        } catch (Exception e){
        	e.printStackTrace();
        	isLogin = false;
        	loggin_done_flg = "0";
//            logger.error(this.user + "登录FTP服务失败！" + e.getMessage());  
        }  
        this.ftpClient.setBufferSize(3*1024*1024);  
        this.ftpClient.setDataTimeout(dataTimeoutSecond * 1000);
        this.ftpClient.setConnectTimeout(99000000);
        
        return isLogin;  
    }  
  
    /** 
     * @退出关闭服务器链接 
     * */  
    public void ftpLogOut() {  
        if (null != this.ftpClient && this.ftpClient.isConnected()) {
            try {
            	this.ftpClient.logout();
            } catch (Exception e) {
            	
            }

            try {
                this.ftpClient.disconnect();// 关闭FTP服务器的连接  
            } catch (Exception e) {  
            	//e.printStackTrace();
            	//e.printStackTrace();
//                    logger.warn("关闭FTP服务器的连接异常！");  
            }  
        }  
    }  
    /*** 
     * 上传Ftp文件 
     * @param localFile 当地文件 
     * @param romotUpLoadePath上传服务器路径 - 应该以/结束 
     * */  
    public boolean uploadFile(File localFile, String romotUpLoadePath) throws Exception {  
        BufferedInputStream inStream = null;  
        boolean success = false;  
        if(!isExistsSrcPath(romotUpLoadePath)){
    		this.ftpClient.makeDirectory(romotUpLoadePath);
    	}

        this.ftpClient.changeWorkingDirectory(romotUpLoadePath);// 改变工作路径  
        inStream = new BufferedInputStream(new FileInputStream(localFile));  
//        logger.info(localFile.getName() + "开始上传.....");

        String rmName = null;
        if(localFile.getName().contains(".")){
        	rmName = localFile.getName().replaceFirst("\\.", "_"+localFile.getParentFile().getName()+".");
        }
        else{
        	rmName = localFile.getName();
        }
        success = this.ftpClient.storeFile(rmName, inStream);  
        
        if (success == true) {  
//            logger.info(localFile.getName() + "上传成功");
            return success;  
        }
        if (inStream != null) {  
        	inStream.close();
        	ftpClient.completePendingCommand();
        }
        
        return success;  
    }  

    /*** 
     * 下载文件 
     * @param remoteFileName   待下载文件名称 
     * @param localDires 下载到当地那个路径下 
     * @param remoteDownLoadPath remoteFileName所在的路径 
     * */  
  
    public String downloadFile(String remoteFileName, String localDires,  
            String remoteDownLoadPath) {  
        String strFilePath = localDires + "/" + remoteFileName;  
        BufferedOutputStream outStream = null;
        InputStream inputStream = null;
        String success = "-1";
        try {
        	//this.ftpClient.enterLocalPassiveMode();
        	/*if(!remoteDownLoadPath.equalsIgnoreCase(remoteDirectory_pre)){
        		this.ftpClient.changeWorkingDirectory(remoteDownLoadPath);
        	}*/
        	this.ftpClient.changeWorkingDirectory(remoteDownLoadPath);
            
            inputStream =  this.ftpClient.retrieveFileStream(remoteFileName);
            
            if(inputStream == null){
            	success = "-2";
            }
            else{
            	File file = new File(localDires);
             	if (!checkFileExist(localDires)) {
             	    file.mkdirs();
             	}
             	
             	outStream = new BufferedOutputStream(new FileOutputStream(strFilePath));  
                //STREAM_TRANSFER_MODE
	            FileCopyUtils.copy(inputStream, outStream);
            }
            /////////////////////////
            //IOUtil.copyCompletely(inputStream, output);
            //output.close();

           // success = this.ftpClient.retrieveFileStream(remoteFileName, outStream);  
            //if (success == true) {  
//                logger.info(remoteFileName + "成功下载到" + strFilePath);  
             //   return success;  
           // }
           
            
        } catch (Exception e){
        	success = "1";
			e.printStackTrace();
		}
        finally {
        	if (null != inputStream) {  
                try {
                	inputStream.close();
                	ftpClient.completePendingCommand();
                } catch (Exception e) {
                	e.printStackTrace();
                }
                
            }
            if (null != outStream) {  
                try {
                    outStream.flush();  
                    outStream.close();  
                } catch (Exception e) {
                	//e.printStackTrace();
                }
            }
        }  
        return success;  
    }


    public String downloadFile( String remoteFileName
					    		,Set<String> remoteSubFileNameSet
					    		,String remoteDownLoadPath
					    		,String localDir
					    		,Map<String,String> enbidMap
								) throws IOException
	{
    	String success = "-1";
    	String strFilePath = null;
        InputStream inputStream = null;
    	this.ftpClient.changeWorkingDirectory(remoteDownLoadPath);
        inputStream = this.ftpClient.retrieveFileStream(remoteFileName);
        
        if(inputStream == null){
        	success = "-2";
        }
        else{
	        boolean execStatus = downloadFileByStream(inputStream,localDir,remoteFileName,remoteSubFileNameSet,enbidMap);
	        
	        if(!execStatus){
	        	success = "1";
	        }
	        if(inputStream != null){
	    		try{
	    			inputStream.close();
	    			//ftpClient.completePendingCommand();
	    		}catch(Exception e){
	    			e.printStackTrace();
	    		}
	        }
			
        }
		
		return success;
	}
    
	public void createDone(String path){
	    try {
	    	File file = new File("DONE"); 
	    	if(!file.exists()){
	    		file.createNewFile();
	    	}
	    	try{
	    	uploadFile(file,path);
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}
    public boolean removeFile(String remoteFileName
    						   )
    {
    	boolean vDelstatus = false;
	    try {
	    	//ftpClient.changeWorkingDirectory(remoteDownLoadPath);
			ftpClient.deleteFile("dele " + remoteFileName + "\r\n");
			vDelstatus = true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return vDelstatus;

    }
    /*** 
     * @上传文件夹 
     * @param localDirectory 
     *            当地文件夹 
     * @param remoteDirectoryPath 
     *            Ftp 服务器路径 以目录"/"结束 
     * */  
   /* public boolean uploadDirectory(String localDirectory,  
            String remoteDirectoryPath) {  
        File src = new File(localDirectory);  
        try {  
            remoteDirectoryPath = remoteDirectoryPath + src.getName() + "/";  
            this.ftpClient.makeDirectory(remoteDirectoryPath);  
            // ftpClient.listDirectories();  
        } catch (Exception e) {  
        	e.printStackTrace();
//            logger.info(remoteDirectoryPath + "目录创建失败");  
        }  
        File[] allFile = src.listFiles();  
        for (int currentFile = 0; currentFile < allFile.length; currentFile++) {  
            if (!allFile[currentFile].isDirectory()) {  
                String srcName = allFile[currentFile].getPath().toString();  
                uploadFile(new File(srcName), remoteDirectoryPath);  
            }  
        }  
        for (int currentFile = 0; currentFile < allFile.length; currentFile++) {  
            if (allFile[currentFile].isDirectory()) {  
                // 递归  
                uploadDirectory(allFile[currentFile].getPath().toString(),  
                        remoteDirectoryPath);  
            }  
        }  
        return true;  
    }  
  */

	public static boolean deleteFile(String sPath) {  
	    boolean flag = false;  
	    File fFile = new File(sPath);  
	    // 路径为文件且不为空则进行删除  
	    if (fFile.isFile() && fFile.exists()) {  
	    	fFile.delete();  
	        flag = true;  
	    }  
	    return flag;  
	}
	
	public static boolean deleteDirectory(String DelePath){
		boolean flag = false;
		File fDelePath =new File(DelePath);
	    if (!fDelePath.exists() || !fDelePath.isDirectory()) {  
	        return flag;  
	    }  
		File[] fsDeleFileName = fDelePath.listFiles();
		for (int i = 0; i < fsDeleFileName.length; i++){
			if (fsDeleFileName[i].isFile()){
				flag = deleteFile(fsDeleFileName[i].getAbsolutePath());
			}
			else if (fsDeleFileName[i].isDirectory()){
				flag = deleteDirectory(fsDeleFileName[i].getAbsolutePath());
			}
		}
		fDelePath.delete();
		return flag;
	}
	
	 public boolean tryconnect(){
		 ftpLogOut();
		 boolean flogin = ftpLoginTimeout();
		 return flogin;
	 }
	 
	 public boolean connectstatus(){
		 boolean connectstatus = false;
		 int vnoop = 0;
		 try{
			 vnoop = ftpClient.noop();
			 connectstatus = true;
		 }catch(Exception e){
			 connectstatus = false;
	         e.printStackTrace();
	         System.out.println("[报警:FTP服务器连接失效] IP:" +this.strIp+" PORT:"+this.intPort+" USER:"+this.user+", 自动重新请求新连接!");
		 }
		 return connectstatus;
	 }
	 
	public static void Delay(int secCnt){  
		long lSleepStart, lSleepEnd;
		lSleepStart = System.currentTimeMillis();
		/*while(true){
			lSleepEnd = System.currentTimeMillis();
			if (lSleepEnd - lSleepStart > secCnt*1000)
				break;
		}*/
		try {
			Thread.sleep(secCnt*1000L);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return;
	}
	/*** 
     * @下载文件夹下多个文件
     * @param localDirectoryPath本地地址 
     * @param remoteDirectory 远程文件夹 
     * @param filterStr 过滤指定日期+小时的文件
     * */  
  

    // FtpClient的Set 和 Get 函数  
    public FTPClient getFtpClient() {  
        return ftpClient;  
    }  
    public void setFtpClient(FTPClient ftpClient) {  
        this.ftpClient = ftpClient;  
    }
    
    public void ListFiles_Recursive(HashMap<String,Long> filePathHashMap
									,String remoteDirectory
									/*,Pattern pattern_filter*/
									,String fileDate) throws Exception {
    	
		String vremoteDirectory =  remoteDirectory;
		//支持递归
		String sFileName = "";
		
		FTPFile[] allFile = this.ftpClient.listFiles(vremoteDirectory);
		for (int iCurrentFile = 0; iCurrentFile < allFile.length; iCurrentFile++){
			sFileName = allFile[iCurrentFile].getName();
			String tempstr = remoteDirectory+"/"+sFileName;
			if (allFile[iCurrentFile].isFile()){
				/*Matcher matcher = pattern_filter.matcher(sFileName.toUpperCase());
            	if (matcher.find()){
            		tempstr = Utils.getlinuxPath(tempstr,"1");
            		filePathHashMap.put(tempstr,allFile[iCurrentFile].getSize());
            	}*/
				if(sFileName.contains(fileDate)){
					tempstr = Utils.getlinuxPath(tempstr,"1");
	        		filePathHashMap.put(tempstr,allFile[iCurrentFile].getSize());
				}
			}
			else if(allFile[iCurrentFile].isDirectory()){
				ListFiles_Recursive(filePathHashMap,tempstr,fileDate);
			}
		}
    }
    
    //输入文件夹
    //输出文件夹下所有明细文件 包含大包
    public void readFoldFiles(HashMap<String,Long> ftpFilesMap,
				    		String remoteDirectory) throws Exception {
    	String vremoteDirectory =  remoteDirectory;
        //支持递归
    	String sFileName = "";
    	
    	FTPFile[] allFile = this.ftpClient.listFiles(vremoteDirectory);
        for (int iCurrentFile = 0; iCurrentFile < allFile.length; iCurrentFile++){
        	sFileName = allFile[iCurrentFile].getName();
        	String tempstr = remoteDirectory+"/"+sFileName;
        	if (allFile[iCurrentFile].isFile()){
        		
        		tempstr = Utils.getlinuxPath(tempstr,"1");
        		//检查包是否为大包
        		String vEnbid = Utils.extractEnbidfromFileName(sFileName,"");
        		
        		if("".equalsIgnoreCase(vEnbid)){
        			//不带基站号的大包就解压缩
        			if(sFileName.toLowerCase().endsWith(".tar.gz") || sFileName.toLowerCase().endsWith(".zip")){
        				//readBigZipMap(ftpFilesMap,sFileName,remoteDirectory);
        			}
        			else{
        			//不带基站号的非大包
        				ftpFilesMap.put(tempstr, allFile[iCurrentFile].getSize());
        			}
        		}
        		else{
        			ftpFilesMap.put(tempstr, allFile[iCurrentFile].getSize());
        		}
        	}
        	else{
        		readFoldFiles(ftpFilesMap,tempstr);
        	}
        } 
    }
    
    
    //远程解压缩大包
    @SuppressWarnings("finally")
	public boolean readBigZipMap(  HashMap<String,Long> ftpFilesMap
	    						  ,String remoteFileName 
						          ,String remoteDownLoadPath
						          ,Pattern pattern_filter
					            ) 
    {	boolean exec_status = false;
        InputStream inputStream = null;
        try {
        	HashMap<String,Long> ftpFilesMap_tmp = new HashMap<String,Long>();
        	this.ftpClient.changeWorkingDirectory(remoteDownLoadPath);
            inputStream = this.ftpClient.retrieveFileStream(remoteFileName);
            getSmallZipMap(remoteFileName,remoteDownLoadPath,inputStream,ftpFilesMap_tmp,pattern_filter);
            ftpFilesMap.putAll(ftpFilesMap_tmp);
            exec_status = true;
        } catch (Exception e){
        	e.printStackTrace();
		}
        finally {
        	if (null != inputStream) {  
                try {
                	inputStream.close();
                	this.ftpClient.completePendingCommand();
                } catch (Exception e) {
                	e.printStackTrace();
                }
            }
        	return exec_status;
        }  
    }
    
    public void getSmallZipMap(  String remoteFileName
					    		,String remoteDownLoadPath
					    		,InputStream inputStream
					    		,HashMap<String,Long> ftpFilesMap
					    		,Pattern pattern_filter
								)
    {
    	String tempstr = remoteDownLoadPath+"/"+remoteFileName;
    	String tempstr2 = "";
    	
    	if(remoteFileName.toLowerCase().endsWith(".tar.gz")){
    		GZIPInputStream is;
    		try {
    			    is = new GZIPInputStream(new BufferedInputStream(
    				  		inputStream));
    			
    	            ArchiveInputStream in = new ArchiveStreamFactory().createArchiveInputStream("tar", is);
    				TarArchiveEntry entry = (TarArchiveEntry) in.getNextEntry();
    				while (entry != null) {
    					String name = entry.getName();
    					//System.out.println(remoteDownLoadPath+"/"+remoteFileName+"/"+name);
    					tempstr2 = tempstr +"/"+name;
    					Matcher matcher = pattern_filter.matcher(name.toUpperCase());
    	            	if (matcher.find()){
    	            		tempstr = Utils.getlinuxPath(tempstr,"1");
    	            		ftpFilesMap.put(tempstr+";"+tempstr2, entry.getSize());
    	            	}
    					
    					try{
    					    entry = (TarArchiveEntry) in.getNextEntry();
    					}catch(Exception e1){
    						ftpFilesMap.clear();
    						e1.printStackTrace();
    						break;
    					}
    				}
    				try{
	    				in.close();
	    				is.close();
    				}catch(Exception e1){
						//e1.printStackTrace();
					}
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
		}else if(remoteFileName.toLowerCase().endsWith(".zip")){
			ZipInputStream Zin=new ZipInputStream(inputStream);
			ZipEntry entry;
			try {
				while((entry = Zin.getNextEntry())!=null){
					
					String name  = entry.getName();
					tempstr2 = tempstr +"/"+name;
					Matcher matcher = pattern_filter.matcher(name.toUpperCase());
	            	if (matcher.find()){
	            		tempstr = Utils.getlinuxPath(tempstr,"1");
	            		ftpFilesMap.put(tempstr+";"+tempstr2, entry.getSize());
	            	}
				}
			} catch (IOException e) {
				ftpFilesMap.clear();
				e.printStackTrace();
			}
			
		}
    }
    
    //获取文件夹列表
    public void getFolders(HashSet<String> alFileDirList,
    		String remoteDirectory,int folderLevel,int folderLevelFact) {
    	if(folderLevel == 0  && isExistsSrcPath(remoteDirectory)){
    		remoteDirectory = Utils.getlinuxPath(remoteDirectory,"0");
    		alFileDirList.add(remoteDirectory);
    		return;
    	}
    	
    	String vremoteDirectory =  remoteDirectory;
        try {
        	String sFileName = "";
        	String sDirName = "";

        	boolean cwboolean = this.ftpClient.changeWorkingDirectory(vremoteDirectory);
        	if(!cwboolean){
        		return;
        	}
        	FTPFile[] allFile = this.ftpClient.listFiles(vremoteDirectory);
        	folderLevelFact ++;
        	if(allFile == null){return;}
        	
            for (int iCurrentFile = 0; iCurrentFile < allFile.length; iCurrentFile++) {
            	sFileName = allFile[iCurrentFile].getName();
            	if (allFile[iCurrentFile].isDirectory() && (!sFileName.equalsIgnoreCase(".")&& !sFileName.equalsIgnoreCase("..")&&!sFileName.equalsIgnoreCase("/"))){
            		if(folderLevelFact == folderLevel){
            			sDirName = remoteDirectory+"/"+sFileName+"/";
            			sDirName = Utils.getlinuxPath(sDirName,"0");
              		  alFileDirList.add(sDirName);
              		  continue;
              	  }
            		
            		getFolders(alFileDirList,remoteDirectory+"/"+sFileName,folderLevel,folderLevelFact);
            	}
            	/*else if(!allFile[iCurrentFile].isDirectory() ){
              	  alFileDirList.add(remoteDirectory+"/");
              	  break;
                }*/
            } 
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    //获取大文件列表
    public void getBigFiles(HashSet<String> alFileDirList,
    		String remoteDirectory,int folderLevel,int folderLevelFact) {
    	/*if(folderLevel == 0  && isExistsSrcPath(remoteDirectory)){
    		remoteDirectory = Utils.getlinuxPath(remoteDirectory,"0");
    		alFileDirList.add(remoteDirectory);
    		return;
    	}*/
    	if(!isExistsSrcPath(remoteDirectory)){
        	System.out.println(this.strIp+"\t"+remoteDirectory+" not exists");
    		return;
    	}
    	String vremoteDirectory =  remoteDirectory;
        try {
        	String sFileName = "";
        	String sDirName = "";
        	
        	FTPFile[] allFile = this.ftpClient.listFiles(vremoteDirectory);
        	folderLevelFact ++;
        	if(allFile == null){return;}

            for (int iCurrentFile = 0; iCurrentFile < allFile.length; iCurrentFile++) {
            	
            	sFileName = allFile[iCurrentFile].getName();
            	if (allFile[iCurrentFile].isDirectory() && (!sFileName.equalsIgnoreCase(".")&& !sFileName.equalsIgnoreCase("..")&&!sFileName.equalsIgnoreCase("/"))){
            		getBigFiles(alFileDirList,remoteDirectory+"/"+sFileName,folderLevel,folderLevelFact);
            	}
            	else if(allFile[iCurrentFile].isFile() ){
            		//System.out.println(Utils.getlinuxPath(remoteDirectory+"/"+sFileName,"1"));
              	  alFileDirList.add(Utils.getlinuxPath(remoteDirectory+"/"+sFileName,"1"));
                }
            } 
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
 
	@Override
	public boolean isExistsSrcPath(String iSrcPath) {
		// TODO Auto-generated method stub
		boolean cwboolean = false;
		String vsrcPath = iSrcPath.replace("//", "/");
		try {
			cwboolean = this.ftpClient.changeWorkingDirectory(vsrcPath);
			if(!cwboolean){
	    		return cwboolean;
	    	}
		} catch (IOException e) {
			//System.out.println("FTP 服务器:Ip:"+strIp+",Port:"+intPort+" 目录路径不存在:"+iSrcPath);
			e.printStackTrace();
		}
		return cwboolean;
	}
	@Override
	   public void getDirectorySize(String iremoteDirectory,List<Long> filesizelist){
		
		boolean isexist = isExistsSrcPath(iremoteDirectory);
		if(!isexist){return;}
		
			   FTPFile[] allFile = null;
			   //FTPFile ftpf = this.ftpClient.mlistFile(remoteDirectory);
			   if(this.connectstatus()){
				   try {
					allFile = this.ftpClient.listFiles(iremoteDirectory);
				} catch (Exception e) {
					//e.printStackTrace();
				}
			   }
			   else{
				   boolean tcon = this.tryconnect();
				   
				   if(!tcon){
					   System.out.println("Ip:"+this.strIp+" 登录失败!");
				   }
			   }
			   for (int iCurrentFile = 0; iCurrentFile < allFile.length; iCurrentFile++) {
				   if(allFile[iCurrentFile].isFile()){
					   filesizelist.add(allFile[iCurrentFile].getSize());
				   }
				   else{
					   getDirectorySize(iremoteDirectory+"/"+allFile[iCurrentFile].getName(),filesizelist);
				   }
			   }
			   //filesize = ftpf.getSize();
	   }
	public static void main(String[] args) {
		/*FtpDownload fd = new FtpDownload("127.0.0.1", 21, "fast", "fast","true",10);
		System.out.println(fd.ftpLoginTimeout());
		fd.downloadFile("mr_95__2015091618.tar.gz", "/",  
	            "/","");*/
	}

}  