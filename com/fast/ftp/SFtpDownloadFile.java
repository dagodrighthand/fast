package com.fast.ftp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
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
import org.apache.commons.net.ftp.FTPFile;
import org.omg.CORBA.SystemException;
import org.springframework.util.FileCopyUtils;

import com.fast.common.*;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

//import org.apache.log4j.Logger;  
/*
 * update by: gdp
 * update date:20150818
 * version:1.0
 * 
 */
public class SFtpDownloadFile extends FtpDownload   implements Runnable{  
	
	private static DateFormat dfLog = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	private Session session = null;
	private ChannelSftp channel = null;
	private JSch jsch = null;
    private String strIp;  
	private String PassiveMode;
    private int intPort;
    private String user;  
    private String password;
    public Date sThreadDate_start =  new Date();
    public ThreadPoolExecutor threadPool_FtpLogin; 
    public boolean boolean_login = false;
    public Thread sThreadCurrent;
    //private int FileCnt;
  
//    private static Logger logger = Logger.getLogger(Ftp.class.getName());  

    /* * 
     * Ftp构造函数 
     */  
    public SFtpDownloadFile(String strIp, int intPort, String user, String Password,String PassiveMode,int idataTimeoutSecond) {  
        //this.FileCnt = 0;
    	this.strIp = strIp;  
        this.intPort = intPort;  
        this.user = user;  
        this.password = Password; 
        this.PassiveMode = PassiveMode;
        super.dataTimeoutSecond = idataTimeoutSecond;
        this.jsch = new JSch();
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
    	if(boolean_login){
    	  loggin_done_flg = "1";
    	}
    	
    }
    
    /** 
     * @return 判断是否登入成功 
     * */  
	public boolean ftpLogin() {
		//if(loggin_done_flg.equalsIgnoreCase("1")){return true;}
    	 boolean isLogin = false;  
    	try{
	        session = jsch.getSession(this.user, this.strIp, this.intPort);
	        session.setPassword(this.password); // 设置密码
	        //具体config中需要配置那些内容，请参照sshd服务器的配置文件/etc/ssh/sshd_config的配置
	 	   Properties config = new Properties();
	 	   com.jcraft.jsch.Logger logger = new SettleLogger();
	 	   jsch.setLogger(logger);
	 	   //设置不用检查hostKey
	 	   //如果设置成“yes”，ssh就不会自动把计算机的密匙加入“$HOME/.ssh/known_hosts”文件，
	 	   //并且一旦计算机的密匙发生了变化，就拒绝连接。
	 	   config.put("StrictHostKeyChecking", "no");
	
	 	   //UseDNS指定，sshd的是否应该看远程主机名，检查解析主机名的远程IP地址映射到相同的IP地址。
	 	   //默认值是 “yes” 此处是由于我们SFTP服务器的DNS解析有问题，则把UseDNS设置为“no”
	 	   config.put("UseDNS", "no");
	 	   session.setConfig(config); // 为Session对象设置properties
		   session.setTimeout(dataTimeoutSecond * 1000); // 设置timeout时候
		   session.connect(); // 经由过程Session建树链接
		   channel = (ChannelSftp) session.openChannel("sftp"); // 打开SFTP通道
		   channel.connect(); // 建树SFTP通道的连接
	       isLogin = true;  
	       loggin_done_flg = "1";
        } catch (Exception e) {
        	e.printStackTrace();
        	System.out.println("FTP 服务器:Ip:"+strIp+",Port:"+intPort+" login fail! 请确认该连接问题");
        	isLogin = false;
        	loggin_done_flg = "0";
        }
        return isLogin;  
    }  
  
    /** 
     * @退出关闭服务器链接 
     * */  
    public void ftpLogOut() {  
    	 try {
    		  if(channel != null) {
    		    channel.disconnect();
    		  }
		  } catch (Exception e) {
			  e.printStackTrace();
		  }
    	 
    	 try {
        	 if(session != null) {
      		    session.disconnect();
      		 }
		  } catch (Exception e) {
			  e.printStackTrace();
		  }
    	 

    }  
	/* private boolean checkFileExist(String localPath) {
		  File file = new File(localPath);
		  return file.exists();
		 }*/
	public void createDone(String path){
	    try {
	    	File file = new File("DONE"); 
	    	if(!file.exists()){
	    		file.createNewFile();
	    	}
	    	try{
	    	uploadFile(file,path);
	    	}catch( Exception e){
	    		e.printStackTrace();
	    	}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}
	 public boolean tryconnect(){
		 ftpLogOut();
		 boolean flogin = ftpLoginTimeout();
		 return flogin;
	 }
	 
    /*** 
     * 下载文件 
     * @param remoteFileName   待下载文件名称 
     * @param localDires 下载到当地那个路径下 
     * @param remoteDownLoadPath remoteFileName所在的路径 
     * */  
  
    @SuppressWarnings("finally")
	public String downloadFile(String remoteFileName, String localDires,  
            String remoteDownLoadPath) {
        String strFilePath = localDires + "/"+ remoteFileName;  
        BufferedOutputStream outStream = null;  
        String success = "-1"; 
        File file;
        InputStream inputStream = null;
        try {
        	channel.cd(remoteDownLoadPath);
        	inputStream = channel.get(remoteFileName);
        	
        	if(inputStream == null){
            	success = "-2";
            }
        	else{
      	    //channel.get(remoteFileName, outStream);
            	file = new File(localDires);
             	if (!checkFileExist(localDires)) {
             	    file.mkdirs();
             	}
             	
            	outStream = new BufferedOutputStream(new FileOutputStream(strFilePath));
        		FileCopyUtils.copy(inputStream, outStream);
        	}
        }
        catch (Exception e) {
        	//System.out.println(""+this.strIp+"\t"+remoteDownLoadPath+"\t"+remoteFileName);
        	if(e.getMessage().contains("No such file")){
        		success = "1";
        	}
        	/*else{
        		success = false;
	        	e.printStackTrace();
        	}*/
        	else{
        		success = "1";
        		e.printStackTrace();
        	}
        	
        } finally {
            if (null != outStream) {  
                try {  
                    outStream.flush();  
                    outStream.close();
                    
                } catch (Exception e) {  
                	e.printStackTrace();
                }  
            }
            if (null != inputStream) {  
                try {  
                    inputStream.close();
                    
                } catch (Exception e) {  
                	e.printStackTrace();
                }  
            }
            
        }
        return success; 
        
    }  
    
    public String downloadFile(    String remoteFileName
					    		,Set<String> remoteSubFileNameSet
					    		,String remoteDownLoadPath
					    		,String localDir
					    		,Map<String,String> enbidMap
			) throws IOException, SftpException
	{
    	String success = "-1";
    	channel.cd(remoteDownLoadPath);
    	InputStream inputStream = channel.get(remoteFileName);
    	
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
	    		}catch(Exception e){}
	        }
    	}
    	return success;
    	/*if(remoteFileName.toLowerCase().endsWith(".tar.gz")){
    		GZIPInputStream is;
    		try {
    			    is = new GZIPInputStream(new BufferedInputStream(
    				  		inputStream));
    			
    	            ArchiveInputStream in = new ArchiveStreamFactory().createArchiveInputStream("tar", is);
    				TarArchiveEntry entry = (TarArchiveEntry) in.getNextEntry();
    				while (entry != null) {
    					String name = entry.getName();
    					if(name.contains(remoteSubFileName)){
    						FileOutputStream out=new FileOutputStream(file);
    						BufferedOutputStream Bout=new BufferedOutputStream(out);  
    		                int b;  
    		                try{
    			                while((b=in.read())!=-1){  
    			                    Bout.write(b);
    			                }
    		                }catch(Exception e){
    		                	e.printStackTrace();
    		                }finally{
    			                Bout.close();
    			                out.close();
    		                }
    		                break;
    					}
    					try{
    						entry = (TarArchiveEntry) in.getNextEntry();
    					}catch(Exception e1){
    						e1.printStackTrace();
    						break;
    					}
    				}
    				try{
	    				in.close();
	    				is.close();
    				}catch(Exception e1){
						e1.printStackTrace();
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
					if(name.contains(remoteSubFileName)){
						FileOutputStream out=new FileOutputStream(file);
						BufferedOutputStream Bout=new BufferedOutputStream(out);  
		                int b;  
		                try{
			                while((b=Zin.read())!=-1){  
			                    Bout.write(b);
			                }
		                }catch(Exception e){
		                	e.printStackTrace();
		                }finally{
			                Bout.close();
			                out.close();
		                }
		                break;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
    	return true;*/
	}
    public void ListFiles_Recursive(HashMap<String,Long> filePathHashMap
									,String remoteDirectory
									/*,Pattern pattern_filter*/
									,String fileDate) throws Exception {
    	String sFileName = "";
    	
    	Vector<LsEntry> vctfile = this.listFiles(remoteDirectory);
    	if(vctfile == null){ return;}
    	Iterator<LsEntry> sftpFileNames = vctfile.iterator(); 
    	  LsEntry isEntity;
          while (sftpFileNames.hasNext()) 
          {
              isEntity = (LsEntry) sftpFileNames.next(); 
              sFileName = isEntity.getFilename();
              String tempstr = remoteDirectory+"/"+sFileName;
              if(!isEntity.getLongname().startsWith("d")){
            	/*Matcher matcher = pattern_filter.matcher(sFileName.toUpperCase());
            	if (matcher.find()){
	          		tempstr = Utils.getlinuxPath(tempstr,"1");
	          		filePathHashMap.put(tempstr,isEntity.getAttrs().getSize());
            	}*/
            	  if(sFileName.contains(fileDate)){
            	     tempstr = Utils.getlinuxPath(tempstr,"1");
	          		 filePathHashMap.put(tempstr,isEntity.getAttrs().getSize());
            	  }
              }
              else if(isEntity.getLongname().startsWith("d") && (!sFileName.equalsIgnoreCase(".")&& !sFileName.equalsIgnoreCase("..")&&!sFileName.equalsIgnoreCase("/"))){
            	  ListFiles_Recursive(filePathHashMap,tempstr,fileDate);
          	}
          }
    }
    public void readFoldFiles(
    		HashMap<String,Long> ftpFilesMap,
    		String remoteDirectory) throws Exception {
        	String sFileName = "";
        	
        	Vector<LsEntry> vctfile = this.listFiles(remoteDirectory);
        	if(vctfile == null){ return;}
        	Iterator<LsEntry> sftpFileNames = vctfile.iterator(); 
	    	  LsEntry isEntity;
	          while (sftpFileNames.hasNext()) 
	          {
	              isEntity = (LsEntry) sftpFileNames.next(); 
	              sFileName = isEntity.getFilename();
	              String tempstr = remoteDirectory+"/"+sFileName;
	              if(!isEntity.getLongname().startsWith("d")){
	            	  
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
	        				ftpFilesMap.put(tempstr, isEntity.getAttrs().getSize());
	        			}
	        		}
	        		else{
	        			ftpFilesMap.put(tempstr, isEntity.getAttrs().getSize());
	        		}
	              }
	              else{
	          		readFoldFiles(ftpFilesMap,tempstr);
	          	}
	          }
    }
    
    //迭代大包
    @SuppressWarnings("finally")
	public boolean readBigZipMap(HashMap<String,Long> ftpFilesMap
    						  ,String remoteFileName 
					          ,String remoteDownLoadPath
					          ,Pattern pattern_filter
					            ) 
    {   boolean exec_status = false;
        InputStream inputStream = null;
        try {
        	HashMap<String,Long> ftpFilesMap_tmp = new HashMap<String,Long>();
            channel.cd(remoteDownLoadPath);
            inputStream = channel.get(remoteFileName);
            getSmallZipMap(remoteFileName,remoteDownLoadPath,inputStream,ftpFilesMap,pattern_filter);
            ftpFilesMap.putAll(ftpFilesMap_tmp);
            exec_status = true;
        } catch (Exception e){
        	e.printStackTrace();
		}
        finally {
        	if (null != inputStream) {  
                try {
                	inputStream.close();
                } catch (Exception e) {
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
    
    public void getFolders(HashSet<String> alFileDirList,
    		
    		String remoteDirectory,int folderLevel,int folderLevelFact) {
    	if(folderLevel == 0 && isExistsSrcPath(remoteDirectory)){
    		remoteDirectory = Utils.getlinuxPath(remoteDirectory,"0");
    		alFileDirList.add(remoteDirectory);
    		return;
    	}
        try {
        	
        	if(!isExistsSrcPath(remoteDirectory)){
        		return;
        	}
        	
        	String sFileName = "";
        	String sDirName = "";
        	Vector<LsEntry> vctfile = this.listFiles(remoteDirectory);
        	
        	if(vctfile == null){return;}
        	
        	folderLevelFact ++;
        	
        	Iterator<LsEntry> sftpFileNames = vctfile.iterator(); 
        	  LsEntry isEntity;
              while (sftpFileNames.hasNext()) 
              {
                  isEntity = (LsEntry) sftpFileNames.next(); 
                  sFileName = isEntity.getFilename();
                 
                  if(isEntity.getLongname().startsWith("d") && (!sFileName.equalsIgnoreCase(".")&& !sFileName.equalsIgnoreCase("..")&&!sFileName.equalsIgnoreCase("/"))){
                	  if(folderLevelFact == folderLevel){
                		sDirName = remoteDirectory+"/"+sFileName+"/";
              			sDirName = Utils.getlinuxPath(sDirName,"0");
                		  alFileDirList.add(sDirName);
                		  continue;
                	  }
                	  
                	  getFolders(alFileDirList,remoteDirectory+"/"+sFileName,folderLevel,folderLevelFact);
                  }
              }
        	
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
    public void getBigFiles(HashSet<String> alFileDirList,
    		String remoteDirectory,int folderLevel,int folderLevelFact) {
    	if(folderLevel == 0 && isExistsSrcPath(remoteDirectory)){
    		remoteDirectory = Utils.getlinuxPath(remoteDirectory,"0");
    		alFileDirList.add(remoteDirectory);
    		return;
    	}
        try {
        	
        	if(!isExistsSrcPath(remoteDirectory)){
        		return;
        	}
        	
        	String sFileName = "";
        	String sDirName = "";
        	Vector<LsEntry> vctfile = this.listFiles(remoteDirectory);
        	
        	if(vctfile == null){return;}
        	
        	folderLevelFact ++;
        	
        	Iterator<LsEntry> sftpFileNames = vctfile.iterator(); 
        	  LsEntry isEntity;
              while (sftpFileNames.hasNext()) 
              {
                  isEntity = (LsEntry) sftpFileNames.next(); 
                  sFileName = isEntity.getFilename();
                 
                  if(isEntity.getLongname().startsWith("d") && (!sFileName.equalsIgnoreCase(".")&& !sFileName.equalsIgnoreCase("..")&&!sFileName.equalsIgnoreCase("/"))){
                	  /*if(folderLevelFact == folderLevel){
                		sDirName = remoteDirectory+"/"+sFileName+"/";
              			sDirName = Utils.getlinuxPath(sDirName,"0");
                		  alFileDirList.add(sDirName);
                		  continue;
                	  }*/
                	  getBigFiles(alFileDirList,remoteDirectory+"/"+sFileName,folderLevel,folderLevelFact);
                  }
                  else if(!isEntity.getLongname().startsWith("d") &&!sFileName.equalsIgnoreCase(".")&& !sFileName.equalsIgnoreCase("..")&&!sFileName.equalsIgnoreCase("/")){
                	  alFileDirList.add(Utils.getlinuxPath(remoteDirectory+"/"+sFileName,"1"));
                  }
              }
        	
        } catch (Exception e) {
        	e.printStackTrace();
        }  
    }
    
    
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
	 
	public static void Delay(int secCnt){  
		long lSleepStart, lSleepEnd;
		lSleepStart = System.currentTimeMillis();
		/*while(true){
			lSleepEnd = System.currentTimeMillis();
			if (lSleepEnd - lSleepStart > secCnt*1000)
				break;
		}*/
		try {
			Thread.sleep(secCnt*1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return;
	}
	 @SuppressWarnings("unchecked")
	 public Vector<LsEntry> listFiles(String remotePath) throws Exception {
	  Vector<LsEntry> vector = null;
	
		  if(!isExistsSrcPath(remotePath)){
      		return null;
      	}
	   vector = channel.ls(remotePath);
	 
	  return vector;
	 }
	/*** 
     * @下载文件夹下多个文件
     * @param localDirectoryPath本地地址 
     * @param remoteDirectory 远程文件夹 
     * @param filterStr 过滤指定日期+小时的文件
     * */  
	@Override
	public boolean connectstatus(){
		 boolean connectstatus = false;
		 boolean sessionstatus = false;
		 boolean channelstatus = false;
		 try{
			 channelstatus = channel.isConnected();
			 sessionstatus = session.isConnected();
			 if(!channelstatus || !sessionstatus){
				 connectstatus = false;
			 }
			 else{
				 connectstatus = true;
			 }
		 }catch(Exception e){
			 //e.printStackTrace();
			 System.out.println("[报警:FTP服务器连接失效] IP:" +this.strIp+" PORT:"+this.intPort+" USER:"+this.user+", 自动重新请求新连接!");
		 }
		 return connectstatus;
	}

	@Override
	public boolean uploadFile(File localFile, String romotUpLoadePath) throws Exception {
		 BufferedInputStream inStream = null;  
	        boolean success = false;  
        
        	if(!isExistsSrcPath(romotUpLoadePath)){//待修复为何不能确认文件路径存在
        		createDir(romotUpLoadePath);///为何创建失败
        	}
        	
            inStream = new BufferedInputStream(new FileInputStream(localFile));  
            channel.cd(romotUpLoadePath);
            String rmName = null;
            if(localFile.getName().contains(".")){
            	rmName = localFile.getName().replaceFirst("\\.", "_"+localFile.getParentFile().getName()+".");
            }
            else{
            	rmName = localFile.getName();
            }
            channel.put(inStream,rmName);
            success = true;
        
            inStream.close();
            
	        return success;
	}
    public boolean removeFile(String remoteFileName
    						 )
	{
		boolean vDelstatus = false;
		try {
			//channel.cd(remoteDownLoadPath);
			channel.rm(remoteFileName);
			vDelstatus = true;
		} catch (Exception e) {
		// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return vDelstatus;
	
	}
	public void createDir(String createpath) {  
		  try {  
		   if (isExistsSrcPath(createpath)) {  
			   channel.cd(createpath);  
		   }  
		   String pathArry[] = createpath.split("/");  
		   StringBuffer filePath = new StringBuffer("/");  
		   for (String path : pathArry) {  
		    if (path.equals("")) {  
		     continue;  
		    }  
		    filePath.append(path + "/");  
		    if (isExistsSrcPath(filePath.toString())) {  
		    	channel.cd(filePath.toString());  
		    } else {
		     // 建立目录  
		    	channel.mkdir(filePath.toString().replace("//", "/"));
		     // 进入并设置为当前目录  
		    	channel.cd(filePath.toString());  
		    }  
		   }  
		   channel.cd(createpath);  
		  } catch (Exception e) {  
			  e.printStackTrace();
			  System.out.println("创建路径错误：" + createpath);  
		  }  
		 }  
	/** 
	  * 判断目录是否存在 
	  */  
/*	 public boolean isDirExist(String directory) {  
	  boolean isDirExistFlag = false;  
	  try {  
	   SftpATTRS sftpATTRS = channel.lstat(directory);  
	   isDirExistFlag = true;  
	   return sftpATTRS.isDir();  
	  } catch (Exception e) {  
	   if (e.getMessage().toLowerCase().equals("no such file")) {  
	    isDirExistFlag = false;  
	   }  
	  }  
	  return isDirExistFlag;  
	 } */
    public boolean isExistsSrcPath(String iSrcPath){
    	boolean cwboolean = false;
    	String vsrcPath = iSrcPath.replace("//", "/");
    	try{
    		Vector<LsEntry> vctfile = channel.ls(vsrcPath);
    		cwboolean = true;
    	}
    	catch(Exception e){
    		//e.printStackTrace();
    		//System.out.println("FTP 服务器:Ip:"+strIp+",Port:"+intPort+" 目录路径不存在:"+iSrcPath);
    	}
    	return cwboolean;
    }
	@Override
    public void getDirectorySize(String remoteDirectory,List<Long> filesizelist){
		boolean isexist = isExistsSrcPath(remoteDirectory);
		if(!isexist){
			return;
		}
        try{
        	Vector<LsEntry> vctfile = this.listFiles(remoteDirectory);
        	if(vctfile == null){ return;}
        	Iterator<LsEntry> sftpFileNames = vctfile.iterator(); 
        	  LsEntry isEntity;
        	  String sFileName;
              while (sftpFileNames.hasNext()) 
              {
            	 isEntity = (LsEntry) sftpFileNames.next(); 
            	 sFileName = isEntity.getFilename();
                 if(isEntity.getLongname().startsWith("d") && (!sFileName.equalsIgnoreCase(".")&& !sFileName.equalsIgnoreCase("..")&&!sFileName.equalsIgnoreCase("/")))
                 {
                	 getDirectorySize(remoteDirectory+"/"+sFileName,filesizelist);
                 }
                 else{
                     filesizelist.add(isEntity.getAttrs().getSize());
                 }
              }
        	
        } catch (Exception e) {
        	e.printStackTrace();
        }  
 }
}