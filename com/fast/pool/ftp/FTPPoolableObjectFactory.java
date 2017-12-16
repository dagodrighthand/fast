package com.fast.pool.ftp;

import org.apache.commons.pool.BasePoolableObjectFactory;

import com.fast.ftp.*;

public class FTPPoolableObjectFactory extends BasePoolableObjectFactory{	
	private String host;
	private int port;
	private String user;
	private String password;
    private String passiveModeConf;
    private String ftptype;
	private int dataTimeoutSecond;
    
    public FTPPoolableObjectFactory(String host,int port,String user,String password,String ftptype,String passiveModeConf, int idataTimeoutSecond){
    	this.host = host;
    	this.port = port;
    	this.user = user;
    	this.password = password;
    	this.passiveModeConf = passiveModeConf;
    	this.ftptype = ftptype;
    	this.dataTimeoutSecond = idataTimeoutSecond;
    }
    
    /** 
     * @param charset �ַ��� 
     * @param lang    ���������Ի��� 
     */  
   /* public void ftpConf(String charset, String lang) {  
    	ftpClient.setControlEncoding(charset);  
        FTPClientConfig conf = new FTPClientConfig(FTPClientConfig.SYST_NT);    
        conf.setServerLanguageCode(lang);  
    }  */
    
    private long keepAliveTimeout = 10;//�ȴ����ͣ���ʱʱ�� 300 set timeout to 5 minutes
    private int controlKeepAliveReplyTimeout = 10;//�ȴ�Ӧ��ʱ��
    private boolean hidden = false;//�Ƿ���ʾ�����������ļ�
    private String msg = "ok";
	@Override
	public Object makeObject() throws Exception {
		 //System.out.println("�������� makeObject "+ftptype);
		FtpDownload ftpDlFiles = null;
		if(ftptype.equalsIgnoreCase("SFTP")){
			ftpDlFiles = new SFtpDownloadFile(host,port,user,password,passiveModeConf,dataTimeoutSecond);
		}
		else if(ftptype.equalsIgnoreCase("FTP")){
			ftpDlFiles = new FtpDownloadFile(host,port,user,password,passiveModeConf,dataTimeoutSecond);
		}
		//ftpDlFiles.ftpLoginTimeout();
		ftpDlFiles.ftpLogin();
        return ftpDlFiles;
	}

	@Override
	public void destroyObject(Object obj) throws Exception {
        //System.out.println("���ն��� destroyObject");
		if(obj instanceof FtpDownload){
			FtpDownload ftpClient=(FtpDownload)obj;
			ftpClient.ftpLogOut();
			ftpClient = null;
			/*if(!ftpClient.isConnected()) return ;
			try{
				ftpClient.logout();
			}catch(Exception e){
				//e.printStackTrace();
			}
			finally{
				ftpClient.disconnect();
			}*/
		}
	}
	@Override
	public boolean validateObject(Object obj) {
		//System.out.println("��֤���� validateObject");
		if(obj instanceof FtpDownload){
			FtpDownload ftpClient=(FtpDownload)obj;
			try {
				return ftpClient.connectstatus();
		      } catch (Exception e) {
		    	  return false;
		      }
		}
		return false;
	}

	@Override
	public void activateObject(Object obj) throws Exception {
		// TODO Auto-generated method stub
		//System.out.println("ȡ���� activateObject");
		super.activateObject(obj);
	}

	@Override
	public void passivateObject(Object obj) throws Exception {
		// TODO Auto-generated method stub
		//System.out.println("������ passivateObject");
		super.passivateObject(obj);
	}
	
}