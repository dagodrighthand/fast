package com.fast.pool.ftp;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

import com.fast.ftp.*;

public class FTPPool extends Pool<FtpDownload>{
	
	public FTPPool(Config poolConfig,String host,int port,String user,String password,String ftptype,String passiveModeConf,int idataTimeoutSecond){
		super(poolConfig, new FTPPoolableObjectFactory(host, port, user, password,ftptype, passiveModeConf,idataTimeoutSecond));
	}
	
}