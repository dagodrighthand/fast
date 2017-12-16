package com.fast.bean;

public class MRServerBean {
	public String MRServerPro;
	public String sIP, sUser, sPwd,sPassiveMode, sSrcPath, sSrcPath_Base, sLocalPath
			, sServerIPPort,sFTPType,sFilterRegExp;
	public int sPort,sFolderLevel;
	
	public String sMonitor_RANGE;
	
	public int getsFolderLevel() {
		return sFolderLevel;
	}
	
	public void setsFolderLevel(int sFolderLevel) {
		this.sFolderLevel = sFolderLevel;
	}
	
	public String getsFilterRegExp() {
		return sFilterRegExp;
	}

	public void setsFilterRegExp(String sFilterRegExp) {
		this.sFilterRegExp = sFilterRegExp;
	}

	public String getsMonitor_RANGE() {
		return sMonitor_RANGE;
	}
	public void setsMonitor_RANGE(String sMonitor_RANGE) {
		this.sMonitor_RANGE = sMonitor_RANGE;
	}
	public MRServerBean(String MRServerPro){
		this.MRServerPro = MRServerPro;
	}
	public String getMRServerPro() {
		return MRServerPro;
	}
	public String getsIP() {
		return sIP;
	}
	public void setsIP(String sIP) {
		this.sIP = sIP;
	}
	public String getsUser() {
		return sUser;
	}
	public void setsUser(String sUser) {
		this.sUser = sUser;
	}
	public String getsPwd() {
		return sPwd;
	}
	public void setsPwd(String sPwd) {
		this.sPwd = sPwd;
	}
	public String getsPassiveMode() {
		return sPassiveMode;
	}
	public void setsPassiveMode(String sPassiveMode) {
		this.sPassiveMode = sPassiveMode;
	}
	public String getsSrcPath() {
		return sSrcPath;
	}
	public void setsSrcPath(String sSrcPath) {
		this.sSrcPath = sSrcPath;
	}
	public String getsSrcPath_Base() {
		return sSrcPath_Base;
	}
	public void setsSrcPath_Base(String sSrcPath_Base) {
		this.sSrcPath_Base = sSrcPath_Base;
	}
	public String getsLocalPath() {
		return sLocalPath;
	}
	public void setsLocalPath(String sLocalPath) {
		this.sLocalPath = sLocalPath;
	}
	public String getsServerIPPort() {
		return sServerIPPort;
	}
	public void setsServerIPPort(String sServerIPPort) {
		this.sServerIPPort = sServerIPPort;
	}
	public String getsFTPType() {
		return sFTPType;
	}
	public void setsFTPType(String sFTPType) {
		this.sFTPType = sFTPType;
	}
	public int getsPort() {
		return sPort;
	}
	public void setsPort(int sPort) {
		this.sPort = sPort;
	}
	
	
}
