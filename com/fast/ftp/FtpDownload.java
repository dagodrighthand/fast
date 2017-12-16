package com.fast.ftp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

import com.fast.common.Utils;
import com.jcraft.jsch.SftpException;

public abstract class FtpDownload {

	public String loggin_done_flg = "0";
	public int dataTimeoutSecond = 10;

	public abstract boolean ftpLogin() throws SocketException, IOException;

	public abstract void ftpLogOut();

	@SuppressWarnings("unused")
	public boolean downloadFileByStream(InputStream inputStream,
			String localDir, String remoteFileName,
			Set<String> remoteSubFileNameSet
			,Map<String,String> enbidMap
			) {
		String localDirFinal = null;
		String strFilePath = null;
		boolean exeStatus = false;
		if (remoteFileName.toLowerCase().endsWith(".tar.gz")) {
			GZIPInputStream is;
			try {
				is = new GZIPInputStream(new BufferedInputStream(inputStream));
				
				ArchiveInputStream in = new ArchiveStreamFactory()
						.createArchiveInputStream("tar", is);
				TarArchiveEntry entry = (TarArchiveEntry) in.getNextEntry();

				while (entry != null) {
					String name = entry.getName();
					for (String subFileName : remoteSubFileNameSet) {
						if (name.contains(subFileName)) {
							String enbid = Utils.extractEnbidfromFileName(subFileName, "");
							String city = enbidMap.get(enbid);
							if(city == null || "".equalsIgnoreCase(city)){city = "TEMP";}
							localDirFinal = localDir.replace("{CITY}", city);
							
							strFilePath = localDirFinal + "/" + subFileName;
							File file = new File(localDirFinal);
			             	if (!checkFileExist(localDirFinal)) {
			             	    file.mkdirs();
			             	}
			             	
			             	file = new File(strFilePath);
			             	
			             	if(!file.exists()){
			             		file.createNewFile();
			             	}
			             	
			             	
							BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
									new FileOutputStream(file));

							byte[] buf = new byte[4096];
							int len;
							try {
								while ((len = in.read(buf)) > 0) {
									bufferedOutputStream.write(buf, 0, len);
								}
								exeStatus = true;
							} catch (Exception e) {
								e.printStackTrace();
							}
							try {
								bufferedOutputStream.flush();
								bufferedOutputStream.close();
							} catch (Exception e1) {
								e1.printStackTrace();
							}
							break;
						}
					}

					try {
						entry = (TarArchiveEntry) in.getNextEntry();
					} catch (Exception e1) {
						e1.printStackTrace();
						break;
					}
				}
				try {
					in.close();
					is.close();
				} catch (Exception e1) {
					e1.printStackTrace();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		} else if (remoteFileName.toLowerCase().endsWith(".zip")) {
			ZipInputStream Zin = new ZipInputStream(inputStream);
			ZipEntry entry;
			try {
				while ((entry = Zin.getNextEntry()) != null) {
					String name = entry.getName();
					for (String subFileName : remoteSubFileNameSet) {
						if (name.contains(subFileName)) {
							
							String enbid = Utils.extractEnbidfromFileName(subFileName, "");
							String city = enbidMap.get(enbid);
							if(city == null || "".equalsIgnoreCase(city)){city = "TEMP";}
							localDirFinal = localDir.replace("{CITY}", city);
							
							strFilePath = localDirFinal + "/" + subFileName;

							File file = new File(localDirFinal);
			             	if (!checkFileExist(localDirFinal)) {
			             	    file.mkdirs();
			             	}
			             	
			             	file = new File(strFilePath);
			             	
			             	if(!file.exists()){
			             		file.createNewFile();
			             	}
			             	
							FileOutputStream out = new FileOutputStream(file);//
							BufferedOutputStream Bout = new BufferedOutputStream(
									out);
							int b;
							
							byte[] buf = new byte[4096];
							int len;
							
							try {
								/*while ((b = Zin.read()) != -1) {
									Bout.write(b);
								}*/
								while ((len = Zin.read(buf)) > 0) {
									Bout.write(buf, 0, len);
								}
								
								exeStatus = true;
							} catch (Exception e) {
								e.printStackTrace();
							} finally {
								Bout.close();
								out.close();
							}
							break;
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				Zin.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return exeStatus;
	}

	public abstract String downloadFile(String remoteFileName,
			String localDires, String remoteDownLoadPath);

	public abstract String downloadFile(String remoteFileName,
			Set<String> remoteSubFileNameSet, String remoteDownLoadPath,
			String localDir
			,Map<String,String> enbidMap) throws IOException, SftpException;

	// public abstract void readFoldFiles(HashMap<String, Long> ftpFilesMap,
	// String remoteDirectory) throws Exception;
	public abstract void ListFiles_Recursive(
			HashMap<String, Long> filePathHashMap, String remoteDirectory,
			String fileDate) throws Exception;

	public abstract boolean readBigZipMap(HashMap<String, Long> ftpFilesMap,
			String remoteFileName, String remoteDownLoadPath,
			Pattern pattern_filter);

	/*
	 * public abstract void getFolders(HashSet<String> alFileDirList, String
	 * remoteDirectory,int folderLevel,int folderLevelFact);
	 * 
	 * public abstract void getBigFiles(HashSet<String> alFileDirList, String
	 * remoteDirectory,int folderLevel,int folderLevelFact);
	 */
	public abstract boolean tryconnect();

	public abstract boolean ftpLoginTimeout();

	public abstract boolean isExistsSrcPath(String iSrcPath);

	public abstract boolean connectstatus();

	public abstract boolean uploadFile(File localFile, String romotUpLoadePath)
			throws Exception;

	public abstract void getDirectorySize(String remoteDirectory,
			List<Long> filesizelist);

	public abstract boolean removeFile(String remoteFileName);

	public abstract void createDone(String path);
	
	 public boolean checkFileExist(String localPath) {
		  File file = new File(localPath);
		  return file.exists();
	 }
}
