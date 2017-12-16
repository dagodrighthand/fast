package com.fast.unzip;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

/*
 * update by: gdp
 * update date:20150818
 * version:1.0
 * 
 */
public class GZip implements Runnable ,Cloneable{

	private BufferedOutputStream bufferedOutputStream;
	
	private boolean executeflg = false;
	private Map<String,String> mapfiles = new HashMap<String,String>();

	public static ConcurrentHashMap<String,Integer> threadstatus = new ConcurrentHashMap<String,Integer>();
	public static ConcurrentHashMap<String,Long> xmlMaps = new ConcurrentHashMap<String,Long>();
	public static ConcurrentHashMap<String,String> invalidentryMaps = new ConcurrentHashMap<String,String>();
	public static ConcurrentHashMap<String,String> threadeNBIDLISTSBYCITY;
	
	String threadstatus_obj;
	public static ThreadPoolExecutor threadPool_parse;
	//static HashMap<String,String> ServerEnbidMaps;
	public GZip(){
		/*threadstatus_obj = getSysRandom();
		threadstatus.put(threadstatus_obj, 0);*/
	}
	
	public void run() {
		Thread th = Thread.currentThread();
		threadstatus_obj = getSysRandom();
		threadstatus.put(threadstatus_obj, 0);
		String zipfileName;
		
		Set<Entry<String,String>> mapgzfileset = mapfiles.entrySet();
		for(Iterator<Map.Entry<String,String>> iterator = mapgzfileset.iterator();iterator.hasNext();)
		{
			//4831
			Map.Entry<String,String> entry = (Map.Entry<String, String>) iterator.next();
			zipfileName = entry.getKey();
			String vfileInfo = entry.getValue();
			
			if(zipfileName.toLowerCase().endsWith(".tar.gz")){
				unzipTARGZ(zipfileName,vfileInfo,"1");
			}else if(zipfileName.toLowerCase().endsWith(".zip")){
				unzipZIP(zipfileName,vfileInfo,"1");
			}
		}
		
		/*for(int i=0;i<listfiles.size();i++){
			zipfileName = listfiles.get(i);
			//System.out.println(listfiles.size()+" "+th.getId()+" "+th.getName()+" "+outputDirectory+" "+zipfileName+" start...");
			if(zipfileName.endsWith(".tar.gz")){
				unzipOarFile(zipfileName);
				File f = new File(zipfileName);
				boolean bl = f.delete();
				//DispatcherMain3.log.info(th.getId()+"delete UnGZip:"+zipfileName+" "+bl);
			}else if(zipfileName.endsWith(".xml.gz")){
				ungzipOarFile(zipfileName);
				File f = new File(zipfileName);
				f.delete();
			}
			if(zipfileName.endsWith(".tar.gz")){
				unzipOarFile(zipfileName);
				//DispatcherMain3.log.info(th.getId()+"delete UnGZip:"+zipfileName+" "+bl);
			}else if(zipfileName.endsWith(".zip")){
				//System.out.println("RUN:"+zipfileName);
				unzipFile(zipfileName);
				File f = new File(zipfileName);
				f.delete();
			}
			//System.out.println(listfiles.size()+" "+th.getId()+" "+th.getName()+" "+outputDirectory+" "+zipfileName+" end");

		}*/
		//System.out.println(th.getId()+" "+th.getName()+" "+outputDirectory+" "+listfiles.size()+" done!");
		
		//DispatcherMainDay.log.info(th.getId()+" UnGZip:"+listfiles.size()+" "+outputDirectory+" suceess!");
		threadstatus.remove(threadstatus_obj);
		threadstatus.put(threadstatus_obj, 1);
	}

	public static void InitThreadPoolExecutor(){
		threadstatus.clear();
		xmlMaps.clear();
		invalidentryMaps.clear();
		threadPool_parse = new ThreadPoolExecutor(30, 150, 0, TimeUnit.MILLISECONDS,
	            new ArrayBlockingQueue<Runnable>(100000));
	}
	
	public static void thutdownThreadPoolExecutor(){
		threadPool_parse.shutdown();
	}
	
	
	public static void GZipFile(HashMap<String,String> mapgzfiles,ConcurrentHashMap<String,String> ithreadeNBIDLISTSBYCITY
			) {
		
		//ExecutorService threadPool = Executors.newFixedThreadPool(256);
		threadeNBIDLISTSBYCITY = ithreadeNBIDLISTSBYCITY;
		//File files = new File(gzfiledir);
		GZip gzip = null;
		long mr_length = 0;
		int j = 0;
		gzip = new GZip();
/*		gzip.threadstatus_obj = getSysRandom();
		threadstatus.put(gzip.threadstatus_obj, 0);*/
		//gzip.outputDirectory = gzfiledir;
		
		Set<Entry<String,String>> mapgzfileset = mapgzfiles.entrySet();
		for(Iterator<Map.Entry<String,String>> iterator = mapgzfileset.iterator();iterator.hasNext();)
		{
			//4831
			Map.Entry<String,String> entry = (Map.Entry<String, String>) iterator.next();
			String vfilename = entry.getKey();
			String vfileInfo = entry.getValue();
			File file = new File(vfilename);
			
			if(!file.exists()){
				continue;
			}
		
			if(!file.getName().endsWith(".xml") && file.length() != 0){
				j++;
				gzip.mapfiles.put(vfilename,vfileInfo);
				mr_length = mr_length + file.length();
				if(j>500 || mr_length >100*1024*1024){
				//new Thread(gzip).start();
					gzip.executeflg = true;
					threadPool_parse.execute(gzip);
				
				j=0;
				mr_length = 0;
				gzip = gzip.clone();
				}
			}

		}
		
		if(gzip.executeflg == false){
			//new Thread(gzip).start();
			threadPool_parse.execute(gzip);
			
		}
	}
	
//zip
	private int unzipZIP(String zipfileName,String ifileInfo,String is_del) {

		int finally_status = 0;
		File ff = new File(zipfileName);
		String outputDirectory = ff.getParent();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(zipfileName);//
			ZipInputStream Zin=new ZipInputStream(fis);
			
			BufferedInputStream Bin=new BufferedInputStream(Zin);  
            File Fout=null;  
            ZipEntry entry;  
            String voutputfileName = ""; 
            String str = "";
            while((entry = Zin.getNextEntry())!=null /*&& !entry.isDirectory()*/){
            	if(entry.getName().toLowerCase().endsWith(".xml")){
            		//is_del = "0";
            		Bin.close();
                    Zin.close();
                    fis.close();
					
					finally_status = 1;
            		break;
            	}
            	str = entry.getName();
            	String vcity = common.Utils.extractCityfrominobid(str,threadeNBIDLISTSBYCITY);
            	String venbid = common.Utils.extractEnbidfromFileName(str,"");
            	
            	if("".equalsIgnoreCase(vcity)){
					voutputfileName = outputDirectory+File.separator+str;
				}
				else{
					voutputfileName = outputDirectory.replace("TEMP", vcity)+File.separator+str;
				}
            	mkFolder(outputDirectory.replace("TEMP", vcity));
            	
                //Fout=new File(voutputfileName);
                
            	if(vcity != null && !"".equalsIgnoreCase(vcity)){
            		mkFolder(outputDirectory.replace("TEMP", vcity));
            	}
                File file = mkFile(voutputfileName);
                /*if(!Fout.exists()){  
                    (new File(Fout.getParent())).mkdirs();  
                } */ 
                FileOutputStream out=new FileOutputStream(file);//
                BufferedOutputStream Bout=new BufferedOutputStream(out);  
                int b;  
                try{
	                while((b=Bin.read())!=-1){  
	                    Bout.write(b);  
	                }
                }catch(Exception e){
                	//is_del = "0";
                	invalidentryMaps.put((zipfileName+"/"+str+" invalid entry!"),"");
                }finally{
	                Bout.close();
	                out.close();
                }
                File fs = new File(voutputfileName);
                if(fs.exists()){
                	xmlMaps.put(ifileInfo+","+voutputfileName.replace("\\", "/"), fs.length());
                	if(str.toLowerCase().endsWith(".tar.gz")){
        				unzipTARGZ(voutputfileName,ifileInfo,"1");
        			}else if(str.toLowerCase().endsWith(".zip")){
        				unzipZIP(voutputfileName,ifileInfo,"1");
        			}
                }
            }  
            Bin.close();
            Zin.close();  
            fis.close();
            if(is_del.equalsIgnoreCase("1")  && finally_status == 0 ){
	            File f = new File(zipfileName);
				f.delete();
            }
		}catch(Exception e){
			e.printStackTrace();
		}
		finally{
			return finally_status;
		}
	}
	
	/*public static void main(String[] args){
		outputDirectory = "D:\\MR_RAW_DAY2\\YICHUN\\";
		unzipFile("D:\\MR_RAW_DAY2\\YICHUN\\TD-LTE_MRS_ZTE_OMC1_20160610234500.zip");
	}*/
//.tar.gz
	private int unzipTARGZ(String zipfileName,String ifileInfo,String is_del) {
		//String is_del = "1";
		File ff = new File(zipfileName);
		String outputDirectory = ff.getParent();
		int finally_status = 0;
		
		FileInputStream fis = null;
		ArchiveInputStream in = null;
		BufferedInputStream bufferedInputStream = null;
		try {
			fis = new FileInputStream(zipfileName);
			GZIPInputStream is = new GZIPInputStream(new BufferedInputStream(
					fis));
			in = new ArchiveStreamFactory().createArchiveInputStream("tar", is);
			bufferedInputStream = new BufferedInputStream(in);
			TarArchiveEntry entry = (TarArchiveEntry) in.getNextEntry();
			while (entry != null) {
				String name = entry.getName();
            	if(entry.getName().toLowerCase().endsWith(".xml")){
            		//is_del = "0";
            		bufferedInputStream.close();
                    in.close();  
                    fis.close();
            		finally_status = 1;
            		break;
            	}

				String[] names = name.split("/");
				String fileName = outputDirectory;
				String str = "";
				for (int i = 0; i < names.length; i++) {
					str = names[i];
					fileName = fileName + File.separator + str;
				}

				//fileName = fileName.replace("TEMP", );
				if (name.endsWith("/")) {
					//mkFolder(fileName);
				} else {
					//System.out.println(str);
					//System.out.println(fileName);
					String vcity = common.Utils.extractCityfrominobid(str,threadeNBIDLISTSBYCITY);
					String voutputfileName = "";
					if("".equalsIgnoreCase(vcity)){
						voutputfileName = outputDirectory+File.separator+str;
					}
					else{
						voutputfileName = outputDirectory.replace("TEMP", vcity)+File.separator+str;
					}
					mkFolder(outputDirectory.replace("TEMP", vcity));
					
					File file = mkFile(voutputfileName);
					bufferedOutputStream = new BufferedOutputStream(
							new FileOutputStream(file));
					int b;
					 byte[] buf = new byte[100240]; 
			            int len; 
			            try{
			            while((len = bufferedInputStream.read(buf)) > 0) { 
			            	bufferedOutputStream.write(buf, 0, len); 
			            } 
			            }catch(Exception e){
			            	invalidentryMaps.put((zipfileName+"/"+str+" invalid entry!"),"");
							//is_del = "0";
							//System.out.println(zipfileName+"/"+str+" invalid entry!");
			            }
			            finally{
			            	bufferedOutputStream.flush();
							bufferedOutputStream.close();
			            }
					
			            File fs = new File(voutputfileName);
		                if(fs.exists()){
		                	xmlMaps.put(ifileInfo+","+voutputfileName.replace("\\", "/"), fs.length());
		                	if(str.toLowerCase().endsWith(".tar.gz")){
		        				unzipTARGZ(voutputfileName,ifileInfo,"1");
		        			}else if(str.toLowerCase().endsWith(".zip")){
		        				unzipZIP(voutputfileName,ifileInfo,"1");
		        			}
		                }
				}
				try{
					entry = (TarArchiveEntry) in.getNextEntry();
				}catch(Exception e){
					//is_del = "1";
					invalidentryMaps.put((zipfileName+"/"+str+" invalid entry!"),"");
					break;
				}
			}
			bufferedInputStream.close();
            in.close();  
            fis.close();
			if(is_del.equalsIgnoreCase("1")  && finally_status == 0){
	            File f = new File(zipfileName);
	            f.delete();
            }
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			return finally_status;
		}

	}
	
	public String getExtension(String f) { 
        String ext = ""; 
        int i = f.lastIndexOf('.'); 

        if (i > 0 &&  i < f.length() - 1) { 
            ext = f.substring(i+1); 
        }      
        return ext; 
    } 
	
	public String getFileName(String f) { 
        String fname = ""; 
        int i = f.lastIndexOf('.'); 

        if (i > 0 &&  i < f.length() - 1) { 
            fname = f.substring(0,i); 
        }      
        return fname; 
    } 
	
	//.xml.gz
	private void ungzipOarFile(String inFileName) {
		try { 

            if (!getExtension(inFileName).equalsIgnoreCase("gz")) { 
            	DispatcherMainDay.log.info("File name must have extension of \".gz\""); 
                System.exit(1); 
            } 

            //System.out.println("Opening the compressed file."); 
            GZIPInputStream in = null; 
            try { 
                in = new GZIPInputStream(new FileInputStream(inFileName)); 
            } catch(Exception e) { 
            	DispatcherMainDay.log.warning("File not found. " + inFileName); 
                System.exit(1); 
            } 

           // System.out.println("Open the output file."); 
            String outFileName = getFileName(inFileName); 
            FileOutputStream out = null; 
           try { 
                out = new FileOutputStream(outFileName); 
            } catch (Exception e) { 
            	DispatcherMainDay.log.warning("Could not write to file. " + outFileName); 
                System.exit(1); 
            } 

            //System.out.println("Transfering bytes from compressed file to the output file."); 
            byte[] buf = new byte[102400]; 
            int len; 
            while((len = in.read(buf)) > 0) { 
                out.write(buf, 0, len); 
            } 

           // System.out.println("Closing the file and stream"); 
            in.close(); 
            out.close(); 
        
        } catch (Exception e) { 
        	e.printStackTrace();
            //System.exit(1); 
        } 
  
	}
	
	private static void mkFolder(String fileName) {
		File f = new File(fileName);
		if (!f.exists()) {
			f.mkdirs();
		}
	}

	private File mkFile(String fileName) {
		File f = new File(fileName);
		try {
			f.createNewFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return f;
	}
	public GZip clone(){
		GZip o = null;
		
		try {
			o = (GZip)super.clone();
			o.executeflg = false;
			o.mapfiles = new HashMap<String,String>();
			/*threadstatus_obj = getSysRandom();
			threadstatus.put(threadstatus_obj, 0);*/

		} catch (Exception e) {
			e.printStackTrace();
		}
		return o;
	}

	public synchronized static String getSysRandom(){
		Random rd = new Random();
		String currtime = String.valueOf(System.currentTimeMillis());
		DateFormat dfErrLog = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		Thread th = Thread.currentThread();
		String SerialNum = dfErrLog.format(new Date())+rd.nextLong()+currtime+th.getId();
		return SerialNum;
	}
	public static void main(String[] args){
		//GZip g = new GZip();
		//g.unzipTARGZ("C:/Users/Administrator/Desktop/compare/LTE_MRGZ_HUAWEI_136.158.196.101_201702210115_201702210130_001.tar.gz","");
		/*
		dispatcherThread.GZip.InitThreadPoolExecutor();
		GZipFile("C:/Users/Administrator/Desktop/���ڲ���/��������ݿⲿ��?",null);
		dispatcherThread.GZip.thutdownThreadPoolExecutor();
		try {
			Thread.currentThread().sleep(10*1000L);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Integer integer1_parse = 1;
		while(true){
			//UNZIP Thread begin
			if(dispatcherThread.GZip.threadPool_parse != null &&(dispatcherThread.GZip.threadPool_parse.getActiveCount() !=0 || dispatcherThread.GZip.threadPool_parse.getQueue().size() !=0)){
				System.out.println("******[Process GZip] ThreadActiveCount:"+dispatcherThread.GZip.threadPool_parse.getActiveCount()+" QueueThread:"+dispatcherThread.GZip.threadPool_parse.getQueue().size());
				try {
					Thread.sleep(10*1000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else{
			}
		}
		
	*/
		/*File f = new File("E:\\BaiduNetdiskDownload\\AH_HF_MOBILE_OMC1_ZTE_MR_eNodeB000_0015_20170505054500_0_0_0.tar.gz");
		f.delete();*/
		GZip gz = new GZip();
		//gz.unzipTARGZ("E:\\BaiduNetdiskDownload\\AH_HF_MOBILE_OMC1_ZTE_MR_eNodeB000_0015_20170505054500_0_0_0.tar.gz","");

		//gz.unzipTARGZ("AH_HF_MOBILE_OMC1_ZTE_MR_eNodeB000_0015_20170505054500_0_0_0.tar.gz","");
		gz.unzipZIP("E:/TEMPWORK/test/FDD-LTE_MR_ZTE_OMC1_20170825041500.zip","","1") ;
	}
}