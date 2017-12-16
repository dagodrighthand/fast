package com.fast.scheduler;


import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.fast.app.SyncFtpAuto;
/*
 * update by: gdp
 * update date:20150818
 * version:1.0
 * 
 */
public class SyncFtpAutoJob implements Job {
	public void execute(JobExecutionContext arg0) {
		if (SyncFtpAuto.thread_run_flag
				.equalsIgnoreCase("TRUE")) {
			return;
		}
		
		SyncFtpAuto.thread_run_flag = "TRUE";
		try {
			SyncFtpAuto.func_main();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
