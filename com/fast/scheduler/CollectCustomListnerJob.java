package com.fast.scheduler;

import java.util.Date;

import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.fast.app.CollectCustomListener;

public class CollectCustomListnerJob  implements Job {
	public void execute(JobExecutionContext arg0) {
		if (CollectCustomListener.thread_run_flag
				.equalsIgnoreCase("TRUE")) {
			return;
		}
		
		CollectCustomListener.thread_run_flag = "TRUE";
		try {
			CollectCustomListener.func_main();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
