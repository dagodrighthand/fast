package com.fast.scheduler;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SimpleTrigger;
import org.quartz.impl.StdSchedulerFactory;

public class CollectCustomListnerScheduler {

	//private static int DispatchTimerMinute;
	private static Scheduler sched = null;;
	private static DateFormat dfMRLog = new SimpleDateFormat("yyyyMMddHHmm");
	public static void init(){
		try {
			/*Date dBefore = new Date();
			DateFormat dfMRLog = new SimpleDateFormat("yyyyMMdd");
			Date endDate  = dfMRLog.parse(dfMRLog.format(dBefore));
			Calendar calLog = Calendar.getInstance();
			calLog.setTime(endDate);
			calLog.add(Calendar.HOUR, DispatchTimerHour);
			calLog.add(Calendar.MINUTE, DispatchTimerMinute);
			dBefore = calLog.getTime();*/
			Date strToDate = new Date();//dfMRLog.parse(NEXT_SYSDATE_MINUTE);
			
			//初始化Scheduler
			sched = (new StdSchedulerFactory()).getScheduler();
			//定时执行MyJob中默认方法
			//第一个是任务的名称，第二个是组名，第三个就是实际当任务需要执行的回调类。
			JobDetail jobDetail = new JobDetail("CollectCustomListnerScheduler",
					"groupsimpletrigger", CollectCustomListnerJob.class);
			//第一个是Trigger的名称，第二个是Trigger的组名，第三个是任务开始时间，第四个是结束时间，第五个是重复
			//次数（使用SimpleTrigger.REPEAT_INDEFINITELY常量表示无限次），最后一个是重复周期（单位是毫秒），
			//触发器，这里是开始3秒后执行，之后每十秒执行一次
			SimpleTrigger simpletrigger = new SimpleTrigger(
					"simpletrigger", 
					"groupsimpletrigger",
					strToDate,
					null,
					SimpleTrigger.REPEAT_INDEFINITELY, 
					10*1000L/*60*6*10L * 1000L*/);
			
			//给我们的时间计划sched添加job及触发器，每个sched可添加多个job及触发器
			sched.scheduleJob(jobDetail, simpletrigger);
			
			//开始执行时间计划
			sched.start();
		} catch (Exception e) {
			e.printStackTrace();
			//exceptionHandle(e);
		}
	}

}
