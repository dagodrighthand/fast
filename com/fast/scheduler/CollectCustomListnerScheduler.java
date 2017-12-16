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
			
			//��ʼ��Scheduler
			sched = (new StdSchedulerFactory()).getScheduler();
			//��ʱִ��MyJob��Ĭ�Ϸ���
			//��һ������������ƣ��ڶ���������������������ʵ�ʵ�������Ҫִ�еĻص��ࡣ
			JobDetail jobDetail = new JobDetail("CollectCustomListnerScheduler",
					"groupsimpletrigger", CollectCustomListnerJob.class);
			//��һ����Trigger�����ƣ��ڶ�����Trigger��������������������ʼʱ�䣬���ĸ��ǽ���ʱ�䣬��������ظ�
			//������ʹ��SimpleTrigger.REPEAT_INDEFINITELY������ʾ���޴Σ������һ�����ظ����ڣ���λ�Ǻ��룩��
			//�������������ǿ�ʼ3���ִ�У�֮��ÿʮ��ִ��һ��
			SimpleTrigger simpletrigger = new SimpleTrigger(
					"simpletrigger", 
					"groupsimpletrigger",
					strToDate,
					null,
					SimpleTrigger.REPEAT_INDEFINITELY, 
					10*1000L/*60*6*10L * 1000L*/);
			
			//�����ǵ�ʱ��ƻ�sched���job����������ÿ��sched����Ӷ��job��������
			sched.scheduleJob(jobDetail, simpletrigger);
			
			//��ʼִ��ʱ��ƻ�
			sched.start();
		} catch (Exception e) {
			e.printStackTrace();
			//exceptionHandle(e);
		}
	}

}
