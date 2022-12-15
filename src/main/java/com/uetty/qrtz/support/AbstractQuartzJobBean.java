package com.uetty.qrtz.support;

import com.uetty.qrtz.spi.ConstantSpi;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public abstract class AbstractQuartzJobBean extends QuartzJobBean {

//    // 分布式锁内执行相关
//    @Autowired
//    protected RedisLockUtil lockUtil;
//
//    protected int getLockWaitSleepSeconds() {
//        return 2;
//    }
//
//    protected int getLockTryTimes() {
//        return 15;
//    }
//
//    protected String getLockName() {
//        return null;
//    }
//
//    /**
//     * 分布式锁中运行代码
//     * @param runnable 运行的代码
//     */
//    protected void executeInLock(Runnable runnable) {
//        RedisLockUtil.Lock lock = null;
//
//        try {
//
//            if (getLockName() == null) {
//                runnable.run();
//                return;
//            }
//
//            int tryTimes = 0;
//            do {
//                if (tryTimes > 0) {
//                    // 睡眠几秒
//                    LockSupport.parkNanos(getLockWaitSleepSeconds() * (long) 1e9);
//                }
//                //noinspection resource
//                lock = lockUtil.lock(getLockName());
//
//            } while (lock == null && (getLockTryTimes() == -1 || tryTimes++ < getLockTryTimes())); // 尝试几次
//
//            if (lock != null) {
//                runnable.run();
//            } else {
//                log.warn("cannot get lock from RedisLockUtil, skip this job");
//            }
//        } finally {
//            if (lock != null) {
//                // 释放锁
//                lock.release();
//            }
//        }
//    }

    protected final Object lock = new Object();

    @Autowired
    protected Scheduler scheduler;

    private static volatile ThreadPoolExecutor executor;

    private static final Map<String, Long> LAST_TRIGGER_TIME_MAP = new HashMap<>();

    /**
     * 提前预定触发时间的最大值-10分钟
     */
    private static final int MAXIMUM_FORWARD_BOOKING_SECONDS = 30 * 60;

    /**
     * 最大排期秒数。自适应触发时，向后排期最多能延期到多少秒
     */
    protected int getAdaptiveMaximumForwardBookingSeconds() {
        return MAXIMUM_FORWARD_BOOKING_SECONDS;
    }

    /**
     * 最小间隔秒数，空值时表示不限制最小间隔。仅自适应触发时，对触发时间进行排期时使用
     * <p>不为空时，自适应触发将保证两次触发间隔大于该值，实际触发时间将为：业务延迟触发秒数 + 延期次数 * 最小间隔秒数。
     * 若该时间大于最大排期秒数，将丢弃触发本次触发</p>
     */
    protected Integer getAdaptiveMinimumIntervalSeconds() {
        return null;
    }

    public abstract String getJobKeyName();

    private JobDataMap buildJobDataMap() {
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(ConstantSpi.getJobManualTriggerFlag(), true);

        String mdcTraceIdKey = ConstantSpi.getMdcTraceIdKey();
        jobDataMap.put(mdcTraceIdKey, MDC.get(mdcTraceIdKey));
        return jobDataMap;
    }

    private long getLastTriggerTime() {
        Long time = LAST_TRIGGER_TIME_MAP.get(getJobKeyName());
        return time == null ? 0L : time.longValue();
    }

    /**
     * 计算下次触发时间
     * @param delaySeconds 最早在多少秒后触发
     * @param adaptive 是否启用自适应触发（如果使用自适应触发，将对触发时间进行排期，保证最小触发间隔和最大排期秒数）
     * @return 下次触发时间（空值表示因最大排期秒数已满，而取消本次排期）
     */
    private Date getTriggerTime(int delaySeconds, boolean adaptive) {

        long currentTimeMillis = System.currentTimeMillis();
        long nextInterval = currentTimeMillis + delaySeconds * 1000L;

        Integer minSecondsTriggerInterval = getAdaptiveMinimumIntervalSeconds();
        if (!adaptive || minSecondsTriggerInterval == null) {
            // 不限制间隔
            return new Date(nextInterval);
        }

        // 如果开启自适应触发时间（对触发时间排期，保证最小触发间隔和最大排期）

        // 根据上次设置的触发时间，计算受触发间隔限制最早可以排期的时间
        long earliestNextTiggerTime = getLastTriggerTime() + minSecondsTriggerInterval * 1000L;
        if (earliestNextTiggerTime > nextInterval) {
            // 确认是否受限，如果是，则将最早可排期时间更新为下次触发时间，保证了触发最小间隔
            nextInterval = earliestNextTiggerTime;
        }

        int maxForwardBookingSeconds = getAdaptiveMaximumForwardBookingSeconds();
        // 最多提前排期到的时间
        long maxBookingTriggerTime = currentTimeMillis + maxForwardBookingSeconds * 1000L;
        if (nextInterval > maxBookingTriggerTime) {
            // 排期已满
            return null;
        }

        return new Date(nextInterval);
    }

    private void updateLastTriggerTime(long lastTriggerTime) {
        LAST_TRIGGER_TIME_MAP.put(getJobKeyName(), lastTriggerTime);
    }

    private void initAsyncExecutor() {
        if (executor == null) {
            synchronized (AbstractQuartzJobBean.class) {
                if (executor == null) {
                    executor = new ThreadPoolExecutor(1, 1,
                            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new DefaultThreadFactory());
                }
            }
        }
    }

    /**
     * 添加触发器（异步）
     * <p>防止高并发情况下，qrtz添加触发器消耗过多资源，导致接口变慢</p>
     * @param delaySeconds 多少秒后触发
     */
    public void addTrigger(int delaySeconds) {
        JobDataMap jobDataMap = buildJobDataMap();

        initAsyncExecutor();

        executor.execute(new AddTriggerTask(jobDataMap, delaySeconds, false));
    }

    /**
     * 添加触发器（异步）
     * <p>防止高并发情况下，qrtz添加触发器消耗过多资源，导致接口变慢</p>
     * @param data 业务参数
     */
    public void addTrigger(Map<String, Object> data) {
        JobDataMap jobDataMap = buildJobDataMap();
        jobDataMap.putAll(data);

        initAsyncExecutor();

        executor.execute(new AddTriggerTask(jobDataMap, 0, false));
    }

    /**
     * 添加触发器（异步）
     * <p>防止高并发情况下，qrtz添加触发器消耗过多资源，导致接口变慢</p>
     * @param data 业务参数
     */
    public void addTrigger(Map<String, Object> data, int delaySeconds) {
        JobDataMap jobDataMap = buildJobDataMap();
        jobDataMap.putAll(data);

        initAsyncExecutor();

        executor.execute(new AddTriggerTask(jobDataMap, delaySeconds, false));
    }

    /**
     * 添加触发器（异步）
     * <p>异步添加触发器，防止高并发情况下，qrtz添加触发器消耗过多资源，导致接口变慢</p>
     */
    public void addTrigger() {
        JobDataMap jobDataMap = buildJobDataMap();
        initAsyncExecutor();

        executor.execute(new AddTriggerTask(jobDataMap, 0, false));
    }

    /**
     * 自适应（将对触发时间进行排期，保证最小触发间隔和最大排期秒数）添加触发器（异步）
     * @param delaySeconds 多少秒后触发
     */
    public void adaptiveAddTrigger(int delaySeconds) {

        JobDataMap jobDataMap = buildJobDataMap();
        initAsyncExecutor();

        executor.execute(new AddTriggerTask(jobDataMap, delaySeconds, true));
    }

    /**
     * 自适应（将对触发时间进行排期，保证最小触发间隔和最大排期秒数）添加触发器（异步）
     * @param data 业务参数
     */
    public void adaptiveAddTrigger(Map<String, Object> data) {

        JobDataMap jobDataMap = buildJobDataMap();
        jobDataMap.putAll(data);
        initAsyncExecutor();

        executor.execute(new AddTriggerTask(jobDataMap, 0, true));
    }

    /**
     * 自适应（将对触发时间进行排期，保证最小触发间隔和最大排期秒数）添加触发器（异步）
     * @param data 业务参数
     * @param delaySeconds 多少秒后触发
     */
    public void adaptiveAddTrigger(Map<String, Object> data, int delaySeconds) {

        JobDataMap jobDataMap = buildJobDataMap();
        jobDataMap.putAll(data);
        initAsyncExecutor();

        executor.execute(new AddTriggerTask(jobDataMap, delaySeconds, true));
    }

    /**
     * 自适应（将对触发时间进行排期，保证最小触发间隔和最大排期秒数）添加触发器（异步）
     */
    public void adaptiveAddTrigger() {

        JobDataMap jobDataMap = buildJobDataMap();
        initAsyncExecutor();

        executor.execute(new AddTriggerTask(jobDataMap, 0, true));
    }

    @AllArgsConstructor
    private class AddTriggerTask implements Runnable {

        private final JobDataMap jobDataMap;

        private final int delaySeconds;

        private final boolean adaptive;

        @Override
        public void run() {
            try {
                // 发送消息触发
                JobKey jobKey = new JobKey(getJobKeyName());

                synchronized (lock) {
                    Date date = getTriggerTime(delaySeconds, adaptive);
                    if (date == null) {
                        log.debug("schedule time is before last trigger time");
                        return;
                    }

                    Trigger trigger = TriggerBuilder.newTrigger()
                            .forJob(jobKey)
                            .usingJobData(jobDataMap)
                            .startAt(date)
                            .build();

                    scheduler.scheduleJob(trigger);

                    updateLastTriggerTime(date.getTime());
                }

                log.debug("add trigger of job --> " + getJobKeyName());
            } catch (SchedulerException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private static class DefaultThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        public DefaultThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = "job-trigger-pool-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
