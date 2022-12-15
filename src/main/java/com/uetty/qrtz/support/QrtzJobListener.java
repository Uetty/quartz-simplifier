package com.uetty.qrtz.support;

import com.uetty.qrtz.spi.ConstantSpi;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.impl.matchers.EverythingMatcher;
import org.slf4j.MDC;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * 全局设定job的logId
 */
@Slf4j
@Component
public class QrtzJobListener implements JobListener, InitializingBean {

    @Autowired
    protected Scheduler scheduler;

    @Override
    public String getName() {
        return "qrtzLogIdListener";
    }

    private void sleuthTypeLogTraceExtend(JobDataMap jobDataMap) {
        String logId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        String traceId = logId;
        String parentSpanId = logId;

        String mdcTraceIdKey = ConstantSpi.getMdcTraceIdKey();
        String mdcParentSpanIdKey = ConstantSpi.getMdcParentSpanIdKey();
        String mdcSpanIdKey = ConstantSpi.getMdcSpanIdKey();

        if (jobDataMap != null && jobDataMap.containsKey(ConstantSpi.getJobManualTriggerFlag())) {
            traceId = (String) jobDataMap.get(mdcTraceIdKey);
            String rawParentSpanId = (String) jobDataMap.get(mdcParentSpanIdKey);
            String rawSpanId = (String) jobDataMap.get(mdcSpanIdKey);
            parentSpanId = rawSpanId;
            log.debug("qrtz extends traceId, and spanId changed: [{}, {}, {}] -> [{}, {}, {}]",
                    traceId, rawSpanId, rawParentSpanId, traceId, logId, parentSpanId);
        }
        MDC.put(mdcTraceIdKey, traceId);
        MDC.put(mdcParentSpanIdKey, parentSpanId);
        MDC.put(mdcSpanIdKey, logId);
    }

    private void commonLogTraceExtend(JobDataMap jobDataMap) {
        String traceId = UUID.randomUUID().toString().replace("-", "");

        String mdcTraceIdKey = ConstantSpi.getMdcTraceIdKey();

        if (jobDataMap != null && jobDataMap.containsKey(ConstantSpi.getJobManualTriggerFlag())) {
            traceId = (String) jobDataMap.get(mdcTraceIdKey);
            if (traceId != null && !"".equals(traceId.trim())) {
                log.debug("qrtz extends traceId {}", traceId);
            }
        }
        MDC.put(mdcTraceIdKey, traceId);
    }

    private void sleuthTypeLogTraceClear(JobDataMap jobDataMap) {
        String mdcTraceIdKey = ConstantSpi.getMdcTraceIdKey();
        String mdcParentSpanIdKey = ConstantSpi.getMdcParentSpanIdKey();
        String mdcSpanIdKey = ConstantSpi.getMdcSpanIdKey();
        MDC.remove(mdcTraceIdKey);
        MDC.remove(mdcParentSpanIdKey);
        MDC.remove(mdcSpanIdKey);
    }

    private void commonLogTraceClear(JobDataMap jobDataMap) {
        String mdcTraceIdKey = ConstantSpi.getMdcTraceIdKey();
        MDC.remove(mdcTraceIdKey);
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext jobExecutionContext) {
        JobDataMap jobDataMap = jobExecutionContext.getMergedJobDataMap();

        if (ConstantSpi.useSleuthTypeLog()) {
            sleuthTypeLogTraceExtend(jobDataMap);
        } else {
            commonLogTraceExtend(jobDataMap);
        }

        log.debug("qrtz job {} startup", jobExecutionContext.getTrigger().getJobKey());
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext jobExecutionContext) {
    }

    @Override
    public void jobWasExecuted(JobExecutionContext jobExecutionContext, JobExecutionException e) {
        JobDataMap jobDataMap = jobExecutionContext.getMergedJobDataMap();

        log.debug("qrtz job {} finished", jobExecutionContext.getTrigger().getJobKey());

        if (jobDataMap == null || !jobDataMap.containsKey(ConstantSpi.getJobManualTriggerFlag())) {
            if (ConstantSpi.useSleuthTypeLog()) {
                sleuthTypeLogTraceClear(jobDataMap);
            } else {
                commonLogTraceClear (jobDataMap);
            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        scheduler.getListenerManager().addJobListener(this, EverythingMatcher.allJobs());
    }
}
