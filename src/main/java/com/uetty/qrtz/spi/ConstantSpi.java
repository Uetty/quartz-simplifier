package com.uetty.qrtz.spi;

import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * 常量spi
 * @author vince
 */
@Slf4j
public class ConstantSpi {

    static ConstantProvider constantProvider = null;

    private static final String MDC_TRACE_ID_KEY = "mdcTraceIdKey";
    private static final String DEFAULT_MDC_TRACE_ID_KEY = "X-B3-TraceId";

    private static final String MDC_PARENT_SPAN_ID_KEY = "mdcParentSpanIdKey";
    private static final String DEFAULT_MDC_PARENT_SPAN_ID_KEY = "X-B3-ParentSpanId";

    private static final String MDC_SPAN_ID_KEY = "mdcSpanIdKey";
    private static final String DEFAULT_MDC_SPAN_ID_KEY = "X-B3-SpanId";

    private static final String USE_SLEUTH_LOG_TYPE = "useSleuthTypeLog";

    private static final String JOB_MANUAL_TRIGGER_FLAG = "MANUAL";

    static {
        ServiceLoader<ConstantProvider> providerServiceLoader = ServiceLoader.load(ConstantProvider.class);
        Iterator<ConstantProvider> iterator = providerServiceLoader.iterator();
        if (iterator.hasNext()) {
            constantProvider = iterator.next();
        }
        if (constantProvider == null) {
            log.warn("constant provider not found");
        }
    }

    public static String getJobManualTriggerFlag() {
        return JOB_MANUAL_TRIGGER_FLAG;
    }

    public static String getMdcTraceIdKey() {
        String key = null;
        if (constantProvider != null) {
            key = constantProvider.getConstantValue(MDC_TRACE_ID_KEY);
        }
        if (key == null || "".equals(key.trim())) {
            key = DEFAULT_MDC_TRACE_ID_KEY;
        }
        return key;
    }

    public static String getMdcParentSpanIdKey() {
        String key = null;
        if (constantProvider != null) {
            key = constantProvider.getConstantValue(MDC_PARENT_SPAN_ID_KEY);
        }
        if (key == null || "".equals(key.trim())) {
            key = DEFAULT_MDC_PARENT_SPAN_ID_KEY;
        }
        return key;
    }

    public static String getMdcSpanIdKey() {
        String key = null;
        if (constantProvider != null) {
            key = constantProvider.getConstantValue(MDC_SPAN_ID_KEY);
        }
        if (key == null || "".equals(key.trim())) {
            key = DEFAULT_MDC_SPAN_ID_KEY;
        }
        return key;
    }

    public static boolean useSleuthTypeLog() {
        boolean use = false;
        if (constantProvider != null) {
            use = "true".equalsIgnoreCase(constantProvider.getConstantValue(USE_SLEUTH_LOG_TYPE));
        }
        return use;
    }
}
