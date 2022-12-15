package com.uetty.qrtz.support;

import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.jdbcjobstore.StdJDBCDelegate;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerKey.triggerKey;

/**
 * 多版本代码兼容的使得不会因为缺少类而导致报错启动失败
 * <p>
 * 使用：
 * <blockquote>
 * spring.quartz.job-store-type=jdbc
 * </blockquote>
 * <blockquote>
 * spring.quartz.properties.org.quartz.jobStore.driverDelegateClass=com.uetty.qrtz.support.CodeVersionCompatibleJDBCDelegate</p>
 * </blockquote>
 * </p>
 * @author vince
 */
public class CodeVersionCompatibleJDBCDelegate extends StdJDBCDelegate {

    private static final String SELECT_MISFIRED_TRIGGERS = "SELECT t.*, j." + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " t "
            + "INNER JOIN " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " j "
            + "ON j." + COL_JOB_GROUP + " = t." + COL_JOB_GROUP + " "
            + "AND j." + COL_JOB_NAME + " = t." + COL_JOB_NAME + " "
            + "WHERE "
            + "t." + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST
            + " AND NOT ("
            + "t." + COL_MISFIRE_INSTRUCTION + " = " + Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY + ") "
            + "AND t." + COL_NEXT_FIRE_TIME + " < ? "
            + "ORDER BY t." + COL_NEXT_FIRE_TIME + " ASC, t." + COL_PRIORITY + " DESC";

    private static final String SELECT_TRIGGERS_IN_STATE = "SELECT " + "t." + COL_TRIGGER_NAME + ", t." + COL_TRIGGER_GROUP + ", j." + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " t "
            + "INNER JOIN " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " j "
            + "ON j." + COL_JOB_GROUP + " = t." + COL_JOB_GROUP + " "
            + "AND j." + COL_JOB_NAME + " = t." + COL_JOB_NAME + " "
            + "WHERE "
            + "t." + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " "
            + "AND t." + COL_TRIGGER_STATE + " = ?";

    private static final String SELECT_MISFIRED_TRIGGERS_IN_STATE = "SELECT " + "t." + COL_TRIGGER_NAME + ", t." + COL_TRIGGER_GROUP + ", j." + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " t " 
            + "INNER JOIN " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " j "
            + "ON j." + COL_JOB_GROUP + " = t." + COL_JOB_GROUP + " "
            + "AND j." + COL_JOB_NAME + " = t." + COL_JOB_NAME + " "
            + "WHERE "
            + "t." + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " "
            + "AND NOT ( t." + COL_MISFIRE_INSTRUCTION + " = " + Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY + ") "
            + "AND t." + COL_NEXT_FIRE_TIME + " < ? "
            + "AND t." + COL_TRIGGER_STATE + " = ? "
            + "ORDER BY t." + COL_NEXT_FIRE_TIME + " ASC, t." + COL_PRIORITY + " DESC";
    
    private static final String SELECT_HAS_MISFIRED_TRIGGERS_IN_STATE = "SELECT " + "t." + COL_TRIGGER_NAME + ", t." + COL_TRIGGER_GROUP + ", j." + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " t " 
            + "INNER JOIN " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " j "
            + "ON j." + COL_JOB_GROUP + " = t." + COL_JOB_GROUP + " "
            + "AND j." + COL_JOB_NAME + " = t." + COL_JOB_NAME + " "
            + "WHERE "
            + "t." + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " "
            + "AND NOT ( t." + COL_MISFIRE_INSTRUCTION + " = " + Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY + ") "
            + "AND t." + COL_NEXT_FIRE_TIME + " < ? "
            + "AND t." + COL_TRIGGER_STATE + " = ? "
            + "ORDER BY t." + COL_NEXT_FIRE_TIME + " ASC, t." + COL_PRIORITY + " DESC";

    private static final String COUNT_MISFIRED_TRIGGERS_IN_STATE = "SELECT COUNT( t." + COL_TRIGGER_NAME + ") count, j." + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " t "
            + "INNER JOIN " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " j "
            + "ON j." + COL_JOB_GROUP + " = t." + COL_JOB_GROUP + " "
            + "AND j." + COL_JOB_NAME + " = t." + COL_JOB_NAME + " " 
            + "WHERE "
            + "t." + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " " 
            + "AND NOT ( t." + COL_MISFIRE_INSTRUCTION + " = " + Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY + ") "
            + "AND t." + COL_NEXT_FIRE_TIME + " < ? "
            + "AND t." + COL_TRIGGER_STATE + " = ? "
            + "GROUP BY j." + COL_JOB_CLASS;
    
    private static final String SELECT_MISFIRED_TRIGGERS_IN_GROUP_IN_STATE = "SELECT " + "t." + COL_TRIGGER_NAME + ", j." + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " t "
            + "INNER JOIN " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " j "
            + "ON j." + COL_JOB_GROUP + " = t." + COL_JOB_GROUP + " "
            + "AND j." + COL_JOB_NAME + " = t." + COL_JOB_NAME + " "
            + "WHERE "
            + "t." + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " " 
            + "AND NOT ( t." + COL_MISFIRE_INSTRUCTION + " = " + Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY + ") " 
            + "AND t." + COL_NEXT_FIRE_TIME + " < ? " 
            + "AND t." + COL_TRIGGER_GROUP + " = ? " 
            + "AND t." + COL_TRIGGER_STATE + " = ? "
            + "ORDER BY t." + COL_NEXT_FIRE_TIME + " ASC, t." + COL_PRIORITY + " DESC";

    private static final String SELECT_INSTANCES_RECOVERABLE_FIRED_TRIGGERS = "SELECT t.*, j." + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_FIRED_TRIGGERS + " t "
            + "INNER JOIN " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " j "
            + "ON j." + COL_JOB_GROUP + " = t." + COL_JOB_GROUP + " "
            + "AND j." + COL_JOB_NAME + " = t." + COL_JOB_NAME + " "
            + "WHERE "
            + "t." + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " "
            + "AND t." + COL_INSTANCE_NAME + " = ? "
            + "AND t." + COL_REQUESTS_RECOVERY + " = ?";

    private static final String SELECT_TRIGGERS_FOR_JOB = "SELECT " + "t." + COL_TRIGGER_NAME + ", t." + COL_TRIGGER_GROUP + ", j." + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " t "
            + "INNER JOIN " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " j "
            + "ON j." + COL_JOB_GROUP + " = t." + COL_JOB_GROUP + " "
            + "AND j." + COL_JOB_NAME + " = t." + COL_JOB_NAME + " "
            + "WHERE "
            + "t." + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " "
            + "AND t." + COL_JOB_NAME + " = ? "
            + "AND t." + COL_JOB_GROUP + " = ?";

    private static final String SELECT_NUM_JOBS = "SELECT COUNT( " + COL_JOB_NAME + ") count, " + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " "
            + "WHERE "
            + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " "
            + "GROUP BY " + COL_JOB_CLASS;

    private static final String SELECT_JOBS_IN_GROUP = "SELECT " + COL_JOB_NAME + ", " + COL_JOB_GROUP + ", " + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " "
            + "WHERE "
            + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST
            + " AND " + COL_JOB_GROUP + " = ?";

    private static final String SELECT_JOBS_IN_GROUP_LIKE = "SELECT " + COL_JOB_NAME + ", " + COL_JOB_GROUP + ", " + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " "
            + "WHERE "
            + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST
            + " AND " + COL_JOB_GROUP + " LIKE ?";

    private static final String SELECT_NEXT_TRIGGER_TO_ACQUIRE = "SELECT t." + COL_TRIGGER_NAME + ", t." + COL_TRIGGER_GROUP + ", "
            + "t." + COL_NEXT_FIRE_TIME + ", t." + COL_PRIORITY + ", j." + COL_JOB_CLASS + " "
            + "FROM " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " t "
            + "INNER JOIN " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " j "
            + "ON j." + COL_JOB_GROUP + " = t." + COL_JOB_GROUP + " "
            + "AND j." + COL_JOB_NAME + " = t." + COL_JOB_NAME + " "
            + "WHERE "
            + "t." + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " "
            + "AND t." + COL_TRIGGER_STATE + " = ? "
            + "AND t." + COL_NEXT_FIRE_TIME + " <= ? "
            + "AND ( t." + COL_MISFIRE_INSTRUCTION + " = -1 "
            + "    OR ( t." + COL_MISFIRE_INSTRUCTION + " != -1 "
            + "        AND t."+ COL_NEXT_FIRE_TIME + " >= ?"
            + "    )"
            + ") "
            + "ORDER BY t."+ COL_NEXT_FIRE_TIME + " ASC, t." + COL_PRIORITY + " DESC";

    private static WeakReference<Map<String, Boolean>> CLASS_NAME_VALIDATION_MAP = new WeakReference<>(new ConcurrentHashMap<>());

    private boolean isClassNameNotExist(String className) {
        Map<String, Boolean> classNameMap = CLASS_NAME_VALIDATION_MAP.get();
        if (classNameMap == null) {
            classNameMap = new ConcurrentHashMap<>();
            CLASS_NAME_VALIDATION_MAP = new WeakReference<>(classNameMap);
        }

        Boolean exists = classNameMap.get(className);
        if (exists != null) {
            boolean isClassNotExist = Boolean.FALSE.equals(exists);
            if (isClassNotExist) {
                logger.debug("job class {} not found, ignore this job", className);
            }
            return isClassNotExist;
        }

        try {
            classLoadHelper.loadClass(className);
        } catch (ClassNotFoundException e) {
            logger.warn("job class {} not found, ignore this job", className);
            logger.error(e.getMessage(), e);
            classNameMap.put(className, false);
            return true;
        }

        classNameMap.put(className, true);
        return false;
    }

    @Override
    public List<TriggerKey> selectMisfiredTriggers(Connection conn, long ts) throws SQLException {
        logger.debug("selectMisfiredTriggers");

        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(rtp(SELECT_MISFIRED_TRIGGERS));
            ps.setBigDecimal(1, new BigDecimal(String.valueOf(ts)));
            rs = ps.executeQuery();

            LinkedList<TriggerKey> list = new LinkedList<>();
            while (rs.next()) {
                String triggerName = rs.getString(COL_TRIGGER_NAME);
                String groupName = rs.getString(COL_TRIGGER_GROUP);
                // 为了兼容多版本代码同时跑一个数据库的情况，预检测JobDetails类存在性，使不会因为类未找到抛出异常报错
                String jobClassName = rs.getString(COL_JOB_CLASS);
                if (isClassNameNotExist(jobClassName)) {
                    continue;
                }
                list.add(triggerKey(triggerName, groupName));
            }
            return list;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    @Override
    public List<TriggerKey> selectTriggersInState(Connection conn, String state) throws SQLException {
        logger.debug("selectTriggersInState");

        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(rtp(SELECT_TRIGGERS_IN_STATE));
            ps.setString(1, state);
            rs = ps.executeQuery();

            LinkedList<TriggerKey> list = new LinkedList<>();
            while (rs.next()) {
                // 为了兼容多版本代码同时跑一个数据库的情况，预检测JobDetails类存在性，使不会因为类未找到抛出异常报错
                String jobClassName = rs.getString(COL_JOB_CLASS);
                if (isClassNameNotExist(jobClassName)) {
                    continue;
                }
                list.add(triggerKey(rs.getString(1), rs.getString(2)));
            }

            return list;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    @Override
    public List<TriggerKey> selectMisfiredTriggersInState(Connection conn, String state, long ts) throws SQLException {
        logger.debug("selectMisfiredTriggersInState");
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(rtp(SELECT_MISFIRED_TRIGGERS_IN_STATE));
            ps.setBigDecimal(1, new BigDecimal(String.valueOf(ts)));
            ps.setString(2, state);
            rs = ps.executeQuery();

            LinkedList<TriggerKey> list = new LinkedList<>();
            while (rs.next()) {
                String triggerName = rs.getString(COL_TRIGGER_NAME);
                String groupName = rs.getString(COL_TRIGGER_GROUP);
                // 为了兼容多版本代码同时跑一个数据库的情况，预检测JobDetails类存在性，使不会因为类未找到抛出异常报错
                String jobClassName = rs.getString(COL_JOB_CLASS);
                if (isClassNameNotExist(jobClassName)) {
                    continue;
                }
                list.add(triggerKey(triggerName, groupName));
            }
            return list;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    @Override
    public boolean hasMisfiredTriggersInState(Connection conn, String state1, long ts, int count, List<TriggerKey> resultList) throws SQLException {
        logger.debug("hasMisfiredTriggersInState");
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(rtp(SELECT_HAS_MISFIRED_TRIGGERS_IN_STATE));
            ps.setBigDecimal(1, new BigDecimal(String.valueOf(ts)));
            ps.setString(2, state1);
            rs = ps.executeQuery();

            boolean hasReachedLimit = false;
            while (rs.next() && (!hasReachedLimit)) {
                if (resultList.size() == count) {
                    hasReachedLimit = true;
                } else {
                    String triggerName = rs.getString(COL_TRIGGER_NAME);
                    String groupName = rs.getString(COL_TRIGGER_GROUP);
                    // 为了兼容多版本代码同时跑一个数据库的情况，预检测JobDetails类存在性，使不会因为类未找到抛出异常报错
                    String jobClassName = rs.getString(COL_JOB_CLASS);
                    if (isClassNameNotExist(jobClassName)) {
                        continue;
                    }
                    resultList.add(triggerKey(triggerName, groupName));
                }
            }

            return hasReachedLimit;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    @Override
    public int countMisfiredTriggersInState(Connection conn, String state1, long ts) throws SQLException {
        logger.debug("countMisfiredTriggersInState");
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(rtp(COUNT_MISFIRED_TRIGGERS_IN_STATE));
            ps.setBigDecimal(1, new BigDecimal(String.valueOf(ts)));
            ps.setString(2, state1);
            rs = ps.executeQuery();

            int count = 0;
            while (rs.next()) {
                int countSingleJob = rs.getInt("count");
                // 为了兼容多版本代码同时跑一个数据库的情况，预检测JobDetails类存在性，使不会因为类未找到抛出异常报错
                String jobClassName = rs.getString(COL_JOB_CLASS);
                if (isClassNameNotExist(jobClassName)) {
                    continue;
                }
                count += countSingleJob;
            }
            
            return count;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    @Override
    public List<TriggerKey> selectMisfiredTriggersInGroupInState(Connection conn, String groupName, String state, long ts) throws SQLException {
        logger.debug("selectMisfiredTriggersInGroupInState");
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn
                    .prepareStatement(rtp(SELECT_MISFIRED_TRIGGERS_IN_GROUP_IN_STATE));
            ps.setBigDecimal(1, new BigDecimal(String.valueOf(ts)));
            ps.setString(2, groupName);
            ps.setString(3, state);
            rs = ps.executeQuery();

            LinkedList<TriggerKey> list = new LinkedList<>();
            while (rs.next()) {
                String triggerName = rs.getString(COL_TRIGGER_NAME);
                // 为了兼容多版本代码同时跑一个数据库的情况，预检测JobDetails类存在性，使不会因为类未找到抛出异常报错
                String jobClassName = rs.getString(COL_JOB_CLASS);
                if (isClassNameNotExist(jobClassName)) {
                    continue;
                }
                list.add(triggerKey(triggerName, groupName));
            }
            return list;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    @Override
    public List<OperableTrigger> selectTriggersForRecoveringJobs(Connection conn) throws SQLException, IOException, ClassNotFoundException {
        logger.debug("selectTriggersForRecoveringJobs");
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn
                    .prepareStatement(rtp(SELECT_INSTANCES_RECOVERABLE_FIRED_TRIGGERS));
            ps.setString(1, instanceId);
            setBoolean(ps, 2, true);
            rs = ps.executeQuery();

            long dumId = System.currentTimeMillis();
            LinkedList<OperableTrigger> list = new LinkedList<>();
            while (rs.next()) {
                String jobName = rs.getString(COL_JOB_NAME);
                String jobGroup = rs.getString(COL_JOB_GROUP);
                String trigName = rs.getString(COL_TRIGGER_NAME);
                String trigGroup = rs.getString(COL_TRIGGER_GROUP);
                long firedTime = rs.getLong(COL_FIRED_TIME);
                long scheduledTime = rs.getLong(COL_SCHED_TIME);
                int priority = rs.getInt(COL_PRIORITY);

                // 为了兼容多版本代码同时跑一个数据库的情况，预检测JobDetails类存在性，使不会因为类未找到抛出异常报错
                String jobClassName = rs.getString(COL_JOB_CLASS);
                if (isClassNameNotExist(jobClassName)) {
                    continue;
                }

                @SuppressWarnings("deprecation")
                SimpleTriggerImpl rcvryTrig = new SimpleTriggerImpl("recover_"
                        + instanceId + "_" + dumId++,
                        Scheduler.DEFAULT_RECOVERY_GROUP, new Date(scheduledTime));
                rcvryTrig.setJobName(jobName);
                rcvryTrig.setJobGroup(jobGroup);
                rcvryTrig.setPriority(priority);
                rcvryTrig.setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY);

                JobDataMap jd = selectTriggerJobDataMap(conn, trigName, trigGroup);
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_NAME, trigName);
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_GROUP, trigGroup);
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_FIRETIME_IN_MILLISECONDS, String.valueOf(firedTime));
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_SCHEDULED_FIRETIME_IN_MILLISECONDS, String.valueOf(scheduledTime));
                rcvryTrig.setJobDataMap(jd);

                list.add(rcvryTrig);
            }
            return list;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    @Override
    public List<TriggerKey> selectTriggerKeysForJob(Connection conn, JobKey jobKey) throws SQLException {
        logger.debug("selectTriggerKeysForJob");
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(rtp(SELECT_TRIGGERS_FOR_JOB));
            ps.setString(1, jobKey.getName());
            ps.setString(2, jobKey.getGroup());
            rs = ps.executeQuery();

            LinkedList<TriggerKey> list = new LinkedList<>();
            while (rs.next()) {
                String trigName = rs.getString(COL_TRIGGER_NAME);
                String trigGroup = rs.getString(COL_TRIGGER_GROUP);

                // 为了兼容多版本代码同时跑一个数据库的情况，预检测JobDetails类存在性，使不会因为类未找到抛出异常报错
                String jobClassName = rs.getString(COL_JOB_CLASS);
                if (isClassNameNotExist(jobClassName)) {
                    continue;
                }

                list.add(triggerKey(trigName, trigGroup));
            }
            return list;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    @Override
    public int selectNumJobs(Connection conn) throws SQLException {
        logger.debug("selectNumJobs");
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(rtp(SELECT_NUM_JOBS));
            rs = ps.executeQuery();

            int count = 0;
            while (rs.next()) {
                int countSingleJob = rs.getInt("count");
                // 为了兼容多版本代码同时跑一个数据库的情况，预检测JobDetails类存在性，使不会因为类未找到抛出异常报错
                String jobClassName = rs.getString(COL_JOB_CLASS);
                if (isClassNameNotExist(jobClassName)) {
                    continue;
                }
                count += countSingleJob;
            }

            return count;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    @Override
    public Set<JobKey> selectJobsInGroup(Connection conn, GroupMatcher<JobKey> matcher)
            throws SQLException {
        logger.debug("selectJobsInGroup");
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            if(isMatcherEquals(matcher)) {
                ps = conn.prepareStatement(rtp(SELECT_JOBS_IN_GROUP));
                ps.setString(1, toSqlEqualsClause(matcher));
            }
            else {
                ps = conn.prepareStatement(rtp(SELECT_JOBS_IN_GROUP_LIKE));
                ps.setString(1, toSqlLikeClause(matcher));
            }
            rs = ps.executeQuery();

            LinkedList<JobKey> list = new LinkedList<>();
            while (rs.next()) {
                // 为了兼容多版本代码同时跑一个数据库的情况，预检测JobDetails类存在性，使不会因为类未找到抛出异常报错
                String jobClassName = rs.getString(COL_JOB_CLASS);
                if (isClassNameNotExist(jobClassName)) {
                    continue;
                }

                list.add(jobKey(rs.getString(COL_JOB_NAME), rs.getString(COL_JOB_GROUP)));
            }

            return new HashSet<>(list);
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    @Override
    public List<TriggerKey> selectTriggerToAcquire(Connection conn, long noLaterThan, long noEarlierThan, int maxCount)
            throws SQLException {
        logger.debug("selectTriggerToAcquire");
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<TriggerKey> nextTriggers = new LinkedList<>();
        try {
            ps = conn.prepareStatement(rtp(SELECT_NEXT_TRIGGER_TO_ACQUIRE));

            // Set max rows to retrieve
            if (maxCount < 1) {
                // we want at least one trigger back.
                maxCount = 1;
            }
            ps.setMaxRows(maxCount);

            // Try to give jdbc driver a hint to hopefully not pull over more than the few rows we actually need.
            // Note: in some jdbc drivers, such as MySQL, you must set maxRows before fetchSize, or you get exception!
            ps.setFetchSize(maxCount);

            ps.setString(1, STATE_WAITING);
            ps.setBigDecimal(2, new BigDecimal(String.valueOf(noLaterThan)));
            ps.setBigDecimal(3, new BigDecimal(String.valueOf(noEarlierThan)));
            rs = ps.executeQuery();

            while (rs.next() && nextTriggers.size() < maxCount) {

                // 为了兼容多版本代码同时跑一个数据库的情况，预检测JobDetails类存在性，使不会因为类未找到抛出异常报错
                String jobClassName = rs.getString(COL_JOB_CLASS);
                if (isClassNameNotExist(jobClassName)) {
                    continue;
                }

                nextTriggers.add(triggerKey(
                        rs.getString(COL_TRIGGER_NAME),
                        rs.getString(COL_TRIGGER_GROUP)));
            }

            return nextTriggers;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

}
