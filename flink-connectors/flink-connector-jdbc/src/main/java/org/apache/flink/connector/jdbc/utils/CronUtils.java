/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.utils;

import com.cronutils.model.CronType;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.amazingwu.concurrent.CronScheduledThreadPoolExecutor;

/** Utils for jdbc connectors. */
public class CronUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CronUtils.class);
    private static Scheduler defaultScheduler;

    private static CronScheduledThreadPoolExecutor cron;

    static {
        try {
            defaultScheduler = StdSchedulerFactory.getDefaultScheduler();
            cron = new CronScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors());
        } catch (SchedulerException e) {
            LOG.error("jdbc lookup cache all cron init scheduler error! exit.", e);
            System.exit(1);
        }
    }

    /**
     * 定时调度任务.
     *
     * @param job
     * @param cron
     * @throws SchedulerException
     */
    public static void schedule(Job job, String cron) {
        try {
            String thread = Thread.currentThread().getName();
            CronTrigger trigger =
                    TriggerBuilder.newTrigger()
                            .withIdentity("t_" + thread, "g_" + thread)
                            .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                            .build();
            JobDetail jobDetail =
                    JobBuilder.newJob(job.getClass())
                            .withIdentity("j_" + thread, "g_" + thread)
                            .build();
            defaultScheduler.scheduleJob(jobDetail, trigger);
            defaultScheduler.start();
        } catch (SchedulerException e) {
            LOG.error("jdbc lookup cache all cron start error! exit.", e);
            System.exit(1);
        }
    }

    public static CronScheduledThreadPoolExecutor getCron() {
        return cron;
    }

    public static void runCron(Runnable command, String cron) {
        CronUtils.cron.scheduleWithCron(command, cron, CronType.QUARTZ);
    }
}
