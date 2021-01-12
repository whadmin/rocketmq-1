/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * MQ线程服务基类提供了如下通用功能
 * <p>
 * 1 启动服务
 * 2 关闭服务
 */
public abstract class ServiceThread implements Runnable {

    /**
     * 内部日志
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 关闭服务线程时,主线程（关闭线程）同步等待服务线程中止最大超时时间
     */
    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 服务执行线程
     */
    private Thread thread;

    /**
     * 线程同步工具, 此类扩展了CountDownLatch,增强了一个重置的线程阻塞状态的方法
     * 用来当服务线程不存在新的任务的情况下（判断hasNotified），阻塞服务线程
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    /**
     * 服务线程存在新的任务发起通知状态。 声明为AtomicBoolean，保证原子性，可见性
     * 服务线程通常在运行时会在循环中判断是否存在新的任务通知，如果不存在则重置waitPoint，并通过waitPoint挂起当前服务线程
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    /**
     * 服务是否停止，stopped是共享变量，设置为volatile保证多个线程对其"可见性"
     */
    protected volatile boolean stopped = false;

    /**
     * 执行服务线程是否是守护线程
     */
    protected boolean isDaemon = false;

    /**
     * 服务线程是否以启动,初始化值为false,声明为AtomicBoolean，保证原子性
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * ServiceThread 构造函数
     */
    public ServiceThread() {

    }

    /**
     * 获取服务名称，模板方法
     */
    public abstract String getServiceName();


    /**
     * 启动服务
     */
    public void start() {
        //打印日志开始启动服务
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        //使用CAS更新服务started状态从false改为true.如果失败直接返回
        if (!started.compareAndSet(false, true)) {
            return;
        }
        //设置服务stopped状态为false
        stopped = false;

        //Runnable是当前对象，具体run方法子类实现
        //线程名称为服务名称，getServiceName()模板方法子类实现
        this.thread = new Thread(this, getServiceName());
        //设置是否为守护线程
        this.thread.setDaemon(isDaemon);
        //启动服务线程
        this.thread.start();
    }

    /**
     * 关闭服务(默认不会被中断服务线程)
     */
    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 关闭服务线程
     *
     * @param interrupt 是否中断服务线程
     */
    public void shutdown(final boolean interrupt) {
        //打印日志开始关闭服务线程
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        //使用CAS更新服务started状态从true改为false.如果失败直接返回
        if (!started.compareAndSet(true, false)) {
            return;
        }
        //设置服务stopped状态为true.
        this.stopped = true;
        //打印日志关闭服务线程
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        //使用CAS更新服务线程通知状态为true,对服务线程发起存在新的任务通知
        //发起通知时优化服务线程调用waitForRunning方法进入阻塞，当前线程会等待服务线程执行完毕，延长关闭服务时间
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            //中断工作线程
            if (interrupt) {
                this.thread.interrupt();
            }
            //记录开始时间
            long beginTime = System.currentTimeMillis();

            //如果不是守护线程， 当前线程shuntdown等待工作线程执行完毕，超时时间this.getJointime()
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            //计算当前线程等待服务线程执行完毕的时间
            long elapsedTime = System.currentTimeMillis() - beginTime;
            //打印日志
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                    + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    /**
     * @return
     */
    public long getJointime() {
        return JOIN_TIME;
    }

    /**
     * 停止服务（已废弃）(默认不会被中断服务线程)
     */
    @Deprecated
    public void stop() {
        this.stop(false);
    }

    /**
     * 停止服务（已废弃）
     *
     * @param interrupt 是否中断服务线程
     */
    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    /**
     * 标记服务停止状态为已停止
     */
    public void makeStop() {
        //如果服务为启动直接返回
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    /**
     * 服务线程存在新的任务，通知服务线程从阻塞中被唤醒
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    /**
     *
     * @param interval 阻塞超时时间
     */
    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        //entry to wait
        waitPoint.reset();

        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
