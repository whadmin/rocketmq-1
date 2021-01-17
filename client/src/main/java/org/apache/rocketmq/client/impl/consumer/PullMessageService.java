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
package org.apache.rocketmq.client.impl.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.utils.ThreadUtils;

/**
 * 拉取消息线程服务（当前MQClientInstanceMQ消费客户端实例）
 * 1 拉取消息服务负责从pullRequestQueue队列中获取PullRequest拉取消息的请求（每一个拉取请求对应一个消息队列拉取任务）
 * 2 拉取请求包含了MessageQueue消费队列，以及ProcessQueue消息处理队列
 * 3 pullRequestQueue队列中PullRequest拉取消息的请求来源于RebalanceService负载均衡服务分配时添加
 */
public class PullMessageService extends ServiceThread {

    /**
     * 内部日志
     */
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 拉取消息请求阻塞队列
     */
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();

    /**
     * MQ客户端实例对象
     */
    private final MQClientInstance mQClientFactory;

    /**
     * 延时任务线程
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "PullMessageServiceScheduledThread");
                }
            });


    /**
     * PullMessageService 构造函数
     *
     * @param mQClientFactory MQ客户端实例对象
     */
    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }


    /**
     * 拉取消息服务工作
     * 1 获取获取注册到MQClientInstance客户端实例的所有MQConsumerInner(消费分组),
     * 2 对当前MQClientInstance客户端实例在当前MQConsumerInner(消费分组)中消费队列进行负载均衡分配
     */
    @Override
    public void run() {
        //打印启动日志
        log.info(this.getServiceName() + " service started");

        //循环中判断是否需要停止服务线程
        while (!this.isStopped()) {
            try {
                //每次从pullRequestQueue获取一个拉取请求
                PullRequest pullRequest = this.pullRequestQueue.take();
                //处理拉取请求
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                //打印异常日志
                log.error("Pull Message Service Run Method exception", e);
            }
        }
        //打印停止日志
        log.info(this.getServiceName() + " service end");
    }

    /**
     * 处理拉取请求
     *
     * @param pullRequest 拉取请求
     */
    private void pullMessage(final PullRequest pullRequest) {
        //获取拉取请求对应的消费分组 MQConsumerInner
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            //通过MQConsumerInner 实现处理拉取请求
            impl.pullMessage(pullRequest);
        } else {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }

    /**
     * 将指定拉取消息请求，延时一段时间添加到 pullRequestQueue
     *
     * @param pullRequest 拉取消息请求
     * @param timeDelay   延时时间（毫秒）
     */
    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageService.this.executePullRequestImmediately(pullRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    /**
     * 将指定拉取消息请求，添加到 pullRequestQueue
     *
     * @param pullRequest 拉取消息请求
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        try {
            this.pullRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            log.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }


    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }


    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }


    /**
     * 使用scheduledExecutorService延时一段时间，执行某个线程任务，
     *
     * @param r         线程任务
     * @param timeDelay 延时时间（毫秒）
     */
    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }
}
