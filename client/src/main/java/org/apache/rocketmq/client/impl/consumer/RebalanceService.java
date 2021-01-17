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

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * 负载均衡线程服务（当前MQClientInstanceMQ消费客户端实例）
 * <p>
 * 1 定时获取获取注册到MQClientInstanceMQ消费客户端实例的所有MQConsumerInner(消费分组),
 * 2 针对当前MQClientInstance客户端实例在当前MQConsumerInner(消费分组)中消费队列进行负载均衡分配
 */
public class RebalanceService extends ServiceThread {

    /**
     * 服务线程阻塞超时时间
     */
    private static long waitInterval =
            Long.parseLong(System.getProperty(
                    "rocketmq.client.rebalance.waitInterval", "20000"));

    /**
     * 内部日志
     */
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * MQ客户端实例对象
     */
    private final MQClientInstance mqClientFactory;

    /**
     * RebalanceService 构造函数
     *
     * @param mqClientFactory MQ客户端实例对象
     */
    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    /**
     * 负载均衡服务核心工作
     * 1 获取获取注册到MQClientInstance客户端实例的所有MQConsumerInner(消费分组),
     * 2 对当前MQClientInstance客户端实例在当前MQConsumerInner(消费分组)中消费队列进行负载均衡分配
     */
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        //循环中判断是否需要停止服务线程
        while (!this.isStopped()) {
            //阻塞服务线程,有超时
            this.waitForRunning(waitInterval);
            //获取获取注册到MQClientInstance客户端实例的所有MQConsumerInner(消费分组),
            //对当前MQClientInstance客户端实例在当前MQConsumerInner(消费分组)中消费队列进行负载均衡分配
            this.mqClientFactory.doRebalance();
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
