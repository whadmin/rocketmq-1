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

import java.util.List;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;

/**
 * 消费消息服务，实现所谓的"Push-被动"消费机制
 * <p>
 * 1 拉取的消息装成 ConsumeRequest
 * 2 ConsumeRequest 提交给ConsumeMessageService实现类
 * 2 ConsumeMessageService 实现类 负责回调用户 MessageListener 消息监听器
 */
public interface ConsumeMessageService {

    /**
     * 启动服务
     */
    void start();

    /**
     * 关闭服务
     *
     * @param awaitTerminateMillis
     */
    void shutdown(long awaitTerminateMillis);

    /**
     * 更新消费消息服务中核心线程数
     *
     * @param corePoolSize 核心线程数
     */
    void updateCorePoolSize(int corePoolSize);

    /**
     * 核心线程数+1
     */
    void incCorePoolSize();

    /**
     * 核心线程数-1
     */
    void decCorePoolSize();

    /**
     * 获取消费消息服务中核心线程数
     *
     * @return 核心线程数
     */
    int getCorePoolSize();

    /**
     * 执行消费消息
     *
     * @param msg        消费消息
     * @param brokerName broker节点名称
     * @return
     */
    ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String brokerName);

    /**
     * 提交消息请求
     *
     * @param msgs 消费消息
     * @param processQueue 执行队列
     * @param messageQueue 消费队列
     * @param dispathToConsume 派遣消费
     */
    void submitConsumeRequest(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final boolean dispathToConsume);
}
