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

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;

/**
 * 消费消息服务
 * <p>
 * 1 负责消费{@link PullRequest}拉取请求拉取的消息
 * 1.1 每一个拉取请求对应了某个消费分组对指定消息队列拉取消息请求。处理拉取请求核心实现 {@link DefaultMQPushConsumerImpl#pullMessage(PullRequest)}
 * 1.2 每一次拉取请求拉取消息会提交到{@link ConsumeMessageService}
 * 1.3 提交到{@link ConsumeMessageService}消息会根据{@link DefaultMQPushConsumer#consumeMessageBatchMaxSize}
 *     配置将消息拆分到一个或多个{@link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest}消费请求任务
 *     提交到消息线程池消费
 * 1.4 {@link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest}
 *     将需要消费的消息回调{@link org.apache.rocketmq.client.consumer.listener.MessageListener}
 * 2 负责消费 broker接收管理后台重发的消息
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
     * 处理重发的消息
     *
     * @param msg        消费消息
     * @param brokerName broker节点名称
     * @return
     */
    ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String brokerName);

    /**
     * 处理消费请求
     * 消费 {@link DefaultMQPushConsumerImpl#pullMessage(PullRequest)}中获取的消息
     *
     * @param msgs             消费消息
     * @param processQueue     执行队列
     * @param messageQueue     消费队列
     * @param dispathToConsume 派遣消费
     */
    void submitConsumeRequest(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final boolean dispathToConsume);
}
