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

import java.util.Set;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 消费分组内部核心接口
 */
public interface MQConsumerInner {

    /**
     * 获取消费分组
     *
     * @return 消费分组
     */
    String groupName();

    /**
     * 获取消息模型
     *
     * @return 消息模型
     */
    MessageModel messageModel();

    /**
     * 获取消费类型
     *
     * @return 消费类型
     */
    ConsumeType consumeType();

    /**
     * 从哪里消费策略
     *
     * @return 从哪里消费策略
     */
    ConsumeFromWhere consumeFromWhere();

    /**
     * 获取所有订阅配置信息集合
     *
     * @return 订阅配置信息集合
     */
    Set<SubscriptionData> subscriptions();

    /**
     * 当前 MQClientInstance MQ消费客户端实例，在当前消费分组，负载均衡分配消息队列
     */
    void doRebalance();

    /**
     * 当前 MQClientInstance MQ消费客户端实例，在当前消费分组，负载均衡分配消息队列久化消费进度
     */
    void persistConsumerOffset();

    /**
     * 更新topic订阅信息
     *
     * @param topic 消息topic
     * @param info  消费队列集合
     */
    void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

    /**
     * 如果topic本地和远程获取路由信息存在差异,需要更新
     * 通过客户端MQConsumerInner配置判断是否需要更新到本地
     *
     * @param topic 消息topic
     * @return
     */
    boolean isSubscribeTopicNeedUpdate(final String topic);

    /**
     * 是否是单元化
     *
     * @return
     */
    boolean isUnitMode();

    /**
     * 获取定指定消费分组指定客户端Id运行信息
     *
     * @return 消费分组指定客户端Id运行信息
     */
    ConsumerRunningInfo consumerRunningInfo();
}
