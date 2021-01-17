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
package org.apache.rocketmq.client.consumer;

import java.util.List;

import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 分配MessageQueue策略
 * <p>
 * AllocateMessageQueueStrategy 用来分配MessageQueue和consumer实例clientID一对一关系的策略算法
 * <p>
 * Topic  ----  consumerGroup 是一对订阅关系
 * <p>
 * 【发送消息】
 * <p>
 * Topic消息会发送到多个broker节点的多个MessageQueue中
 * <p>
 * TopicA -----brokerNameA  -----MessageQueue1
 * -----MessageQueue2
 * -----MessageQueue3
 * -----MessageQueue4
 * <p>
 * TopicA -----brokerNameB  -----MessageQueue1
 * -----MessageQueue2
 * -----MessageQueue3
 * -----MessageQueue4
 * <p>
 * 【消息消息】
 * <p>
 * 每一个消费的客户端IP可能对应多个 consumerGroup
 * <p>
 * clientID  -----consumerGroupA
 * -----consumerGroupB
 */
public interface AllocateMessageQueueStrategy {

    /**
     * 给当前 MQClientInstance 消费者实例分配消息队列
     *
     * @param consumerGroup MQClientInstance 消费者实例对应消费分组
     * @param currentCID    MQClientInstance 消费者实例客户端Id
     * @param mqAll         需要分配全部消息队列列表
     * @param cidAll        当前消费者组中的所有QClientInstance 消费者实例列表
     * @return 当前MQClientInstance 消费者实例分配的消息队列列表
     */
    List<MessageQueue> allocate(
            final String consumerGroup,
            final String currentCID,
            final List<MessageQueue> mqAll,
            final List<String> cidAll
    );

    /**
     * 分配策略名称
     *
     * @return 分配策略名称
     */
    String getName();
}
