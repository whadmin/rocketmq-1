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

import java.util.Set;

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * MQConsumer 接口
 */
public interface MQConsumer extends MQAdmin {

    /**
     * 如果consumer消费消息失败，consumer将延迟消耗一些时间，将消息将被发送回broker，
     *
     * @param msg        发送返回消息
     * @param delayLevel 延迟级别。
     * @throws RemotingException    远程调用异常
     * @throws MQBrokerException    broker异常
     * @throws InterruptedException 中断异常
     * @throws MQClientException    客户端异常
     */
    @Deprecated
    void sendMessageBack(final MessageExt msg, final int delayLevel) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;

    /**
     * 如果consumer消费消息失败，consumer将延迟消耗一些时间，将消息将被发送回broker，
     *
     * @param msg        发送返回消息
     * @param delayLevel 延迟级别。
     * @param brokerName broker节点名称
     * @throws RemotingException    远程调用异常
     * @throws MQBrokerException    broker异常
     * @throws InterruptedException 中断异常
     * @throws MQClientException    客户端异常
     */
    void sendMessageBack(final MessageExt msg, final int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * 根据消息topic对应MessageQueue(消息队列列表)
     *
     * @param topic 消息topic
     * @return MessageQueue(消息队列列表)
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;
}
