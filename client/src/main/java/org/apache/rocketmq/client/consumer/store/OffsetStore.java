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
package org.apache.rocketmq.client.consumer.store;

import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消费进度持久化接口
 */
public interface OffsetStore {

    /**
     * 加载
     */
    void load() throws MQClientException;


    /**
     * 更新指定消费队列缓存中消费进度
     *
     * @param mq           消费队列
     * @param offset       消费进度
     * @param increaseOnly 是否使用CAS保证一定修改成功
     */
    void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

    /**
     * 读取消费队列进度，可以选择缓存或持久化读取
     * 集群模式缓存读取本地内存，持久化读取远程broker
     * 广播模式缓存读取本地内存，持久化读取本地文件
     *
     * @param mq   消费队列
     * @param type 缓存/持久化
     */
    long readOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * 对消费队列集合进度进行持久化 （集群模式发送到远程broker，广播模式写入本地文件）
     *
     * @param mqs 消费队列集合
     */
    void persistAll(final Set<MessageQueue> mqs);

    /**
     * 对消费队列进度进行持久化 （集群模式发送到远程到broker，广播模式写入本地文件）
     *
     * @param mq 消费队列
     */
    void persist(final MessageQueue mq);

    /**
     * 清空指定消息队列本地消费进度缓存
     *
     * @param mq 消费队列
     */
    void removeOffset(MessageQueue mq);

    /**
     * 克隆缓存中消费进度
     *
     * @param topic 消息topic
     */
    Map<MessageQueue, Long> cloneOffsetTable(String topic);

    /**
     * 集群模式更新broker存储的消费进度
     */
    void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;
}
