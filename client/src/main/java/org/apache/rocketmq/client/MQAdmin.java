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
package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * MQ管理的基本接口
 */
public interface MQAdmin {

    /**
     * 创建Topic
     *
     * @param key      accesskey
     * @param newTopic topic 名称
     * @param queueNum topic queue(队列) number（数量）
     */
    void createTopic(final String key, final String newTopic, final int queueNum)
            throws MQClientException;

    /**
     * 创建Topic
     *
     * @param key          accesskey
     * @param newTopic     topic 名称
     * @param queueNum     topic queue(队列) number（数量）
     * @param topicSysFlag topic 系统标识
     */
    void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
            throws MQClientException;


    /**
     * 根据某个时间（以毫秒为单位）获取消息队列偏移量<br> 由于更多的IO开销，请谨慎致电
     * broker内部AdminBrokerProcessor负责处理
     * 请求Code:RequestCode.SEARCH_OFFSET_BY_TIMESTAMP
     *
     * @param mq        消息队列
     * @param timestamp
     * @return offset
     */
    long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;

    /**
     * 获取指定消息队列最大逻辑偏移量
     * broker内部AdminBrokerProcessor负责处理
     * 请求Code:RequestCode.GET_MAX_OFFSET
     *
     * @param mq 消息队列
     * @return 最大偏移
     */
    long maxOffset(final MessageQueue mq) throws MQClientException;

    /**
     * 获取指定消息队列最小逻辑偏移量
     * broker内部AdminBrokerProcessor负责处理
     * 请求Code:RequestCode.GET_MIN_OFFSET
     *
     * @param mq 消息队列
     * @return 最小偏移
     */
    long minOffset(final MessageQueue mq) throws MQClientException;

    /**
     * 获取指定消息队最早的存储消息时间
     *
     * @param mq 消息队列
     * @return 最早的存储消息时间
     */
    long earliestMsgStoreTime(final MessageQueue mq) throws MQClientException;

    /**
     * 根据消息ID查询消息
     *
     * @param offsetMsgId 消息id
     * @return message
     */
    MessageExt viewMessage(final String offsetMsgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    /**
     * 根据条件查询消息（条件包括指定topic+指定消息key+时间范围）的消息
     *
     * @param topic  消息topic
     * @param key    消息key
     * @param maxNum 返回满足消息的最大数量
     * @param begin  查询消息产生时间开始
     * @param end    查询消息产生时间结束
     * @return Instance of QueryResult
     */
    QueryResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
                             final long end) throws MQClientException, InterruptedException;
    /**
     * 根据消息ID查询消息
     * 1 优先通过消息ID查询消息
     * 2 如果1查询不到根据topic+消息key(key=msgId)查询消息
     */
    MessageExt viewMessage(String topic,
                           String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

}