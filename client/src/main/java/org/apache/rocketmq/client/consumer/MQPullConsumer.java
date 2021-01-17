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

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 拉取消费者接口
 */
public interface MQPullConsumer extends MQConsumer {

    /**
     * 启动 MQPullConsumer
     */
    void start() throws MQClientException;

    /**
     * 关闭 MQPullConsumer
     */
    void shutdown();


    /**
     * 根据主题当前获取Consumers实例分配消息队列
     * 每个消费分组中实例会被负载均衡消息指定消息队列
     *
     * @param topic 消息topic
     * @return 当前Consumers实例分配消息队列
     */
    Set<MessageQueue> fetchMessageQueuesInBalance(final String topic) throws MQClientException;


    /**
     * 注册消息队列侦听器(消费队列负载均衡分配给消息实例发送变更时候通知回调(Consumers实例加人或退出时))
     * 每个消费分组中实例会被负载均衡消息指定消息队列
     *
     * @param topic    消息topic
     * @param listener 消息队列侦听器
     */
    void registerMessageQueueListener(final String topic, final MessageQueueListener listener);


    /**
     * 拉取消息，支持消息过滤使用TAG过滤表达式类型，不阻塞
     *
     * @param mq            拉取消息对应消息队列
     * @param subExpression 仅支持或操作，例如“ tag1 || tag2 || tag3”，如果为null或*表达式，则表示全部订阅。
     * @param offset        拉取消息队列逻辑起始地址
     * @param maxNums       拉取消息最大消息数量
     * @return 拉取结果
     */
    PullResult pull(final MessageQueue mq, final String subExpression, final long offset,
                    final int maxNums) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;


    /**
     * 拉出消息，如果没有消息到达，则阻塞一段时间
     *
     * @param mq            拉取消息对应消息队列
     * @param subExpression 仅支持或操作，例如“ tag1 || tag2 || tag3”，如果为null或*表达式，则表示全部订阅。
     * @param offset        拉取消息队列逻辑起始地址
     * @param maxNums       拉取消息最大消息数量
     */
    PullResult pullBlockIfNotFound(final MessageQueue mq, final String subExpression,
                                   final long offset, final int maxNums) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException;


    /**
     * 相对于{@link #pull(MessageQueue, String, long, int)}设置超时时间，
     *
     * @param mq            拉取消息对应消息队列
     * @param subExpression 仅支持或操作，例如“ tag1 || tag2 || tag3”，如果为null或*表达式，则表示全部订阅。
     * @param offset        拉取消息队列逻辑起始地址
     * @param maxNums       拉取消息最大消息数量
     * @param timeout       超时时间
     * @return 拉取结果
     */
    PullResult pull(final MessageQueue mq, final String subExpression, final long offset,
                    final int maxNums, final long timeout) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException;


    /**
     * 拉取消息，消息过滤使用特定类型的消息选择器(支持TAG过滤，SLQ92过滤)，不阻塞
     *
     * @param mq       拉取消息对应消息队列
     * @param selector 消息过滤使用特定类型的消息选择器(支持TAG过滤，SLQ92过滤)
     * @param offset   拉取消息队列逻辑起始地址
     * @param maxNums  拉取消息最大消息数量
     * @return 拉取结果
     */
    PullResult pull(final MessageQueue mq, final MessageSelector selector, final long offset,
                    final int maxNums) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    /**
     * 相对于{@link #pull(MessageQueue, MessageSelector, long, int)}设置超时时间，
     *
     * @param mq       拉取消息对应消息队列
     * @param selector 消息过滤使用特定类型的消息选择器(支持TAG过滤，SLQ92过滤)
     * @param offset   拉取消息队列逻辑起始地址
     * @param maxNums  拉取消息最大消息数量
     * @param timeout  超时时间
     */
    PullResult pull(final MessageQueue mq, final MessageSelector selector, final long offset,
                    final int maxNums, final long timeout) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;


    /**
     * 以异步方式提取消息，支持消息过滤使用TAG过滤表达式类型
     *
     * @param mq            拉取消息对应消息队列
     * @param subExpression 仅支持或操作，例如“ tag1 || tag2 || tag3”，如果为null或*表达式，则表示全部订阅。
     * @param offset        拉取消息队列逻辑起始地址
     * @param maxNums       拉取消息最大消息数量
     * @param pullCallback  拉取消息异步回调
     */
    void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums,
              final PullCallback pullCallback) throws MQClientException, RemotingException,
            InterruptedException;

    /**
     * 以异步方式提取消息，支持消息过滤使用TAG过滤表达式类型，如果没有消息到达，则阻塞。
     *
     * @param mq            拉取消息对应消息队列
     * @param subExpression 仅支持或操作，例如“ tag1 || tag2 || tag3”，如果为null或*表达式，则表示全部订阅。
     * @param offset        拉取消息队列逻辑起始地址
     * @param maxNums       拉取消息最大消息数量
     * @param pullCallback  拉取消息异步回调
     */
    void pullBlockIfNotFound(final MessageQueue mq, final String subExpression, final long offset,
                             final int maxNums, final PullCallback pullCallback) throws MQClientException, RemotingException,
            InterruptedException;

    /**
     * 相对于{@link #pull(MessageQueue, String, long, int, PullCallback)}设置超时时间
     *
     * @param mq            拉取消息对应消息队列
     * @param subExpression 仅支持或操作，例如“ tag1 || tag2 || tag3”，如果为null或*表达式，则表示全部订阅。
     * @param offset        拉取消息队列逻辑起始地址
     * @param maxNums       拉取消息最大消息数量
     * @param pullCallback  拉取消息异步回调
     * @param timeout       超时时间
     */
    void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums,
              final PullCallback pullCallback, long timeout) throws MQClientException, RemotingException,
            InterruptedException;

    /**
     * 以异步方式提取消息，消息过滤使用特定类型的消息选择器(支持TAG过滤，SLQ92过滤)
     *
     * @param mq           拉取消息对应消息队列
     * @param selector     消息过滤使用特定类型的消息选择器(支持TAG过滤，SLQ92过滤)
     * @param offset       拉取消息队列逻辑起始地址
     * @param maxNums      拉取消息最大消息数量
     * @param pullCallback 拉取消息异步回调
     */
    void pull(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums,
              final PullCallback pullCallback) throws MQClientException, RemotingException,
            InterruptedException;

    /**
     * 相对于{@link #pull(MessageQueue, MessageSelector, long, int, PullCallback)}设置超时时间，
     *
     * @param mq           拉取消息对应消息队列
     * @param selector     消息过滤使用特定类型的消息选择器(支持TAG过滤，SLQ92过滤)
     * @param offset       拉取消息队列逻辑起始地址
     * @param maxNums      拉取消息最大消息数量
     * @param pullCallback 拉取消息异步回调
     * @param timeout      超时时间
     */
    void pull(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums,
              final PullCallback pullCallback, long timeout) throws MQClientException, RemotingException,
            InterruptedException;


    /**
     * 更新当前消费分组对当前消费队列消费逻辑偏移
     */
    void updateConsumeOffset(final MessageQueue mq, final long offset) throws MQClientException;

    /**
     * 获取消息对了当前消费分组消费逻辑偏移
     */
    long fetchConsumeOffset(final MessageQueue mq, final boolean fromStore) throws MQClientException;


    /**
     * 如果使用失败，则会将消息发送回给代理，并在一段时间后延迟使用。<br>注意！消息只能在同一组中使用。
     */
    void sendMessageBack(MessageExt msg, int delayLevel, String brokerName, String consumerGroup)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

}
