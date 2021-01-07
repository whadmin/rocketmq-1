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

import java.util.Collection;
import java.util.List;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * rocketmq6.0引入了LitePullConsumer
 *
 * 解决Add lite pull consumer support for RocketMQ #1388，
 *
 * https://github.com/apache/rocketmq/issues/1388
 *
 * 提供了如下功能：
 *
 （1）支持自动重新平衡订阅方式消费消息。
 （2）支持以分配方式消耗消息，没有自动重新平衡支持。
 （3）为指定的消息队列添加查找/提交偏移量。
 */
public interface LitePullConsumer {

    /**
     * 启动 LitePullConsumer
     */
    void start() throws MQClientException;

    /**
     * 关闭 LitePullConsumer
     */
    void shutdown();

    /**
     * 基于topic订阅消息，消息过滤使用TAG过滤表达式类型
     *
     * @param subExpression 仅支持或操作，例如“ tag1 || tag2 || tag3”，如果为null或*表达式，则表示全部订阅。
     * @throws MQClientException 如果有任何客户端错误。
     */
    void subscribe(final String topic, final String subExpression) throws MQClientException;

    /**
     * 基于topic订阅消息,消息过滤使用特定类型的消息选择器(支持TAG过滤，SLQ92过滤)
     *
     * @param topic 消息topic。
     * @param selector 消息选择器,如果为null不过滤消息
     *
     * @throws MQClientException 如果有任何客户端错误。
     */
    void subscribe(final String topic, final MessageSelector selector) throws MQClientException;

    /**
     * 取消指定topic消息订阅
     *
     * @param topic 消息topic。
     */
    void unsubscribe(final String topic);


    /**
     * 基于订阅的topic拉取消息
     *
     * @return 消息列表，可以为null
     */
    List<MessageExt> poll();

    /**
     * 基于订阅的topic拉取消息同时设置超时时间
     *
     * @param timeout 如果没有数据，则等待轮询所花费的时间（以毫秒为单位）。不能为负
     * @return 消息列表，可以为null
     */
    List<MessageExt> poll(long timeout);


    /**
     * 手动将消息队列列表分配给当前Consumer实例。不允许增量分配，并且将替换先前的分配（如果有）。
     *
     * @param messageQueues 消息对了集合
     */
    void assign(Collection<MessageQueue> messageQueues);

    /**
     * Consumer当前实例暂停从请求的消息队列中拉取消息。
     *
     * 由于实施了预拉，poll中的数据提取不会立即停止，直到请求的消息队列中的消息耗尽为止。
     *
     * 请注意，此方法不会影响消息队列订阅。特别是，它不会导致组重新平衡。
     *
     * @param messageQueues 需要暂停的消息队列。
     */
    void pause(Collection<MessageQueue> messageQueues);

    /**
     * 恢复已被{@link #pause（Collection）}暂停的指定消息队列。
     *
     * @param messageQueues 需要恢复的消息队列。
     */
    void resume(Collection<MessageQueue> messageQueues);

    /**
     * 查找给定消息队列最后提交的消费逻辑偏移量。
     *
     * @param messageQueue 消费队列
     * @return 消费队列偏移量，如果偏移量等于-1，则表示代理中没有偏移量。
     * @throws MQClientException 如果有任何客户端错误。
     */
    Long committed(MessageQueue messageQueue) throws MQClientException;

    /**
     * 通过时间戳查找给定消息队列的逻辑偏移量。
     * 每个消息队列返回的偏移量是最早的偏移量，其时间戳大于或等于相应消息队列中的给定时间戳记。
     *
     * @param messageQueue 消费队列
     * @param timestamp
     * @return offset
     * @throws MQClientException 如果有任何客户端错误。
     */
    Long offsetForTimestamp(MessageQueue messageQueue, Long timestamp) throws MQClientException;

    /**
     * 覆盖指定消费对了逻辑偏移量，
     * 这个操作只能只用于下一次poll,如果在使用过程中随意使用此API，则可能会丢失数据。
     *
     * @param messageQueue 消费队列
     * @param offset  消费队列消费逻辑偏移量
     */
    void seek(MessageQueue messageQueue, long offset) throws MQClientException;

    /**
     * 用使用者将在下一次轮询中使用的开始偏移量覆盖获取偏移量。
     * 如果对同一消息队列多次调用此API，则最新的偏移量将用于下一个poll（）。
     * 请注意，如果在使用过程中随意使用此API，则可能会丢失数据。
     *
     * @param messageQueue
     */
    void seekToBegin(MessageQueue messageQueue)throws MQClientException;

    /**
     * 用使用者将在下一次轮询中使用的结束偏移量覆盖获取偏移量。
     * 如果对同一消息队列多次调用此API，则最新的偏移量将用于下一个poll（）。
     * 请注意，如果在使用过程中随意使用此API，则可能会丢失数据。
     *
     * @param messageQueue
     */
    void seekToEnd(MessageQueue messageQueue)throws MQClientException;


    /**
     * 是否启用自动提交消息逻辑偏移量（Consumer当前实例分配的消费队列）
     *
     * @return true 如果启用自动提交，则为false，如果禁用自动提交。
     */
    boolean isAutoCommit();

    /**
     * 设置是否启用自动提交消息逻辑偏移量（Consumer当前实例分配的消费队列）。
     *
     * @param autoCommit 是否启用自动提交。
     */
    void setAutoCommit(boolean autoCommit);

    /**
     * 手动提交消费队列消息逻辑偏移量。
     */
    void commitSync();

    /**
     * 根据主题当前获取Consumers实例分配消息队列
     * 每个消费分组中实例会被负载均衡消息指定消息队列
     *
     * @param topic 消息topic
     * @return  消息队列集合
     * @throws MQClientException 如果有任何客户端错误。
     */
    Collection<MessageQueue> fetchMessageQueues(String topic) throws MQClientException;

    /**
     * 更新nameSer服务器地址。
     */
    void updateNameServerAddress(String nameServerAddress);

    /**
     * 注册指定topic消息队列侦听器
     *
     * @param topic 消息topic
     * @param topicMessageQueueChangeListener 指定topic消息队列变更监听器
     * @throws MQClientException 如果有任何客户端错误。
     */
    void registerTopicMessageQueueChangeListener(String topic,
                                                 TopicMessageQueueChangeListener topicMessageQueueChangeListener) throws MQClientException;

}
