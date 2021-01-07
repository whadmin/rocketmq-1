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

import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;

/**
 * 推动消费者接口
 */
public interface MQPushConsumer extends MQConsumer {

    /**
     * 启动PushConsumer
     */
    void start() throws MQClientException;

    /**
     * 关闭PushConsumer
     */
    void shutdown();

    /**
     * 注册消息监听器（已废弃）
     */
    @Deprecated
    void registerMessageListener(MessageListener messageListener);

    /**
     * 注册并发事件监听器
     */
    void registerMessageListener(final MessageListenerConcurrently messageListener);

    /**
     * 注册顺序消息事件监听器
     */
    void registerMessageListener(final MessageListenerOrderly messageListener);

    /**
     * 基于topic订阅消息，消息过滤使用类模式（已废弃）
     */
    @Deprecated
    void subscribe(final String topic, final String fullClassName,
                   final String filterClassSource) throws MQClientException;

    /**
     * 基于topic订阅消息，消息过滤使用TAG过滤表达式类型
     *
     * @param subExpression 仅支持或操作，例如“ tag1 || tag2 || tag3”，如果为null或*表达式，则表示全部订阅。
     */
    void subscribe(final String topic, final String subExpression) throws MQClientException;

    /**
     * 基于topic订阅消息,消息过滤使用特定类型的消息选择器(支持TAG过滤，SLQ92过滤)
     *
     * @param selector 消息选择器,如果为null不过滤消息
     */
    void subscribe(final String topic, final MessageSelector selector) throws MQClientException;

    /**
     * 取消指定topic消息订阅
     *
     * @param topic message topic
     */
    void unsubscribe(final String topic);

    /**
     * 动态更新使用者线程池大小
     */
    void updateCorePoolSize(int corePoolSize);

    /**
     * 暂停消费
     */
    void suspend();

    /**
     * 恢复消费
     */
    void resume();
}
