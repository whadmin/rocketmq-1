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

import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 注册消息队列侦听器
 * MessageQueueListener由应用程序实现
 * 消费队列负载均衡分配给消息实例发送变更时候通知回调(Consumers实例加人或退出时)
 */
public interface MessageQueueListener {

    /**
     * 消费队列变更时回调
     *
     * @param topic     消息topic
     * @param mqAll     此消息主题中的所有队列
     * @param mqDivided 分配给当前使用者消息队列
     */
    void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
                             final Set<MessageQueue> mqDivided);
}
