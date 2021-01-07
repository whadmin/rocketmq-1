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
package org.apache.rocketmq.client.consumer.listener;

import java.util.List;

import org.apache.rocketmq.common.message.MessageExt;

/**
 * 并发消息监听器
 * MessageListener并发实现接口对象，并发消息监听器用于同时接收异步传递的消息
 */
public interface MessageListenerConcurrently extends MessageListener {

    /**
     * 不建议抛出异常，而不是如果使用失败则返回ConsumeConcurrentlyStatus.RECONSUME_LATER
     *
     * @param msgs msgs.size（）> = 1 <br> DefaultMQPushConsumer.consumeMessageBatchMaxSize = 1，您可以在此处修改
     * @return 并发消息状态
     */
    ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
                                             final ConsumeConcurrentlyContext context);
}
