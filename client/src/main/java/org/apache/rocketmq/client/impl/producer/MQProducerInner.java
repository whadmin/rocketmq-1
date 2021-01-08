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
package org.apache.rocketmq.client.impl.producer;

import java.util.Set;

import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;

/**
 * Producer 内部核心实现接口
 */
public interface MQProducerInner {

    /**
     * 获取所有发送消息topic
     */
    Set<String> getPublishTopicList();

    /**
     * 如果topic本地和远程获取路由信息是否需要更新
     * 通过客户端MQProducerInner
     *
     * @param topic
     * @return
     */
    boolean isPublishTopicNeedUpdate(final String topic);

    /**
     * 获取 TransactionCheckListener(已废弃)
     */
    TransactionCheckListener checkListener();

    /**
     * 获取 TransactionListener (已废弃) 事务消息监听器
     */
    TransactionListener getCheckListener();

    /**
     * 检查事务状态
     *
     * @param addr               地址
     * @param msg                消息
     * @param checkRequestHeader 消息头部
     */
    void checkTransactionState(
            final String addr,
            final MessageExt msg,
            final CheckTransactionStateRequestHeader checkRequestHeader);

    /**
     * topic和topic对应发布信息
     *
     * @param topic 发送消息topic
     * @param info  topic对应发布信息
     */
    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);

    /**
     * 单元化
     */
    boolean isUnitMode();
}
