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
package org.apache.rocketmq.common.consumer;

/**
 * 指定从哪里消费策略
 */
public enum ConsumeFromWhere {

    /**
     * 默认策略，从该队列最尾开始消费，即跳过历史消息
     *
     * 消费者客户在之前停止的地方继续购物。如果是新启动的消费者客户端，则根据消费者群体的老化情况，有两个
     *
     * 1 如果消费者组是最近创建的，则最早的订阅消息尚未到期，这意味着该消费者组代表着最近启动的业务，那么消费将从一开始就开始；
     *
     * 2 如果最早的订阅消息已过期，则消费将从最新消息开始
     * 消息，这意味着在启动时间戳之前发出的消息将被忽略。
     */
    CONSUME_FROM_LAST_OFFSET,

    /**
     * 从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
     */
    CONSUME_FROM_FIRST_OFFSET,


    /**
     * 从某个时间点开始消费，和setConsumeTimestamp()配合使用，默认是半个小时以前
     */
    CONSUME_FROM_TIMESTAMP,

    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
    @Deprecated
    CONSUME_FROM_MIN_OFFSET,
    @Deprecated
    CONSUME_FROM_MAX_OFFSET,
}
