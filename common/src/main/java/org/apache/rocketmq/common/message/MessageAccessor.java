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

package org.apache.rocketmq.common.message;

import java.util.Map;

public class MessageAccessor {

    public static void clearProperty(final Message msg, final String name) {
        msg.clearProperty(name);
    }

    public static void setProperties(final Message msg, Map<String, String> properties) {
        msg.setProperties(properties);
    }

    public static void setTransferFlag(final Message msg, String unit) {
        putProperty(msg, MessageConst.PROPERTY_TRANSFER_FLAG, unit);
    }

    public static void putProperty(final Message msg, final String name, final String value) {
        msg.putProperty(name, value);
    }

    public static String getTransferFlag(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_TRANSFER_FLAG);
    }

    public static void setCorrectionFlag(final Message msg, String unit) {
        putProperty(msg, MessageConst.PROPERTY_CORRECTION_FLAG, unit);
    }

    public static String getCorrectionFlag(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_CORRECTION_FLAG);
    }

    /**
     * 设置消息原始消息ID，消息重试会新创建一个消息需要创建关联原始消息ID
     *
     * @param msg             消息
     * @param originMessageId 消息原始id
     */
    public static void setOriginMessageId(final Message msg, String originMessageId) {
        putProperty(msg, MessageConst.PROPERTY_ORIGIN_MESSAGE_ID, originMessageId);
    }

    /**
     * 获取消息原始消息ID，消息重试会新创建一个消息需要创建关联原始消息ID
     *
     * @param msg 消息
     * @return
     */
    public static String getOriginMessageId(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
    }

    public static void setMQ2Flag(final Message msg, String flag) {
        putProperty(msg, MessageConst.PROPERTY_MQ2_FLAG, flag);
    }

    public static String getMQ2Flag(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_MQ2_FLAG);
    }

    /**
     * 设置消息重试次数
     *
     * @param msg            消息
     * @param reconsumeTimes 重试次数
     */
    public static void setReconsumeTime(final Message msg, String reconsumeTimes) {
        putProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME, reconsumeTimes);
    }

    /**
     * 获取消息重试次数
     *
     * @param msg 消息
     * @return 重试次数
     */
    public static String getReconsumeTime(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_RECONSUME_TIME);
    }

    /**
     * 设置消息最大重试次数
     *
     * @param msg               消息
     * @param maxReconsumeTimes 最大重试次数
     */
    public static void setMaxReconsumeTimes(final Message msg, String maxReconsumeTimes) {
        putProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES, maxReconsumeTimes);
    }

    /**
     * 获取消息最大重试次数
     *
     * @param msg 消息
     * @return 最大重试次数
     */
    public static String getMaxReconsumeTimes(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
    }

    /**
     * 设置消息开始提交给并发消息监听器消费的时间戳
     *
     * @param msg                           消息
     * @param propertyConsumeStartTimeStamp 时间戳
     */
    public static void setConsumeStartTimeStamp(final Message msg, String propertyConsumeStartTimeStamp) {
        putProperty(msg, MessageConst.PROPERTY_CONSUME_START_TIMESTAMP, propertyConsumeStartTimeStamp);
    }

    /**
     * 获取消息开始提交给并发消息监听器消费的时间戳
     *
     * @param msg 消息
     * @return 时间戳
     */
    public static String getConsumeStartTimeStamp(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_CONSUME_START_TIMESTAMP);
    }

    /**
     * 克隆消息
     *
     * @param msg 消息
     * @return
     */
    public static Message cloneMessage(final Message msg) {
        Message newMsg = new Message(msg.getTopic(), msg.getBody());
        newMsg.setFlag(msg.getFlag());
        newMsg.setProperties(msg.getProperties());
        return newMsg;
    }

}
