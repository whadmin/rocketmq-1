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
package org.apache.rocketmq.common.filter;

import java.net.URL;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 过滤API
 * <p>
 * 负责通过topic过滤表达式构造 SubscriptionData 订阅配置信息
 * 负责通过指定过滤类型 SubscriptionData 订阅配置信息
 */
public class FilterAPI {


    /**
     * 获取指定Class资源URL(不知道为什么这代码放这?)
     *
     * @param className
     * @return
     */
    public static URL classFile(final String className) {
        final String javaSource = simpleClassName(className) + ".java";
        URL url = FilterAPI.class.getClassLoader().getResource(javaSource);
        return url;
    }

    /**
     * 过滤class名称中 '.' (不知道为什么这代码放这?)
     *
     * @param className
     * @return
     */
    public static String simpleClassName(final String className) {
        String simple = className;
        int index = className.lastIndexOf(".");
        if (index >= 0) {
            simple = className.substring(index + 1);
        }

        return simple;
    }

    /**
     * 构造订阅配置信息（通过topic过滤表达式）
     *
     * @param consumerGroup 消费分组
     * @param topic         消费topic
     * @param subString     TAG过滤过滤表达式
     * @return
     * @throws Exception
     */
    public static SubscriptionData buildSubscriptionData(final String consumerGroup, String topic,
                                                         String subString) throws Exception {

        //创建订阅配置信息
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);

        //如果表达式是 * | 获取空字符串，设置为 *
        if (null == subString || subString.equals(SubscriptionData.SUB_ALL) || subString.length() == 0) {
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
        }
        //如果表达式是 * | 获取空字符串，解析 TAG过滤过滤表达式
        else {
            String[] tags = subString.split("\\|\\|");
            if (tags.length > 0) {
                for (String tag : tags) {
                    if (tag.length() > 0) {
                        String trimString = tag.trim();
                        if (trimString.length() > 0) {
                            subscriptionData.getTagsSet().add(trimString);
                            subscriptionData.getCodeSet().add(trimString.hashCode());
                        }
                    }
                }
            } else {
                throw new Exception("subString split error");
            }
        }
        //返回
        return subscriptionData;
    }

    /**
     * 构造订阅配置信息（通过topic过滤表达式）
     *
     * @param topic     消费topic
     * @param subString topic过滤表达式
     * @param type      过滤类型
     * @return
     * @throws Exception
     */
    public static SubscriptionData build(final String topic, final String subString,
                                         final String type) throws Exception {

        //如果过滤类型是TAG过滤
        if (ExpressionType.TAG.equals(type) || type == null) {
            return buildSubscriptionData(null, topic, subString);
        }

        //如果过滤类型是SQL过滤，如果过滤表达式为null，抛出异常
        if (subString == null || subString.length() < 1) {
            throw new IllegalArgumentException("Expression can't be null! " + type);
        }

        //构造订阅配置信息
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);

        //返回
        return subscriptionData;
    }
}
