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

import org.apache.rocketmq.common.filter.ExpressionType;

/**
 * 消息选择器：在服务器上选择消息。
 * <p>
 * 现在，支持：
 * <li>TAG过滤:  {@link org.apache.rocketmq.common.filter.ExpressionType#TAG}
 * <li>SLQ92过滤: {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}
 */
public class MessageSelector {

    /**
     * 消息过滤表达式类型
     *
     * @see org.apache.rocketmq.common.filter.ExpressionType
     */
    private String type;

    /**
     * 消息过滤表达式
     */
    private String expression;

    private MessageSelector(String type, String expression) {
        this.type = type;
        this.expression = expression;
    }

    /**
     * 使用SLQ92创建消息选择器
     *
     * @param sql 如果为null或为空，将被视为全选消息。
     */
    public static MessageSelector bySql(String sql) {
        return new MessageSelector(ExpressionType.SQL92, sql);
    }

    /**
     * 使用TAG过滤创建消息选择器
     *
     * @param tag 如果为null或为空，或者为“ *”，则将被视为全选消息。
     */
    public static MessageSelector byTag(String tag) {
        return new MessageSelector(ExpressionType.TAG, tag);
    }

    public String getExpressionType() {
        return type;
    }

    public String getExpression() {
        return expression;
    }
}
