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

/**
 * 异步拉取消息回调
 */
public interface PullCallback {

    /**
     * 异步拉取消成功回调
     *
     * @param pullResult 拉取消息结果
     */
    void onSuccess(final PullResult pullResult);

    /**
     * 异步拉取消异常回调
     *
     * @param e 拉取消息异常
     */
    void onException(final Throwable e);
}
