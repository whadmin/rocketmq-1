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

package org.apache.rocketmq.common.protocol.body;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * broker节点内部消费分组配置
 */
public class SubscriptionGroupWrapper extends RemotingSerializable {
    /**
     * broker节点内部消费分组配置以及对应的topic
     */
    private ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable =
        new ConcurrentHashMap<String, SubscriptionGroupConfig>(1024);
    /**
     * 数据版本
     */
    private DataVersion dataVersion = new DataVersion();

    public ConcurrentMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTable;
    }

    public void setSubscriptionGroupTable(
        ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable) {
        this.subscriptionGroupTable = subscriptionGroupTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }
}
