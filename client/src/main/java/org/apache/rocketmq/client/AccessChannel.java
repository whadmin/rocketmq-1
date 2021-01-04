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
package org.apache.rocketmq.client;

/**
 * 用于设置访问通道，如果需要将rocketmq服务迁移到云，建议设置为“ CLOUD”。否则设置为“ LOCAL”，尤其是使用了消息跟踪功能
 */
public enum AccessChannel {

    /**
     * 表示连接到专用IDC群集。
     */
    LOCAL,

    /**
     * 表示连接到云服务。
     */
    CLOUD,
}
