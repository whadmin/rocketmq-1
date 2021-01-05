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
package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

/**
 * MQClientManager：是一个单例工厂类，用来构造MQClientInstance实例
 * <p>
 * 在同一个JVM内部客户端ID相同的MQClientInstance是复用的
 * <p>
 * 客户端ID：本地IP@instanceName
 */
public class MQClientManager {

    /**
     * 内部日志
     */
    private final static InternalLogger log = ClientLogger.getLog();

    /**
     * 单例MQClientManager
     */
    private static MQClientManager instance = new MQClientManager();

    /**
     * MQClientInstance.instanceIndex 计数器
     */
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();

    /**
     * 使用ConcurrentMap保存构造的MQClientInstance实例
     */
    private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable =
            new ConcurrentHashMap<String, MQClientInstance>();


    private MQClientManager() {
    }

    /**
     * 获取单例MQClientManager对象
     *
     * @return
     */
    public static MQClientManager getInstance() {
        return instance;
    }

    /**
     * 获取并构造MQClientInstance
     *
     * @param clientConfig 客户端配置
     * @return
     */
    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig) {
        return getOrCreateMQClientInstance(clientConfig, null);
    }

    /**
     * 获取并构造MQClientInstance
     *
     * @param clientConfig 客户端配置
     * @param rpcHook      RPC钩子
     * @return
     */
    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        //构造客户端ClientId
        String clientId = clientConfig.buildMQClientId();
        //从factoryTable通过clientId 获取MQClientInstance
        MQClientInstance instance = this.factoryTable.get(clientId);
        //无法factoryTable从获取MQClientInstance
        if (null == instance) {
            //创建MQClientInstance实例
            instance =
                    new MQClientInstance(clientConfig.cloneClientConfig(),
                            this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            //重新添加到factoryTable（如果不存在（新的entry），那么会向map中添加该键值对，并返回null，如果已经存在，那么不会覆盖已有的值，直接返回已经存在的值）
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }
        //返回MQClientInstance实例
        return instance;
    }

    /**
     * 删除指定客户端对应MQClientInstance实例
     *
     * @param clientId 客户端id
     */
    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
