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
package org.apache.rocketmq.tools.admin;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.RollbackStats;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.BrokerStatsData;
import org.apache.rocketmq.common.protocol.body.ClusterAclVersionInfo;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.api.MessageTrack;

public interface MQAdminExt extends MQAdmin {
    void start() throws MQClientException;

    void shutdown();


    void createAndUpdateTopicConfig(final String addr,
                                    final TopicConfig config) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    void createAndUpdatePlainAccessConfig(final String addr, final PlainAccessConfig plainAccessConfig) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    void deletePlainAccessConfig(final String addr, final String accessKey) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    void updateGlobalWhiteAddrConfig(final String addr, final String globalWhiteAddrs) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    ClusterAclVersionInfo examineBrokerClusterAclVersionInfo(final String addr) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    AclConfig examineBrokerClusterAclConfig(final String addr) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    TopicConfig examineTopicConfig(final String addr, final String topic);

    /**
     * 获取指定Topic对应的 获取TopicStatsTable状态信息
     * TopicStatsTable内部包括该Topic每个MessageQueue信息，以及每个MessageQueue逻辑偏移信息
     *
     * @param topic 消息Topic
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    TopicStatsTable examineTopicStats(
            final String topic) throws RemotingException, MQClientException, InterruptedException,
            MQBrokerException;

    /**
     * 获取指定Topic对应的 TopicRouteData 主题路由信息
     * TopicRouteData内部包括Topic消息队列信息（包括分配规则和数据同步方式）列表，TopicBroker节点列表
     *
     * @param topic 消息Topic
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     */
    TopicRouteData examineTopicRouteInfo(
            final String topic) throws RemotingException, MQClientException, InterruptedException;


    /**
     * 获取指定Topic当前集群在线的订阅消费分组
     *
     * @param topic 消息Topic
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    GroupList queryTopicConsumeByWho(final String topic) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQBrokerException, RemotingException, MQClientException;


    /************************************** 生产组相关接口 **************************************/


    /**
     * 查询指定指定producerGroup（生产分组） ProducerConnection(生产分组连接信息)
     *
     * @param producerGroup
     * @param topic
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    ProducerConnection examineProducerConnectionInfo(final String producerGroup,
                                                     final String topic) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException;


    /************************************** 消费组相关接口 **************************************/

    /**
     * 查询指定groupName（消费分组）  ConsumeStats（消费分组状态信息）
     * 包括:
     * 所有MessageQueue(消息队列)以及对应的OffsetWrapper(消费进度)
     * <p>
     * 这里需要注意由于没有指定topic，当groupName（消费分组）订阅了多个Topic时会返回多个Topic内部的
     * MessageQueue(消息队列)以及对应的OffsetWrapper(消费进度)
     *
     * @param consumerGroup
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    ConsumeStats examineConsumeStats(
            final String consumerGroup) throws RemotingException, MQClientException, InterruptedException,
            MQBrokerException;

    /**
     * 查询指定topic指定groupName（消费分组）  ConsumeStats（消费分组状态信息）
     * 包括:
     * 所有MessageQueue(消息队列)以及对应的OffsetWrapper(消费进度)
     *
     * @param consumerGroup
     * @param topic
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    ConsumeStats examineConsumeStats(final String consumerGroup,
                                     final String topic) throws RemotingException, MQClientException,
            InterruptedException, MQBrokerException;


    /**
     * 查询指定指定groupName（消费分组） ConsumerConnection(消费组连接信息)
     * 包括:
     * <p>
     * 1 终端连接信息（id,地址，语言，版本）
     * 2 消费topic以及topic对应的订阅配置信息
     * 3 客户端类型（推，拉）
     * 4 消费模型（广播，集群）
     * 5 消费策略
     *
     * @param consumerGroup 订阅组
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    ConsumerConnection examineConsumerConnectionInfo(final String consumerGroup) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException, RemotingException,
            MQClientException;


    /**
     * 查询指定指定groupName（消费分组），指定客户端Id ConsumerRunningInfo(消费实例信息)
     * 包括:
     * <p>
     * 1 Topic订阅配置信息列表
     * 2 当前消费实例分配的MessageQueue(消息)以及ProcessQueueInfo（处理消费队列信息）
     * 3 当前消费实例不同topic以及ConsumeStatus(消费实例消费状态（各种RT,TPS统计))
     *
     * @param consumerGroup
     * @param clientId
     * @param jstack
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     */
    ConsumerRunningInfo getConsumerRunningInfo(final String consumerGroup, final String clientId, final boolean jstack)
            throws RemotingException, MQClientException, InterruptedException;


    /**
     * 选择broker节点一个实例（终端），删除指定消费分组
     *
     * @param addr      消费实例（终端）
     * @param groupName 消费分组
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void deleteSubscriptionGroup(final String addr, String groupName) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /************************************** 集群相关接口 **************************************/

    /**
     * 查询集群信息（包含所有集群和关联broker节点，broker节点数据）
     *
     * @return
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     */
    ClusterInfo examineBrokerClusterInfo() throws InterruptedException, MQBrokerException, RemotingTimeoutException,
            RemotingSendRequestException, RemotingConnectException;


    int wipeWritePermOfBroker(final String namesrvAddr, String brokerName) throws RemotingCommandException,
            RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException;

    void putKVConfig(final String namespace, final String key, final String value);

    String getKVConfig(final String namespace,
                       final String key) throws RemotingException, MQClientException, InterruptedException;

    KVTable getKVListByNamespace(
            final String namespace) throws RemotingException, MQClientException, InterruptedException;

    void deleteTopicInBroker(final Set<String> addrs, final String topic) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    void deleteTopicInNameServer(final Set<String> addrs,
                                 final String topic) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    void createAndUpdateKvConfig(String namespace, String key,
                                 String value) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    void deleteKvConfig(String namespace, String key) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException;

    List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp, boolean force)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String group, long timestamp, boolean isForce)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    void resetOffsetNew(String consumerGroup, String topic, long timestamp) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group,
                                                          String clientAddr) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;

    void createOrUpdateOrderConf(String key, String value,
                                 boolean isCluster) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    List<QueueTimeSpan> queryConsumeTimeSpan(final String topic,
                                             final String group) throws InterruptedException, MQBrokerException,
            RemotingException, MQClientException;

    boolean cleanExpiredConsumerQueue(String cluster) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;

    boolean cleanExpiredConsumerQueueByAddr(String addr) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;

    boolean cleanUnusedTopic(String cluster) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;

    boolean cleanUnusedTopicByAddr(String addr) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;


    ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup,
                                                        String clientId,
                                                        String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup,
                                                        String clientId,
                                                        String topic,
                                                        String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    List<MessageTrack> messageTrackDetail(
            MessageExt msg) throws RemotingException, MQClientException, InterruptedException,
            MQBrokerException;

    void cloneGroupOffset(String srcGroup, String destGroup, String topic, boolean isOffline) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException;

    BrokerStatsData viewBrokerStatsData(final String brokerAddr, final String statsName, final String statsKey)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQClientException,
            InterruptedException;

    Set<String> getClusterList(final String topic) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;

    ConsumeStatsList fetchConsumeStatsInBroker(final String brokerAddr, boolean isOrder,
                                               long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;

    Set<String> getTopicClusterList(
            final String topic) throws InterruptedException, MQBrokerException, MQClientException, RemotingException;


    /************************************** broker相关,以及从从broker数据（topic,topic配置，消费分组，消费分组配置，消息队列） **************************************/

    /**
     * 查询指定broker所有消费分组的配置（并安装topic分组）
     * 包括
     * Topic以及对应订阅该Topic SubscriptionGroupConfig（消费分组配置）
     * 数据版本来源（是否时master）
     *
     * @param brokerAddr    broker实例地址
     * @param timeoutMillis 超时时间
     * @return
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws MQBrokerException
     */
    SubscriptionGroupWrapper getAllSubscriptionGroup(final String brokerAddr,
                                                     long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQBrokerException;


    /**
     * 查询指定broker指定消费分组的消费分组配置
     *
     * @param addr
     * @param group
     * @return
     */
    SubscriptionGroupConfig examineSubscriptionGroupConfig(final String addr, final String group);


    /**
     * 向broker创建/更新 消费分组及其消费分组配置
     * 如果broker配置 autoCreateSubscriptionGroup=false,需要先手动创建
     * (这个信息存储在broker)
     *
     * @param addr
     * @param config
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void createAndUpdateSubscriptionGroupConfig(final String addr,
                                                final SubscriptionGroupConfig config) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;


    /**
     * 查询指定broker实例内部所有Topic配置信息
     * 包括
     * Topic以及对应TopicConfig(配置)
     * 数据版本来源（是否时master）
     *
     * @param brokerAddr    broker实例地址
     * @param timeoutMillis 超时时间
     * @return
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws MQBrokerException
     */
    TopicConfigSerializeWrapper getAllTopicGroup(final String brokerAddr,
                                                 long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQBrokerException;


    /**
     * 查询指定broker实例内部状态
     * 包括：
     * （消费消息TPS	昨日生产总数	昨日消费总数	今天生产总数	今天消费总数...）
     *
     * @param brokerAddr
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    KVTable fetchBrokerRuntimeStats(
            final String brokerAddr) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQBrokerException;


    /**
     * 更新指定broker实例内部属性
     *
     * @param brokerAddr broker实例地址
     * @param properties 内部属性
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws UnsupportedEncodingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    void updateBrokerConfig(final String brokerAddr, final Properties properties) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException;

    /**
     * 查询指定broker实例内部属性
     *
     * @param brokerAddr broker实例地址
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws UnsupportedEncodingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    Properties getBrokerConfig(final String brokerAddr) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException;


    /**
     * 更新指定broker实例，指定消费队列在指定消费分组的消费逻辑偏移
     *
     * @param brokerAddr
     * @param consumeGroup
     * @param mq
     * @param offset
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    void updateConsumeOffset(String brokerAddr, String consumeGroup, MessageQueue mq,
                             long offset) throws RemotingException, InterruptedException, MQBrokerException;


    /**
     * 查询消费队列中数据
     *
     * @param brokerAddr    broker ip address
     * @param topic         topic
     * @param queueId       id of queue
     * @param index         start offset
     * @param count         how many
     * @param consumerGroup group
     */
    QueryConsumeQueueResponseBody queryConsumeQueue(final String brokerAddr,
                                                    final String topic, final int queueId,
                                                    final long index, final int count, final String consumerGroup)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException;


    /**
     * 查询所有topic
     * <p>
     * 这里没有参数是根据当前namsserver，获取当前注册到nameserver所有集群所有broker中的所有topic
     *
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     */
    TopicList fetchAllTopicList() throws RemotingException, MQClientException, InterruptedException;


    /**
     * 查询指定集群所有topic
     *
     * @param clusterName
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     */
    TopicList fetchTopicsByCLuster(
            String clusterName) throws RemotingException, MQClientException, InterruptedException;




    /************************************** nameServer相关接口 **************************************/


    /**
     * 更新nameServer 属性
     */
    void updateNameServerConfig(final Properties properties,
                                final List<String> nameServers) throws InterruptedException, RemotingConnectException,
            UnsupportedEncodingException, RemotingSendRequestException, RemotingTimeoutException,
            MQClientException, MQBrokerException;

    /**
     * 获取nameServer 属性
     *
     * @return nameServer 属性
     */
    Map<String, Properties> getNameServerConfig(final List<String> nameServers) throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
            MQClientException, UnsupportedEncodingException;


    /**
     * 获取nameServer地址
     *
     * @return
     */
    List<String> getNameServerAddressList();


    /************************************** 消息相关接口 **************************************/


    boolean resumeCheckHalfMessage(String msgId)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    boolean resumeCheckHalfMessage(final String topic, final String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;
}
