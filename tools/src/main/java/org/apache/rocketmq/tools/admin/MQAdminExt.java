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

    /************************************** Topic 维度相关接口 **************************************/

    /**
     * 向指定broker节点创建/更新一个topic以及topic配置
     *
     * @param addr   broker节点某个实例地址
     * @param config topic配置
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void createAndUpdateTopicConfig(final String addr,
                                    final TopicConfig config) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    /**
     * 查询指定broker节点指定topic的topic的配置
     *
     * @param addr  broker节点某个实例地址
     * @param topic topic名称
     * @return
     */
    TopicConfig examineTopicConfig(final String addr, final String topic);


    /**
     * 删除多个broker节点上的topic的topic配置
     *
     * @param addrs 多个broker节点master broker实例地址
     * @param topic topic名称
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void deleteTopicInBroker(final Set<String> addrs, final String topic) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * 删除多个nameserver topic(核心是用来删除路由信息)
     *
     * @param addrs 多个nameserver地址
     * @param topic topic名称
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void deleteTopicInNameServer(final Set<String> addrs,
                                 final String topic) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    /**
     * 查询所有topic
     * <p>
     * 1 根据当前namsserver，获取当前注册到nameserver所有集群
     * 2 获取所有集群所有broker节点
     * 3 获取所有broker节点配置的topic
     *
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     */
    TopicList fetchAllTopicList() throws RemotingException, MQClientException, InterruptedException;


    /**
     * 获取指定Topic对应 TopicStatsTable 状态表格
     * TopicStatsTable包括:
     * 1 存储该Topic每个MessageQueue信息，
     * 2 以及每个MessageQueue对应的 TopicOffset(消息移量数据，包括存储逻辑最大最小偏移量，最后更新时间戳）
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
     * 从nameserver获取指定Topic对应的 TopicRouteData 主题路由信息
     * TopicRouteData包括:
     * 1 topic在不同broker节点配置信息（读队列数量，写队列数量，权限..）
     * 2 存储topic消息每个broker节点数据（包括每个broker节点内所有broker实例类型和地址..）
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


    /**
     * 查询当前topic关联所有集群（空实现）
     *
     * @param topic topic名称
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws MQClientException
     * @throws InterruptedException
     */
    Set<String> getClusterList(final String topic) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;


    /**
     * 查询当前topic关联所有集群
     *
     * @param topic topic名称
     * @return
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws MQClientException
     * @throws RemotingException
     */
    Set<String> getTopicClusterList(
            final String topic) throws InterruptedException, MQBrokerException, MQClientException, RemotingException;


    /************************************** 生产组相关接口 **************************************/


    /**
     * 查询指定指定producerGroup（生产分组）对应 ProducerConnection(生产分组连接信息)
     * <p>
     * ProducerConnection包括:
     * 终端连接信息列表，其中连接信息包括（id,地址，语言，版本）
     *
     * @param producerGroup 生产分组
     * @param topic         topic名称
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    ProducerConnection examineProducerConnectionInfo(final String producerGroup,
                                                     final String topic) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException;


    /************************************** 消费分组相关接口 **************************************/


    /**
     * 向指定broker节点创建/更新 一个消费分组以及消费分组配置
     * <p>
     * 如果broker配置 autoCreateSubscriptionGroup=false,需要先手动创建否则客户端不无法接收消息
     *
     * @param addr   broker节点某个实例地址
     * @param config 消费分组配置
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void createAndUpdateSubscriptionGroupConfig(final String addr,
                                                final SubscriptionGroupConfig config) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;

    /**
     * 向指定broker节点删除指定消费分组以及消费分组配置
     * <p>
     * 如果broker配置 autoCreateSubscriptionGroup=false,需要先手动创建否则客户端不无法接收消息
     *
     * @param addr      broker节点某个实例地址
     * @param groupName 消费分组
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void deleteSubscriptionGroup(final String addr, String groupName) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * 查询指定指定消费分组对应ConsumerConnection(消费组连接信息)
     * ConsumerConnection包括:
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
     * 查询指定消费分组对应ConsumeStats（消费分组状态信息）
     * ConsumeStats包括:
     * 1 每个消费分组订阅MessageQueue(消息队列)信息
     * 2 订阅MessageQueue(消息队列)对应的OffsetWrapper(消费进度)
     * 3 消费分组Tps
     * 4 消费分组消息堆积
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
     * 查询指定topic指定消费分组对应ConsumeStats（消费分组状态信息）
     * ConsumeStats包括:
     * 1 每个消费分组订阅MessageQueue(消息队列)信息
     * 2 订阅MessageQueue(消息队列)对应的OffsetWrapper(消费进度)
     * 3 消费分组Tps
     * 4 消费分组消息堆积
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
     * 查询指定topic指定消费分组对应  QueueTimeSpan列表（QueueTimeSpan信息）
     * QueueTimeSpan 包括：
     * 消息队列信息
     * minTimeStamp 偏移量最小消息产生时间
     * maxTimeStamp 偏移量最大消息产生时间
     * consumeTimeStamp 当前消费分组消费消息产生时间
     * maxTimeStamp-consumeTimeStamp 延时时间
     *
     * @param topic
     * @param group
     * @return
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    List<QueueTimeSpan> queryConsumeTimeSpan(final String topic,
                                             final String group) throws InterruptedException, MQBrokerException,
            RemotingException, MQClientException;


    /**
     * 重置指定消费分组指定topic消费进度（按照时间戳）（老版本）
     * <p>
     * 1 查询当前消费分组对应Topic关联所有消息队列
     * 2 重新指定时间戳对应最大逻辑偏移量
     * 3 重置所有消费队列偏移量未2中计算的值
     *
     * @param consumerGroup 消费分组
     * @param topic         topic名称
     * @param timestamp     指定时间戳
     * @param force         如果查询指定时间戳在消费队列逻辑偏移量<当前消费逻辑偏移量做当前队列不重置进度
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp, boolean force)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * 重置指定消费分组指定topic消费进度（按照时间戳）（新版本）
     * <p>
     * 1 查询当前消费分组对应Topic关联所有消息队列
     * 2 重新指定时间戳对应最大逻辑偏移量
     * 3 重置所有消费队列偏移量未2中计算的值
     *
     * @param topic     topic名称
     * @param group     消费分组
     * @param timestamp 指定时间戳
     * @param isForce   如果查询指定时间戳在消费队列逻辑偏移量<当前消费逻辑偏移量做当前队列不重置进度
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String group, long timestamp, boolean isForce)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * 内部实现调用resetOffsetByTimestamp一样，isForce为true
     *
     * @param topic         topic名称
     * @param consumerGroup 消费分组
     * @param timestamp     指定时间戳
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void resetOffsetNew(String consumerGroup, String topic, long timestamp) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * 查询指定指定消费分组指定客户端地址指定topic消费信息
     * <p>
     * Map<String, Map<MessageQueue, Long>>
     * <p>
     * String 客户端地址
     * MessageQueue 消息队列
     * Long 消费逻辑偏移量
     *
     * @param topic      topic名称
     * @param group      消费分组
     * @param clientAddr 消费实例客户端地址
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group,
                                                          String clientAddr) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;


    /**
     * 查询指定指定消费分组指定客户端Id对应ConsumerRunningInfo(消费实例信息)
     * ConsumerRunningInfo包括:
     * <p>
     * 1 该消费实例上所有配置Topic订阅配置信息
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
     * 克隆指定topic,指定消费分组的消费进度到指定消费分组
     *
     * @param srcGroup  原消费分组
     * @param destGroup 目标消费分组
     * @param topic     消息目标
     * @param isOffline 是否在线
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    void cloneGroupOffset(String srcGroup, String destGroup, String topic, boolean isOffline) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException;


    /************************************** 集群相关接口 **************************************/

    /**
     * 查询集群信息（包含集群关联所有broker节点，以及broker节点数据（其中包括所有broekr实例类型以及地址））
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

    /**
     * 查询集群ACL数据版本
     *
     * @param addr 传入集群中某个broker实例地址
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    ClusterAclVersionInfo examineBrokerClusterAclVersionInfo(final String addr) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    /**
     * 清理集群过期的消息队列
     *
     * @param cluster 集群名称
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws MQClientException
     * @throws InterruptedException
     */
    boolean cleanExpiredConsumerQueue(String cluster) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;

    /**
     * 清理集群未使用的topic
     *
     * @param cluster 集群名称
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws MQClientException
     * @throws InterruptedException
     */
    boolean cleanUnusedTopic(String cluster) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;


    /************************************** broker相关 **************************************/

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
     * 查询指定broker实例内部所有Topic配置信息
     * <p>
     * TopicConfigSerializeWrapper 包括
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


    /**
     * 查询某个borker实例内指定topic配置
     * （broker节点内所有broker实例topic配置相同）
     *
     * @param addr
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    AclConfig examineBrokerClusterAclConfig(final String addr) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * @param addr
     * @param plainAccessConfig
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void createAndUpdatePlainAccessConfig(final String addr, final PlainAccessConfig plainAccessConfig) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    /**
     * @param addr
     * @param accessKey
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void deletePlainAccessConfig(final String addr, final String accessKey) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    /**
     * @param addr
     * @param globalWhiteAddrs
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void updateGlobalWhiteAddrConfig(final String addr, final String globalWhiteAddrs) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    /**
     * @param brokerAddr
     * @param statsName
     * @param statsKey
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws MQClientException
     * @throws InterruptedException
     */
    BrokerStatsData viewBrokerStatsData(final String brokerAddr, final String statsName, final String statsKey)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQClientException,
            InterruptedException;


    /**
     * @param brokerAddr
     * @param isOrder
     * @param timeoutMillis
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws MQClientException
     * @throws InterruptedException
     */
    ConsumeStatsList fetchConsumeStatsInBroker(final String brokerAddr, boolean isOrder,
                                               long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;


    /**
     * 清理指定broker节点过期消息队列
     *
     * @param addr
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws MQClientException
     * @throws InterruptedException
     */
    boolean cleanExpiredConsumerQueueByAddr(String addr) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;


    /**
     * 清理指定broker节点未使用topic
     *
     * @param addr
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws MQClientException
     * @throws InterruptedException
     */
    boolean cleanUnusedTopicByAddr(String addr) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQClientException, InterruptedException;


    /**
     * @param namesrvAddr
     * @param brokerName
     * @return
     * @throws RemotingCommandException
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws InterruptedException
     * @throws MQClientException
     */
    int wipeWritePermOfBroker(final String namesrvAddr, String brokerName) throws RemotingCommandException,
            RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException;


    /************************************** nameServer相关接口 **************************************/

    /**
     * 获取当前客户端配置nameServer地址列表
     *
     * @return
     */
    List<String> getNameServerAddressList();


    /**
     * 更新指定nameServer 属性
     */
    void updateNameServerConfig(final Properties properties,
                                final List<String> nameServers) throws InterruptedException, RemotingConnectException,
            UnsupportedEncodingException, RemotingSendRequestException, RemotingTimeoutException,
            MQClientException, MQBrokerException;

    /**
     * 获取指定nameServer属性配置
     *
     * @return nameServer 属性配置
     */
    Map<String, Properties> getNameServerConfig(final List<String> nameServers) throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
            MQClientException, UnsupportedEncodingException;


    /**
     * 向nameserver指定namespace，指定key的注册value（空实现）
     *
     * @param namespace 命名空间
     * @param key       key名称
     * @param value     key值
     */
    void putKVConfig(final String namespace, final String key, final String value);


    /**
     * 向nameserver指定namespace，指定key的注册value
     *
     * @param namespace 命名空间
     * @param key       key名称
     * @param value     key值
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void createAndUpdateKvConfig(String namespace, String key,
                                 String value) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * 向nameserver删除指定namespace，指定key的值
     *
     * @param namespace 命名空间
     * @param key       key名称
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void deleteKvConfig(String namespace, String key) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException;


    /**
     * 向nameserver创建跟新 NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG空间下的key,value值
     * <p>
     * NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFI 用来存储指定topic配置（用来保证严格的顺序）参数NamesrvConfig.orderMessageEnable
     *
     * @param key       key值
     * @param value     value值
     * @param isCluster 是否是集群
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void createOrUpdateOrderConf(String key, String value,
                                 boolean isCluster) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    /**
     * 查询指定namespace，指定key的value
     *
     * @param namespace 命名空间
     * @param key       key名称
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     */
    String getKVConfig(final String namespace,
                       final String key) throws RemotingException, MQClientException, InterruptedException;

    /**
     * 查询指定namespace所有key-value
     *
     * @param namespace 命名空间
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     */
    KVTable getKVListByNamespace(
            final String namespace) throws RemotingException, MQClientException, InterruptedException;


    /************************************** 消息相关接口 **************************************/

    /**
     * 检查事务消息
     *
     * @param msgId 消息id
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    boolean resumeCheckHalfMessage(String msgId)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    /**
     * 检查事务消息
     *
     * @param topic topic名称
     * @param msgId 消息id
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    boolean resumeCheckHalfMessage(final String topic, final String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    /**
     * 查询指定消息的消息轨迹
     *
     * @param msg 消息
     * @return 消息规矩列表
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    List<MessageTrack> messageTrackDetail(
            MessageExt msg) throws RemotingException, MQClientException, InterruptedException,
            MQBrokerException;


    /**
     * 直接消费消息
     *
     * @param consumerGroup 消费分组
     * @param clientId      消费分组客户端id
     * @param msgId         消息id
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup,
                                                        String clientId,
                                                        String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    /**
     * 直接消费消息
     *
     * @param consumerGroup 消费分组
     * @param clientId      消费分组客户端id
     * @param topic         topic名称
     * @param msgId         消息id
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup,
                                                        String clientId,
                                                        String topic,
                                                        String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;
}
