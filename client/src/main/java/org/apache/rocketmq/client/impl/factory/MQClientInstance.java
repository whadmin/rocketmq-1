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
package org.apache.rocketmq.client.impl.factory;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.RebalanceService;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * MQ客户端实例对象
 * 1 MQClientInstance 服务于如下4个角色 RPC远程调用客户端通用实现
 * <p>
 * DefaultMQAdminExtImpl            运维系统Admin
 * DefaultMQProducerImpl            生成消息Producer
 * DefaultMQPushConsumerImpl        推送消息Consumer
 * DefaultMQPullConsumerImpl        拉取消息Consumer
 * <p>
 * 2 MQClientInstance 通过 MQClientManager工厂对象创建，每一个MQClientInstance表示一个进程
 * <p>
 * 3 MQ消费客户端实例 MQClientInstance（进程）和消费分组关系（多对多）
 * 1一个消费分组可以存在多个消费者，每一个消费者对应到一个MQ客户端实例 MQClientInstance 一个进程
 * 2一个MQ客户端实例 MQClientInstance，或一个进程内也可以可以配置多个消费分组
 * MQ消费客户端实例 MQClientInstance（进程）和生产分组关系（多对多）
 * 1一个生产分组可以存在多个消费者，每一个消费者对应到一个MQ客户端实例 MQClientInstance 一个进程
 * 2一个MQ客户端实例 MQClientInstance，或一个进程内也可以可以配置多个生产分组
 * <p>
 * 4 核心功能
 * 4.1 定时任务 startScheduledTask
 * 4.2 获取获取注册到 MQClientInstance客户端实例的所有MQConsumerInner对每个MQConsumerInner执行负责均衡
 * 4.3 Producer/Consumer 通用功能
 */
public class MQClientInstance {

    /**
     * 锁超时时间
     */
    private final static long LOCK_TIMEOUT_MILLIS = 3000;

    /**
     * 内部日志
     */
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * Namesrv同步锁
     */
    private final Lock lockNamesrv = new ReentrantLock();

    /**
     * Heartbeat同步锁
     */
    private final Lock lockHeartbeat = new ReentrantLock();

    /**
     * 客户端ID
     */
    private final String clientId;

    /**
     * 客户端配置
     */
    private final ClientConfig clientConfig;

    /**
     * MQClientInstance坐标
     * MQClientManager创建MQClientInstance计数累加
     */
    private final int instanceIndex;


    /**
     * MQClientInstance 启动时间戳
     */
    private final long bootTimestamp = System.currentTimeMillis();

    /**
     * 保存使用当前对象作为客户端的MQProducerInner ConcurrentMap集合  key:groupName-value:MQProducerInner
     */
    private final ConcurrentMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<String, MQProducerInner>();

    /**
     * 保存使用当前对象作为客户端的MQConsumerInner ConcurrentMap集合  key:groupName-value:MQConsumerInner
     */
    private final ConcurrentMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();

    /**
     * 保存使用当前对象作为客户端的MQConsumerInner ConcurrentMap集合  key:groupName-value:MQConsumerInner
     */
    private final ConcurrentMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<String, MQAdminExtInner>();

    /**
     * NettyRPC客户端配置
     */
    private final NettyClientConfig nettyClientConfig;

    /**
     * MQ远程客户端实现
     */
    private final MQClientAPIImpl mQClientAPIImpl;

    /**
     * MQ管理实现
     */
    private final MQAdminImpl mQAdminImpl;

    /**
     * 从nameserver获取不同topic以及对应的路由信息 key:Topic-value:TopicRouteData
     */
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();

    /**
     * 从nameserver获取不同的broker节点异以及对应节点内不同类型broker和地址端口
     */
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentHashMap<String, HashMap<Long, String>>();

    /**
     * 从nameserver获取不同的broker节点异以及节点内broker实例地址和在线状态
     */
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable =
            new ConcurrentHashMap<String, HashMap<String, Integer>>();

    /**
     * 定时任务线程池
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });

    /**
     * 处理服务端发送请求处理类
     */
    private final ClientRemotingProcessor clientRemotingProcessor;

    /**
     * 拉取消息服务【为MQConsumerInner提供服务】
     */
    private final PullMessageService pullMessageService;

    /**
     * 均衡消息服务【为MQConsumerInner提供服务】
     */
    private final RebalanceService rebalanceService;

    /**
     * 内置DefaultMQProducer【为MQProducerInner提供服务】
     */
    private final DefaultMQProducer defaultMQProducer;

    /**
     * Consumer统计服务【为MQConsumerInner提供服务】
     */
    private final ConsumerStatsManager consumerStatsManager;

    /**
     * 记录发送心跳包计数累计次数
     */
    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);

    /**
     * 服务状态
     */
    private ServiceState serviceState = ServiceState.CREATE_JUST;

    /**
     * 随机数
     */
    private Random random = new Random();

    /**
     * 构造MQClientInstance
     *
     * @param clientConfig  客户端配置
     * @param instanceIndex 实例下标
     * @param clientId      客户端id
     */
    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    /**
     * 构造DefaultMQProducerImpl设置DefaultMQProducer
     *
     * @param clientConfig  客户端配置
     * @param instanceIndex 实例下标
     * @param clientId      客户端id
     * @param rpcHook       RPC钩子
     */
    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        //设置客户端配置
        this.clientConfig = clientConfig;
        //设置MQClientInstance坐标
        this.instanceIndex = instanceIndex;

        //创建NettyRPC客户端配置
        this.nettyClientConfig = new NettyClientConfig();
        //设置NettyRPC客户端回调线程池中线程数量
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        //设置NettyRPC客户端TLS
        this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());

        //创建处理服务端发送请求处理类
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);

        //创建NettyRPC客户端
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig);

        //客户端配置设置namesrvAddr地址
        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }
        //设置客户端ID
        this.clientId = clientId;

        //创建MQ管理实现
        this.mQAdminImpl = new MQAdminImpl(this);

        //创建拉取消息服务【为MQConsumerInner提供服务】
        this.pullMessageService = new PullMessageService(this);

        //创建均衡消息服务【为MQConsumerInner提供服务】
        this.rebalanceService = new RebalanceService(this);

        //创建内置DefaultMQProducer【为MQProducerInner提供服务】
        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        //重置客户端配置
        this.defaultMQProducer.resetClientConfig(clientConfig);

        //创建Consumer统计服务【为MQConsumerInner提供服务】
        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

        log.info("Created a new client Instance, InstanceIndex:{}, ClientID:{}, ClientConfig:{}, ClientVersion:{}, SerializerType:{}",
                this.instanceIndex,
                this.clientId,
                this.clientConfig,
                MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    /***************** MQClientInstance 启动关闭开始  *****************/

    /**
     * 启动 MQClientInstance 客户端实例
     */
    public void start() throws MQClientException {

        synchronized (this) {
            //判断当前服务状态
            switch (this.serviceState) {
                //如果服务状态刚刚创建
                case CREATE_JUST:
                    //设置服务状态启动失败
                    this.serviceState = ServiceState.START_FAILED;
                    //如果未设置namsser地址获取获取远程http服务器上面nameSrv地址
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    //启动MQ客户端
                    this.mQClientAPIImpl.start();
                    // 启动定时任务
                    this.startScheduledTask();
                    // 启动拉取服务
                    this.pullMessageService.start();
                    // 启动均衡服务
                    this.rebalanceService.start();
                    // 启动默认defaultMQProducer
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    // 打印日志
                    log.info("the client factory [{}] start OK", this.clientId);
                    // 设置服务状态已运行
                    this.serviceState = ServiceState.RUNNING;
                    break;
                //如果服务启动失败
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    /**
     * 关闭 MQClientInstance 客户端实例
     */
    public void shutdown() {
        // consumerTable 不为空 返回
        if (!this.consumerTable.isEmpty())
            return;

        // adminExtTable 不为空 返回
        if (!this.adminExtTable.isEmpty())
            return;

        // producerTable 不为空 返回
        if (this.producerTable.size() > 1)
            return;

        synchronized (this) {
            //判断当前服务状态
            switch (this.serviceState) {
                //如果服务刚刚创建
                case CREATE_JUST:
                    break;
                //如果服务正在运行
                case RUNNING:
                    //关闭defaultMQProducer
                    this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);
                    //设置服务状态服务正在关闭
                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    //关闭拉取消息服务
                    this.pullMessageService.shutdown(true);
                    //关闭定时任务
                    this.scheduledExecutorService.shutdown();
                    //关闭MQ客户端实现
                    this.mQClientAPIImpl.shutdown();
                    //关闭均衡服务
                    this.rebalanceService.shutdown();
                    //从MQClientManager 删除当前MQClientInstance 客户端实例
                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    //打印日志
                    log.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                //如果服务正在关闭
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }


    /***************** MQClientInstance 启动关闭结束  *****************/


    /***************** MQClientInstance 启动定时任务开始  *****************/

    /**
     * 启动定时任务
     */
    private void startScheduledTask() {

        /**
         * 如果未配置nameser地址，定时通过topAddressing 获取nameSrv地址
         */
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        /**
         * 定时获取MQClientInstance客户端实例中MQConsumerInner/MQProducerInner配置topic
         * 从Namerser获取每一个topic对应的路由信息，更新到 MQClientInstance客户端实例 属性中
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);

        /**
         * 1 定时获取注册到 MQClientInstance客户端实例 的所有broker实例对离线broker进行清理
         *   这里判断broker实例是否离线来源于当前 MQClientInstance客户端实例 所有路由信息
         *
         * 2 获取注册到 MQClientInstance客户端实例 的所有broker实例发送 MQClientInstance客户端实例 HeartbeatData心跳数据
         *   获取注册到 MQClientInstance客户端实例 的所有MQConsumerInner，并对MQConsumerInner中支持filterServer订阅配置进行上传注册。
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    MQClientInstance.this.cleanOfflineBroker();
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.persistAllConsumerOffset();
                } catch (Exception e) {
                    log.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.adjustThreadPool();
                } catch (Exception e) {
                    log.error("ScheduledTask adjustThreadPool exception", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    /***************** updateTopicRouteInfoFromNameServer 开始  *****************/

    /**
     * 获取MQClientInstance客户端实例中MQConsumerInner/MQProducerInner配置topic
     * 从Namerser获取每一个topic对应的路由信息，更新到 MQClientInstance客户端实例 属性中
     */
    public void updateTopicRouteInfoFromNameServer() {
        //记录需要获取路由信息topic
        Set<String> topicList = new HashSet<String>();

        //获取注册到 MQClientInstance客户端实例 的所有MQConsumerInner
        //获取MQConsumerInner所有SubscriptionData订阅配置，
        //将所有订阅配置的topic添加到topicList
        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            topicList.add(subData.getTopic());
                        }
                    }
                }
            }
        }
        //获取注册到 MQClientInstance客户端实例 的所有MQProducerInner
        //获取所有发送消息topic添加到topicList
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }
        //遍历topicList，从Namerser获取每一个topic对应的路由信息，更新到 MQClientInstance客户端实例 属性中
        for (String topic : topicList) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    /**
     * 1 从Namerser获取topic对应的路由信息
     * 2 更新到 MQClientInstance客户端实例 属性中
     * 其中包括：
     * topicRouteTable 不同topic以及对应的路由信息
     * brokerAddrTable 不同的broker节点异以及对应节点内不同类型broker和地址端口
     * 所有注册MQProducerInner 更新topic和topic对应发布信息
     * 所有注册MQConsumerInner 更新topic和Set<MessageQueue> 消息队列集合
     *
     * @param topic topic名称
     * @return 是否成功
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }


    /**
     * 1 从Namerser获取topic对应的路由信息
     * 2 更新到MQClientInstance客户端实例 属性中
     * 其中包括：
     * topicRouteTable 不同topic以及对应的路由信息
     * brokerAddrTable 不同的broker节点异以及对应节点内不同类型broker和地址端口
     * 所有注册MQProducerInner 更新topic和topic对应发布信息
     * 所有注册MQConsumerInner 更新topic和Set<MessageQueue> 读取消息队列集合
     *
     * @param topic             topic名称
     * @param isDefault         是否获取是默认topic="TBW102"的路由信息
     * @param defaultMQProducer
     * @return 是否成功
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault,
                                                      DefaultMQProducer defaultMQProducer) {
        try {
            //Namesrv同步锁加锁,超时
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    //如果参数指定的是默认topic="TBW102"
                    if (isDefault && defaultMQProducer != null) {
                        //获取topic="TBW102"的路由信息
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(),
                                1000 * 3);
                        //重置"TBW102"的路由信息中配置
                        if (topicRouteData != null) {
                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    }
                    //查询指定topic的路由信息
                    else {
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    }
                    //如果topic路由信息不为空
                    if (topicRouteData != null) {
                        //获取本地旧的topic路由信息
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        //判断路由信息是否发送改变（本地和远程获取的对比）
                        boolean changed = topicRouteDataIsChange(old, topicRouteData);
                        //如果发送改变还需要通过客户端MQProducerInner，MQConsumerInner判断是否需要更新
                        if (!changed) {
                            //如果topic本地和远程获取路由信息是否需要更新
                            //通过客户端MQProducerInner，MQConsumerInner配置判断
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }
                        //发送改变
                        if (changed) {
                            //clone TopicRouteData
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                            //更新不同的broker节点异以及对应节点内不同类型broker和地址端口
                            //如果某个broker下线,那么brokerAddrTable数据是不会删除的，通过cleanOfflineBroker() 定时从brokerAddrTable清理掉下线的broker
                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }


                            //topicRouteData转换成TopicPublishInfo
                            //更新MQProducerInner 发送消息topic和对应路由信息
                            {
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                publishInfo.setHaveTopicRouterInfo(true);
                                Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQProducerInner> entry = it.next();
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        //topic和topic对应发布信息
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }

                            //获取topicRouteData路由信息中可以读取MessageQueue数量,创建MessageQueue添加到Set<MessageQueue> mqList集合类
                            //将mqList集合类更新同步到所有使用当前对象作为客户端ConsumerInner.rebalanceImpl.topicSubscribeInfoTable
                            {
                                //获取topicRouteData路由信息中可以读取MessageQueue数量,创建MessageQueue添加到Set<MessageQueue> mqList集合类
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQConsumerInner> entry = it.next();
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);

                            //更新topic路由信息
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                    }
                } catch (MQClientException e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } catch (RemotingException e) {
                    log.error("updateTopicRouteInfoFromNameServer Exception", e);
                    throw new IllegalStateException(e);
                } finally {
                    //Namesrv同步锁解锁
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }
        return false;
    }

    /**
     * 判断路由信息是否发送改变（本地和远程获取的对比）
     *
     * @param olddata
     * @param nowdata
     * @return
     */
    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = olddata.cloneTopicRouteData();
        TopicRouteData now = nowdata.cloneTopicRouteData();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);
    }


    /**
     * 如果topic本地和远程获取路由信息是否需要更新
     * 通过客户端MQProducerInner，MQConsumerInner配置判断
     *
     * @param topic
     * @return
     */
    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        //客户端MQProducerInner判断
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }
        //客户端MMQConsumerInner判断
        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isSubscribeTopicNeedUpdate(topic);
                }
            }
        }

        return result;
    }


    /**
     * topicRouteData转换成TopicPublishInfo
     * TopicPublishInfo 表示给MQProducerInner使用topic路由信息
     * 我们会把转换获取TopicPublishInfo更新同步到所有使用当前对象作为客户端MQProducerInner.topicPublishInfoTable】
     *
     * @param topic
     * @param route
     * @return
     */
    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        //创建TopicPublishInfo
        TopicPublishInfo info = new TopicPublishInfo();
        //设置路由信息
        info.setTopicRouteData(route);
        //判断路由信息中topic需要保证严格顺序
        //如果需要保证严格顺序，通过route.getOrderTopicConf()获取MessageQueue消息队列列表信息
        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {
            //通过route.getOrderTopicConf()获取MessageQueue消息队列列表信息
            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }
            }

            info.setOrderTopic(true);
        }
        //如果路由信息中topic不需要保证严格顺序
        //从route.getQueueDatas()获取MessageQueue消息队列列表信息
        else {
            List<QueueData> qds = route.getQueueDatas();
            //排序topic 在指定broker节点配置信息列表
            Collections.sort(qds);
            //遍历topic 在指定broker节点配置信息列表
            for (QueueData qd : qds) {
                //指定broker节点配置信息列表是否可写入
                if (PermName.isWriteable(qd.getPerm())) {
                    //记录BrokerData 用来描述一个Broker节点数据
                    BrokerData brokerData = null;
                    for (BrokerData bd : route.getBrokerDatas()) {
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }
                    //不存在 Broker节点数据
                    if (null == brokerData) {
                        continue;
                    }
                    //判断是否不存在master节点
                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }
                    //按照配置构造MessageQueue消息队列
                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(mq);
                    }
                }
            }
            //设置不支撑严格顺序
            info.setOrderTopic(false);
        }
        //返回
        return info;
    }

    /**
     * 获取topicRouteData路由信息中可以读取MessageQueue数量,创建MessageQueue添加到Set<MessageQueue> mqList集合类
     *
     * @param topic
     * @param route
     * @return
     */
    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<MessageQueue>();
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            if (PermName.isReadable(qd.getPerm())) {
                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        }
        return mqList;
    }

    /***************** cleanOfflineBroker 开始  *****************/


    /**
     * 获取注册到 MQClientInstance客户端实例 的所有broker实例对离线broker进行清理
     * <p>
     * 这里判断broker实例是否离线来源于当前 MQClientInstance客户端实例 所有路由信息
     */
    private void cleanOfflineBroker() {
        try {
            //Namesrv同步锁加锁
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                try {
                    //记录更新后brokerAddrTable（broker节点异以及对应节点内不同类型broker和地址端口）
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<String, HashMap<Long, String>>();
                    //获取原始brokerAddrTable迭代器
                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    //对brokerAddrTable进行迭代
                    while (itBrokerTable.hasNext()) {
                        //获取entry
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        //获取broker节点
                        String brokerName = entry.getKey();
                        //获取broker节点内broker实例状态和地址
                        HashMap<Long, String> oneTable = entry.getValue();

                        //记录当前broker节点内broker实例状态和地址的克隆
                        HashMap<Long, String> cloneAddrTable = new HashMap<Long, String>();
                        cloneAddrTable.putAll(oneTable);

                        //获取cloneAddrTable迭代器
                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        //对cloneAddrTable进行迭代
                        while (it.hasNext()) {
                            //获取Entry
                            Entry<Long, String> ee = it.next();
                            //获取broker地址
                            String addr = ee.getValue();
                            //判断broker地址是否存在 MQClientInstance客户端实例 topicRouteTable 路由信息中，
                            //如果不存在则从cloneAddrTable删除
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        //如果当前节点broker实例都不在路由信息中则删除itBrokerTable，添加到updatedTable
                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }
                    //如果存在更新 updatedTable，则覆盖添加到brokerAddrTable
                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    //Namesrv同步锁解锁
                    this.lockNamesrv.unlock();
                }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }

    /**
     * 判断broker地址是否存在 MQClientInstance客户端实例 topicRouteTable 路由信息中
     *
     * @param addr broker实例地址
     * @return
     */
    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
        //获取topicRouteTable迭代器
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        //对topicRouteTable迭代
        while (it.hasNext()) {
            //获取Entry
            Entry<String, TopicRouteData> entry = it.next();
            //获取当前topic路由信息
            TopicRouteData topicRouteData = entry.getValue();
            //获取当前topic BrokerData broker节点数据集合
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            //遍历 BrokerData broke节点数据集合
            for (BrokerData bd : bds) {
                //判断地址是否不为null
                if (bd.getBrokerAddrs() != null) {
                    //当前broker地址是否存在于其中
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist)
                        return true;
                }
            }
        }
        return false;
    }


    /***************** sendHeartbeatToAllBrokerWithLock 开始  *****************/

    /**
     * 1 获取注册到 MQClientInstance客户端实例 的所有broker实例发送 MQClientInstance客户端实例 HeartbeatData心跳数据
     * 2 获取注册到 MQClientInstance客户端实例 的所有MQConsumerInner，并对MQConsumerInner中支持filterServer订阅配置进行上传注册。
     */
    public void sendHeartbeatToAllBrokerWithLock() {
        //获取心跳锁
        if (this.lockHeartbeat.tryLock()) {
            try {
                //获取注册到 MQClientInstance客户端实例 的所有broker实例发送 MQClientInstance客户端实例 HeartbeatData心跳数据
                this.sendHeartbeatToAllBroker();
                //获取注册到 MQClientInstance客户端实例 的所有MQConsumerInner，并对MQConsumerInner中支持
                //filterServer订阅配置进行上传注册。
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                //解心跳锁
                this.lockHeartbeat.unlock();
            }
        } else {
            //未获取锁打印日志
            log.warn("lock heartBeat, but failed.");
        }
    }

    /**
     * 获取注册到 MQClientInstance客户端实例 的所有broker实例发送 MQClientInstance客户端实例 HeartbeatData心跳数据
     * <p>
     * 1 准备 MQClientInstance客户端实例 HeartbeatData 心跳数据
     * 2 如果HeartbeatData 心跳数据内部如果生产者和消费者心跳数据都为空直接返回
     * 3 获取注册客户端实例注册的所有broker实例作为发送发送broker实例
     * 4 如果消费者心跳数据为空，且当前broker节点类型非master则从发送broker实例中过滤
     * 5 向所有发送broker实例发送客户端实例HeartbeatData心跳数据
     */
    private void sendHeartbeatToAllBroker() {
        //准备 MQClientInstance客户端实例 HeartbeatData 心跳数据
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();

        //记录HeartbeatData中生产者心跳数据是否为空
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        //记录HeartbeatData中消费者心跳数据是否为空
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        //如果生产者和消费者心跳数据都为空直接返回
        if (producerEmpty && consumerEmpty) {
            log.warn("sending heartbeat, but no consumer and no producer");
            return;
        }

        //判断brokerAddrTable是否不为空（获取路由信息的时候已初始化）
        if (!this.brokerAddrTable.isEmpty()) {
            //发送心跳包计数累计次数+1
            long times = this.sendHeartbeatTimesTotal.getAndIncrement();
            //获取brokerAddrTable所有Entry
            Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
            //遍历brokerAddrTable
            while (it.hasNext()) {
                //获取entry
                Entry<String, HashMap<Long, String>> entry = it.next();
                //获取broker节点名称
                String brokerName = entry.getKey();
                //获取broker节点内所有broker实例类型和地址
                HashMap<Long, String> oneTable = entry.getValue();
                //判断oneTable是否不为空
                if (oneTable != null) {
                    //遍历oneTable
                    for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                        //获取broker节点某个broker实例类型
                        Long id = entry1.getKey();
                        //获取broker节点某个broker实例地址
                        String addr = entry1.getValue();
                        //判断addr是否不为空
                        if (addr != null) {
                            //如果消费者心跳数据为空，且当前broker节点类型非master跳过
                            if (consumerEmpty) {
                                if (id != MixAll.MASTER_ID)
                                    continue;
                            }
                            try {
                                //向broker发送心跳数据(生产者实例/消费者实例),返回发送broker实例所在broker节点版本号
                                int version = this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);

                                //如果发送broker实例所在broker节点没记录版本号则初始化
                                if (!this.brokerVersionTable.containsKey(brokerName)) {
                                    this.brokerVersionTable.put(brokerName, new HashMap<String, Integer>(4));
                                }
                                this.brokerVersionTable.get(brokerName).put(addr, version);

                                //每发送20次心跳打印日志
                                if (times % 20 == 0) {
                                    log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                                    log.info(heartbeatData.toString());
                                }
                            } catch (Exception e) {
                                if (this.isBrokerInNameServer(addr)) {
                                    log.info("send heart beat to broker[{} {} {}] failed", brokerName, id, addr, e);
                                } else {
                                    log.info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName,
                                            id, addr, e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 准备 MQClientInstance客户端实例 HeartbeatData 心跳数据
     *
     * @return
     */
    private HeartbeatData prepareHeartbeatData() {
        //创建心跳数据
        HeartbeatData heartbeatData = new HeartbeatData();
        //设置客户端ID
        heartbeatData.setClientID(this.clientId);

        //为每个注册到客户端实例的MQConsumerInner创建一个 ConsumerData消费者心跳数据，添加到 HeartbeatData
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());

                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }

        //为每个注册到客户端实例的MQProducerInner创建一个 ProducerData 生产者心跳数据，添加到 HeartbeatData
        for (Map.Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());
                heartbeatData.getProducerDataSet().add(producerData);
            }
        }
        //返回心跳数据
        return heartbeatData;
    }

    /**
     * 获取注册到 MQClientInstance客户端实例 的所有MQConsumerInner，并对MQConsumerInner中支持
     * filterServer订阅配置进行上传注册。
     * <p>
     * 1 获取注册MQClientInstance客户端实例 的所有MQConsumerInner
     * 2 获取MQConsumerInner所有订阅配置
     * 3 过滤支持filterServer订阅配置
     * 4 对支持filterServer订阅配置在其配置filterServer上进行注册
     */
    private void uploadFilterClassSource() {
        //获取consumerTable所有Entry的迭代器
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        //对consumerTable所有Entry进行迭代
        while (it.hasNext()) {
            //获取consumerTable指定Entry
            Entry<String, MQConsumerInner> next = it.next();
            //获取MQConsumerInner
            MQConsumerInner consumer = next.getValue();
            //判断当前MQConsumerInner消费类型是否是"推"
            if (ConsumeType.CONSUME_PASSIVELY == consumer.consumeType()) {
                //获取当前当前MQConsumerInner所有订阅配置信息集合
                Set<SubscriptionData> subscriptions = consumer.subscriptions();
                //遍历当前当前MQConsumerInner所有订阅配置信息集合
                for (SubscriptionData sub : subscriptions) {
                    //判断订阅配置是否支持过滤
                    if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) {
                        final String consumerGroup = consumer.groupName();
                        final String className = sub.getSubString();
                        final String topic = sub.getTopic();
                        final String filterClassSource = sub.getFilterClassSource();
                        try {
                            //向filterServer注册过滤配置
                            this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                        } catch (Exception e) {
                            log.error("uploadFilterClassToAllFilterServer Exception", e);
                        }
                    }
                }
            }
        }
    }


    /**
     * 对支持filterServer订阅配置在其配置filterServer上进行注册
     * <p>
     * 1 对过滤配置（规则）进行编码和rc32加密
     * 2 获取指定topic的路由信息，在路由信息中获取filterServer地址
     * 3 向指定filterServer支持过滤配置（规则）
     *
     * @param consumerGroup
     * @param fullClassName
     * @param topic
     * @param filterClassSource
     * @throws UnsupportedEncodingException
     */
    @Deprecated
    private void uploadFilterClassToAllFilterServer(final String consumerGroup, final String fullClassName,
                                                    final String topic,
                                                    final String filterClassSource) throws UnsupportedEncodingException {

        //记录对过滤配置（规则）进行编码
        byte[] classBody = null;
        //记录对过滤配置（规则）进行编码后crc32加密结果
        int classCRC = 0;
        try {
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            log.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}",
                    fullClassName,
                    RemotingHelper.exceptionSimpleDesc(e1));
        }

        //获取当前topic路由信息
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        //判断topic路由信息是否不为空，且存在filterServer
        if (topicRouteData != null
                && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            //获取所有filterServer配置Entry的迭代器
            Iterator<Entry<String, List<String>>> it = topicRouteData.getFilterServerTable().entrySet().iterator();
            //遍历filterServer配置Entry的迭代器
            while (it.hasNext()) {
                //获取指定Entry
                Entry<String, List<String>> next = it.next();
                //获取filterServer地址
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    try {
                        //向指定filterServer支持过滤配置
                        this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody,
                                5000);

                        log.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr, consumerGroup,
                                topic, fullClassName);

                    } catch (Exception e) {
                        log.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        } else {
            log.warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                    consumerGroup, topic, fullClassName);
        }
    }

    /***************** persistAllConsumerOffset 开始  *****************/

    private void persistAllConsumerOffset() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            impl.persistConsumerOffset();
        }
    }

    /***************** adjustThreadPool 开始  *****************/

    public void adjustThreadPool() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception e) {
                }
            }
        }
    }

    /***************** MQClientInstance 启动定时任务结束  *****************/


    /***************** producerTable 属性相关 *****************/

    /**
     * 注册生产者分组
     *
     * @param group    生产者分组名称
     * @param producer MQProducerInner
     */
    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }
        //注册，如果不存在注册成功返回null,如果存在则注册失败返回原始的MQProducerInner
        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    /**
     * 取消注册生产者分组
     *
     * @param group 生产者分组
     */
    public void unregisterProducer(final String group) {
        //取消注册
        this.producerTable.remove(group);
        this.unregisterClientWithLock(group, null);
    }

    /**
     * 获取指定生产者分组对应 MQProducerInner
     *
     * @param group 生产者分组
     * @return MQProducerInner
     */
    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    /***************** consumerTable 属性相关 *****************/


    /**
     * 从 MQClientInstance客户端实例 注册消费者分组
     *
     * @param group    消费者分组名称
     * @param consumer MQConsumerInner
     */
    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }
        //注册，如果不存在注册成功返回null,如果存在则注册失败返回原始的MQConsumerInner
        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }

    /**
     * 从 MQClientInstance客户端实例 取消注册消费者分组
     *
     * @param group 消费者分组
     */
    public void unregisterConsumer(final String group) {
        //取消注册
        this.consumerTable.remove(group);
        this.unregisterClientWithLock(null, group);
    }

    /**
     * 从 MQClientInstance客户端实例 获取指定消费者分组对应 MQConsumerInner
     *
     * @param group 消费者分组
     * @return MQConsumerInner
     */
    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }


    /**
     * DefaultMQPushConsumerImpl 启动时调用
     * <p>
     * 获取注册到 MQClientInstance客户端实例 的所有MQConsumerInner，并对MQConsumerInner所有SubscriptionData订阅配置
     * 发送到broker master实例进行检查
     * <p>
     * 1 获取所有注册到客户端实例 MQConsumerInner以及分组
     * 2 获取MQConsumerInner内部设置得订阅配置信息集合
     * 3 过滤掉消息过滤表达类型是TAG过滤得订阅配置
     * 4 调用broker检查订阅配置
     *
     * @throws MQClientException
     */
    public void checkClientInBroker() throws MQClientException {
        //获取所有注册到客户端实例 MQConsumerInner以及分组
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        //对所有注册MQConsumerInner进行遍历
        while (it.hasNext()) {
            //获得Entry
            Entry<String, MQConsumerInner> entry = it.next();
            //获取MQConsumerInner内部设置得订阅配置信息集合
            Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
            //如果订阅配置信息集合为空直接返回
            if (subscriptionInner == null || subscriptionInner.isEmpty()) {
                return;
            }

            //遍历订阅配置信息集合
            for (SubscriptionData subscriptionData : subscriptionInner) {
                //如果订阅配置消息过滤表达类型是TAG过滤直接返回
                if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    continue;
                }
                //获取存储topic消息某个broker实例
                String addr = findBrokerAddrByTopic(subscriptionData.getTopic());

                if (addr != null) {
                    try {
                        //通过MQ客户端实现检查订阅配置信息
                        this.getMQClientAPIImpl().checkClientInBroker(
                                addr, entry.getKey(), this.clientId, subscriptionData, 3 * 1000
                        );
                    } catch (Exception e) {
                        if (e instanceof MQClientException) {
                            throw (MQClientException) e;
                        } else {
                            throw new MQClientException("Check client in broker error, maybe because you use "
                                    + subscriptionData.getExpressionType() + " to filter message, but server has not been upgraded to support!"
                                    + "This error would not affect the launch of consumer, but may has impact on message receiving if you " +
                                    "have use the new features which are not supported by server, please check the log!", e);
                        }
                    }
                }
            }
        }
    }


    /***************** adminExtTable 属性相关 *****************/

    /**
     * 注册admin分组
     *
     * @param group admin分组
     * @param admin MQAdminExtInner
     */
    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }
        //注册，如果不存在注册成功返回null,如果存在则注册失败返回原始的MQAdminExtInner
        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            log.warn("the admin group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    /**
     * 取消注册消费者分组
     *
     * @param group 消费者分组
     */
    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }


    /***************** unregisterClient 相关 *****************/

    private void unregisterClientWithLock(final String producerGroup, final String consumerGroup) {
        try {
            //Heartbeat同步锁加锁
            if (this.lockHeartbeat.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    //主线客户端
                    this.unregisterClient(producerGroup, consumerGroup);
                } catch (Exception e) {
                    log.error("unregisterClient exception", e);
                } finally {
                    //Heartbeat同步锁释放
                    this.lockHeartbeat.unlock();
                }
            } else {
                log.warn("lock heartBeat, but failed.");
            }
        } catch (InterruptedException e) {
            log.warn("unregisterClientWithLock exception", e);
        }
    }

    /**
     * 获取注册到 MQClientInstance客户端实例 的所有broker实例,注销指定客户端
     *
     * @param producerGroup 生产者分组
     * @param consumerGroup 消费者分组
     */
    private void unregisterClient(final String producerGroup, final String consumerGroup) {
        //获取brokerAddrTable迭代器
        Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        //对brokerAddrTable迭代
        while (it.hasNext()) {
            //获取Entry
            Entry<String, HashMap<Long, String>> entry = it.next();
            //获取broker节点
            String brokerName = entry.getKey();
            //获取broker节点内实例类型和地址
            HashMap<Long, String> oneTable = entry.getValue();
            //判断oneTable不为null
            if (oneTable != null) {
                //遍历oneTable
                for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                    //获取broker实例地址
                    String addr = entry1.getValue();
                    if (addr != null) {
                        try {
                            //注销指定客户端
                            this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, 3000);
                            log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup, consumerGroup, brokerName, entry1.getKey(), addr);
                        } catch (RemotingException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (InterruptedException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (MQBrokerException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        }
                    }
                }
            }
        }
    }


    /***************** rebalanceService 均衡消息服相关 *****************/

    /**
     * 通知均衡消息服务有新的任务从阻塞中唤醒
     */
    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    /**
     * 获取获取注册到MQClientInstance客户端实例的所有MQConsumerInner(消费分组),
     * 对当前MQClientInstance客户端实例在当前MQConsumerInner(消费分组)中消费队列进行负载均衡分配
     */
    public void doRebalance() {
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    impl.doRebalance();
                } catch (Throwable e) {
                    log.error("doRebalance exception", e);
                }
            }
        }
    }

    /***************** topicRouteTable 属性相关 *****************/

    /**
     * 在MQClientINSTANCE 客户端实例中,判断指定broker地址是否被注册
     * 是否被注册通过从topicRouteTable路由信息中broker节点数据是否存在为依据。
     *
     * @param brokerAddr 消息topic
     * @return
     */
    private boolean isBrokerInNameServer(final String brokerAddr) {
        //获取topicRouteTable迭代器
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        //对topicRouteTable迭代
        while (it.hasNext()) {
            //获取Entry
            Entry<String, TopicRouteData> itNext = it.next();
            //获取当前topic路由中broker节点数据列表
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            //遍历topic路由中broker节点数据列表
            for (BrokerData bd : brokerDatas) {
                //判断指定borker地址是否存在于某个broker节点个broker实例列表中
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain)
                    return true;
            }
        }

        return false;
    }

    /**
     * 在MQClientINSTANCE 客户端实例中获取存储topic消息某个随机broker实例
     *
     * @param topic 消息topic
     * @return
     */
    public String findBrokerAddrByTopic(final String topic) {
        //获得topic路由信息
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null) {
            //获取存储topic所有broker节点数据
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                //随机获取某一个broker节点数据
                int index = random.nextInt(brokers.size());
                BrokerData bd = brokers.get(index % brokers.size());
                //从随机获取broker节点数据随机选择一个broker实例
                return bd.selectBrokerAddr();
            }
        }
        return null;
    }

    /***************** brokerAddrTable/brokerVersionTable 属性相关 *****************/


    /**
     * 在MQClientINSTANCE 客户端实例中获取指定broker节点某个broker实例
     *
     * @param brokerName broker节点
     * @return
     */
    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        //broker实例地址
        String brokerAddr = null;
        //是否是slave
        boolean slave = false;
        //是否找到
        boolean found = false;

        //获取指定broker节点所有broker实例类型和地址map
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        //遍历map
        if (map != null && !map.isEmpty()) {
            for (Map.Entry<Long, String> entry : map.entrySet()) {
                //获取broker实例类型
                Long id = entry.getKey();
                //获取broker实例地址
                brokerAddr = entry.getValue();

                //如果broker实例地址存在则找到退出
                if (brokerAddr != null) {
                    //设置找到
                    found = true;
                    //设置是否是slave
                    if (MixAll.MASTER_ID == id) {
                        slave = false;
                    } else {
                        slave = true;
                    }
                    //退出
                    break;

                }
            } // end of for
        }

        //如果找到返回FindBrokerResult
        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }
        //没有找到返回null
        return null;
    }

    /**
     * 在MQClientINSTANCE 客户端实例中 指定broker节点，指定broker实例类型的实例查询结果
     *
     * @param brokerName     broker节点名称
     * @param brokerId       broker实例id
     * @param onlyThisBroker 如果没有找到，且onlyThisBroker=true,在同一个broker节点随机选择一个非master的broker节点
     * @return
     */
    public FindBrokerResult findBrokerAddressInSubscribe(
            final String brokerName,
            final long brokerId,
            final boolean onlyThisBroker
    ) {
        //broker实例地址
        String brokerAddr = null;
        //是否是slave
        boolean slave = false;
        //是否找到
        boolean found = false;

        //获取指定broker节点所有broker实例类型和地址map
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        //判断map是否不为null
        if (map != null && !map.isEmpty()) {
            //获取指定类型broker地址
            brokerAddr = map.get(brokerId);
            //设置是否是slave
            slave = brokerId != MixAll.MASTER_ID;
            //设置是否找到
            found = brokerAddr != null;

            //如果没有找到，且onlyThisBroker=true,在同一个broker节点随机选择一个非master的broker节点
            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = entry.getKey() != MixAll.MASTER_ID;
                found = true;
            }
        }
        //如果找到返回FindBrokerResult
        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }
        //没有找到返回null
        return null;
    }

    /**
     * 获取指定broker节点，指定broker实例的版本
     *
     * @param brokerName broker节点
     * @param brokerAddr broker实例
     * @return
     */
    public int findBrokerVersion(String brokerName, String brokerAddr) {
        if (this.brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        //To do need to fresh the version
        return 0;
    }


    /**
     * 在MQClientINSTANCE 客户端实例中 获取broker节点master实例地址
     *
     * @param brokerName
     * @return
     */
    public String findBrokerAddressInPublish(final String brokerName) {
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }


    /***************** clientId  属性相关 *****************/

    /**
     * 获取客户端Id
     *
     * @return
     */
    public String getClientId() {
        return clientId;
    }


    /**
     * 解析消息队列进度（设置原始topic）
     *
     * @param offsetTable 解析消息队列进度
     * @param namespace   命名空间
     * @return newOffsetTable 解析后消息队列进度
     */
    public Map<MessageQueue, Long> parseOffsetTableFromBroker(Map<MessageQueue, Long> offsetTable, String namespace) {
        //记录解析后消息队列进度
        HashMap<MessageQueue, Long> newOffsetTable = new HashMap<MessageQueue, Long>();
        //判断命名空间是否不为空，解析消息队列进度（设置原始topic）
        if (StringUtils.isNotEmpty(namespace)) {
            for (Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
                MessageQueue queue = entry.getKey();
                queue.setTopic(NamespaceUtil.withoutNamespace(queue.getTopic(), namespace));
                newOffsetTable.put(queue, entry.getValue());
            }
        } else {
            newOffsetTable.putAll(offsetTable);
        }
        //返回
        return newOffsetTable;
    }


    /**
     * 查询指定消费分组所有MQ消费客户端实例
     *
     * @param topic 消息topic
     * @param group 消费分组
     * @return
     */
    public List<String> findConsumerIdList(final String topic, final String group) {
        //在MQClientINSTANCE 客户端实例中获取存储topic消息某个随机broker实例
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        //如果不存在
        if (null == brokerAddr) {
            // 从Namerser获取topic对应的路由信息
            // 更新到 MQClientInstance客户端实例 属性中
            this.updateTopicRouteInfoFromNameServer(topic);
            //重新在MQClientINSTANCE 客户端实例中获取存储topic消息某个随机broker实例
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }

        //如果存在
        if (null != brokerAddr) {
            try {
                //使用MQ远程客户端实现调用broker查询指定消费分组所有MQ消费客户端实例
                return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, 3000);
            } catch (Exception e) {
                log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
            }
        }

        return null;
    }


    public void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                log.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }
            consumer.suspend();

            ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
                    ProcessQueue pq = entry.getValue();
                    pq.setDropped(true);
                    pq.clear();
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
            }

            Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                Long offset = offsetTable.get(mq);
                if (topic.equals(mq.getTopic()) && offset != null) {
                    try {
                        consumer.updateConsumeOffset(mq, offset);
                        consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable.get(mq));
                        iterator.remove();
                    } catch (Exception e) {
                        log.warn("reset offset failed. group={}, {}", group, mq, e);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.resume();
            }
        }
    }

    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl != null && impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }

    public MQAdminImpl getMQAdminImpl() {
        return mQAdminImpl;
    }

    public long getBootTimestamp() {
        return bootTimestamp;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public ConcurrentMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg,
                                                               final String consumerGroup,
                                                               final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (null != mqConsumerInner) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;

            ConsumeMessageDirectlyResult result = consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
            return result;
        }

        return null;
    }

    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);

        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

        List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();

        StringBuilder strBuilder = new StringBuilder();
        if (nsList != null) {
            for (String addr : nsList) {
                strBuilder.append(addr).append(";");
            }
        }

        String nsAddr = strBuilder.toString();
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION,
                MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

        return consumerRunningInfo;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return consumerStatsManager;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }
}
