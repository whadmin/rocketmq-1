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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * MQConsumerInner内部核心实现
 * <p>
 * 1 负责MQPushConsumer接口核心实现
 * 2 负责MQAdmin接口实现
 * 3 拉取处理{@link PullRequest}拉取请求
 * 3.1 拉取{@link PullRequest}拉取请求中指定消费队列的消息，添加到{@link ProcessQueue}消息处理队列
 * 3.1 将拉取消息提交到{@link ConsumeMessageService}
 */
public class DefaultMQPushConsumerImpl implements MQConsumerInner {

    /**
     * 拉取消息发生异常时，延迟时间
     */
    private long pullTimeDelayMillsWhenException = 3000;

    /**
     * 流量控制间隔
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;

    /**
     * 延迟暂停拉服务的时间
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;

    /**
     * broker暂停最大时间
     */
    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;

    /**
     * 发生暂停时消费超时时间
     */
    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;

    /**
     * 内部日志
     */
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * defaultMQPushConsumer
     */
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    /**
     * 消息队列负载均衡服务
     */
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);

    /**
     * 过滤消息钩子列表（注册到内部 PullAPIWrapper）
     */
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    /**
     * 消费开始时间戳
     */
    private final long consumerStartTimestamp = System.currentTimeMillis();

    /**
     * 消费消息钩子列表（消费前，消费后回调）
     */
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();

    /**
     * RPC钩子
     */
    private final RPCHook rpcHook;

    /**
     * 服务状态
     */
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    /**
     * MQ客户端实例对象
     */
    private MQClientInstance mQClientFactory;

    /**
     * 拉取消息API包装类
     */
    private PullAPIWrapper pullAPIWrapper;

    /**
     * 是否消费暂停
     */
    private volatile boolean pause = false;

    /**
     * 是否顺序消费，
     * 不同值对应ConsumeMessageService不同实现类
     */
    private boolean consumeOrderly = false;

    /**
     * 消息监听器
     */
    private MessageListener messageListenerInner;

    /**
     * 消费偏移量持久化
     */
    private OffsetStore offsetStore;

    /**
     * 消费消息服务
     * ConsumeMessageOrderlyService 实现顺序消费
     * ConsumeMessageConcurrentlyService 实现并发消费
     */
    private ConsumeMessageService consumeMessageService;

    /**
     * 队列流控制时间
     */
    private long queueFlowControlTimes = 0;

    /**
     * 排队最大跨度流量控制时间
     */
    private long queueMaxSpanFlowControlTimes = 0;


    /**
     * DefaultMQPushConsumerImpl 构造函数
     *
     * @param defaultMQPushConsumer defaultMQPushConsumer
     * @param rpcHook               RPC钩子
     */
    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
        this.pullTimeDelayMillsWhenException = defaultMQPushConsumer.getPullTimeDelayMillsWhenException();
    }


    /**************** MQPushConsumer接口核心实现 ****************/

    /**************** DefaultMQPushConsumerImpl 启动关闭开始 ****************/

    /**
     * 启动DefaultMQPushConsumerImpl
     *
     * @throws MQClientException MQ客户端异常
     */
    public synchronized void start() throws MQClientException {
        //判断Consumer服务状态
        switch (this.serviceState) {
            //服务状态是服务刚刚创建
            case CREATE_JUST:
                //打印日志
                log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                        this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());

                // 1 首先设置服务状态启动失败
                this.serviceState = ServiceState.START_FAILED;
                // 2 校验 defaultMQPushConsumer配置
                this.checkConfig();
                // 3 拷贝defaultMQPushConsumer配置
                this.copySubscription();
                // 4 如果消息模型是集群,设置instanceName（实例名称）为"pid@hostname"
                if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultMQPushConsumer.changeInstanceNameToPID();
                }
                // 5 通过MQClientManager创建一个mq远程客户端对象MQClientInstance(每一个唯一的客户端对应的MQClientInstance是唯一的)
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

                // 6 消息队列负载均衡服务设置属性
                this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                // 6.1 消息队列负载均衡服务设置消息模型
                this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                // 6.2 消息队列负载均衡服务设置分配MessageQueue策略
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                // 6.3 消息队列负载均衡服务设置MQ客户端实例对象
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                //7 构造 PullAPIWrapper 拉取消息API包装类
                this.pullAPIWrapper = new PullAPIWrapper(
                        mQClientFactory,
                        this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
                //7.1 PullAPIWrapper 注册过滤消息钩子列表
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);


                //8 设置消费偏移量持久化
                //8.1 手动设置
                if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                    this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                } else {
                    //8.2 自动设置，通过消息模型
                    switch (this.defaultMQPushConsumer.getMessageModel()) {
                        //广播
                        case BROADCASTING:
                            //创建偏移量持久化接口本地存储实现实现
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        //集群
                        case CLUSTERING:
                            //创建偏移量持久化接口远程存储实现实现
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    //设置设置消费偏移量持久化
                    this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
                }
                //8.3 费偏移量持久化加载（远程空实现）
                this.offsetStore.load();

                //9判断消息监听类型构造不同消费消息服务，设置，启动
                //如果使用的是顺序监听器
                if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                    //设置是否使用顺序消费服务
                    this.consumeOrderly = true;
                    //创建顺序消息服务
                    this.consumeMessageService =
                            new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                }
                //如果使用的并发监听器
                else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                    //设置是否使用顺序消费服务
                    this.consumeOrderly = false;
                    //创建顺序消息服务
                    this.consumeMessageService =
                            new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                }
                //9 启动消费消息服务
                this.consumeMessageService.start();

                //10 将defaultMQPushConsumer 注册到MQ客户端实例对象.consumerTable中
                //1 个 mQClientFactory（MQ客户端实例对象一个进程）对应多个defaultMQPushConsumer
                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
                    throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                }
                //11 启动MQ客户端实例对象
                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
                //12 设置服务正在运行
                this.serviceState = ServiceState.RUNNING;
                break;
            //服务状态是正在运行
            case RUNNING:
                //服务状态是启动失败
            case START_FAILED:
                //服务状态是正在关闭
            case SHUTDOWN_ALREADY:
                //抛出异常
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }
        //获取所有订阅topic，更新到客户端实例属性中
        this.updateTopicSubscribeInfoWhenSubscriptionChanged();

        //检查所有注册到客户端实例的MQConsumerInner，并获取MQConsumerInner所有订阅配置到broker检查
        this.mQClientFactory.checkClientInBroker();

        //1 获取注册到 MQClientInstance客户端实例 的所有broker实例发送 MQClientInstance客户端实例 HeartbeatData心跳数据
        //2 获取注册到 MQClientInstance客户端实例 的所有MQConsumerInner，并对MQConsumerInner中支持filterServer订阅配置进行上传注册。
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        //唤醒客户端实例内部均衡消息服务（本质是一个线程）
        this.mQClientFactory.rebalanceImmediately();
    }

    /**
     * 关闭 DefaultMQPushConsumerImpl
     */
    public void shutdown() {
        shutdown(0);
    }


    /**
     * 关闭 DefaultMQPushConsumerImpl
     *
     * @param awaitTerminateMillis 等待超时时间
     */
    public synchronized void shutdown(long awaitTerminateMillis) {
        //判断服务状态
        switch (this.serviceState) {
            //如果服务刚刚创建
            case CREATE_JUST:
                break;
            //如果服务正在运行
            case RUNNING:
                //关闭消费消息服务
                this.consumeMessageService.shutdown(awaitTerminateMillis);
                //持久化消费进度
                this.persistConsumerOffset();
                //从 MQClientInstance客户端实例 注销消费者分组
                this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
                //关闭 MQClientInstance客户端实例
                this.mQClientFactory.shutdown();
                //打印日志
                log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
                //关闭消息队列负载均衡服务
                this.rebalanceImpl.destroy();
                //设置服务状态正在关闭
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            //如果服务正在关闭
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    /**
     * 校验配置
     *
     * @throws MQClientException
     */
    private void checkConfig() throws MQClientException {
        //使用Validators 校验分组名称
        Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());

        //分组名称不能为null
        if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
            throw new MQClientException(
                    "consumerGroup is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }
        //分组名称不能为MixAll.DEFAULT_CONSUMER_GROUP
        if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                    "consumerGroup can not equal "
                            + MixAll.DEFAULT_CONSUMER_GROUP
                            + ", please specify another one."
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }
        //消息模型不为为null
        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException(
                    "messageModel is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }
        //消费者消费策略不为为null
        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException(
                    "consumeFromWhere is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }
        //消息回溯时间节点格式验证
        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
        if (null == dt) {
            throw new MQClientException(
                    "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received "
                            + this.defaultMQPushConsumer.getConsumeTimestamp()
                            + " " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // allocateMessageQueueStrategy不能为null
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                    "allocateMessageQueueStrategy is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // subscription(消息topic以及消息topic过滤表达式)不能为null
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException(
                    "subscription is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // messageListener(消息监听器)不能为null
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException(
                    "messageListener is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        //消息监听必须使用 MessageListenerOrderly | MessageListenerConcurrently 其中一个实现
        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException(
                    "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeThreadMin（ConsumeMessageService 消费线程池核心线程数）范围校验
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1
                || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000) {
            throw new MQClientException(
                    "consumeThreadMin Out of range [1, 1000]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeThreadMax(消费线程池最大线程数)范围校验
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException(
                    "consumeThreadMax Out of range [1, 1000]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeThreadMin(消费线程池核心线程数)范围校验
        if (this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax()) {
            throw new MQClientException(
                    "consumeThreadMin (" + this.defaultMQPushConsumer.getConsumeThreadMin() + ") "
                            + "is larger than consumeThreadMax (" + this.defaultMQPushConsumer.getConsumeThreadMax() + ")",
                    null);
        }

        // consumeConcurrentlyMaxSpan(并发消息消费时处理队列最大跨度)范围校验
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
                || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException(
                    "consumeConcurrentlyMaxSpan Out of range [1, 65535]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullThresholdForQueue(队列级别的流量控制阈值)范围校验
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException(
                    "pullThresholdForQueue Out of range [1, 65535]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullThresholdForTopic(主题级别的流量控制阈值)范围校验
        if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1) {
            if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500) {
                throw new MQClientException(
                        "pullThresholdForTopic Out of range [1, 6553500]"
                                + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                        null);
            }
        }

        // pullThresholdSizeForQueue(队列级别限制缓存的消息大小)范围校验
        if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024) {
            throw new MQClientException(
                    "pullThresholdSizeForQueue Out of range [1, 1024]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        //pullThresholdSizeForTopic(主题级别限制缓存消息大小)范围校验
        if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() != -1) {
            // pullThresholdSizeForTopic
            if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForTopic() > 102400) {
                throw new MQClientException(
                        "pullThresholdSizeForTopic Out of range [1, 102400]"
                                + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                        null);
            }
        }

        // pullInterval(推模式下任务间隔时间)范围校验
        if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException(
                    "pullInterval Out of range [0, 65535]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeMessageBatchMaxSize（每次执行MessageListener#consumerMessage传入消息的数量）范围校验
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
                || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException(
                    "consumeMessageBatchMaxSize Out of range [1, 1024]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullBatchSize（推模式下任务批量拉取的条数）范围校验
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException(
                    "pullBatchSize Out of range [1, 1024]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }
    }

    /**
     * 拷贝defaultMQPushConsumer配置
     *
     * @throws MQClientException
     */
    private void copySubscription() throws MQClientException {
        try {
            //获取消息topic以及消息topic过滤表达式
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    //通过topic过滤表达式,构造订阅配置信息
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),
                            topic, subString);
                    //将topic以及topic订阅配置信息注册到 RebalanceImpl 消息队列负载均衡服务
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }

            //从defaultMQPushConsumer获取 messageListener 消息监听器
            //设置到messageListenerInner
            if (null == this.messageListenerInner) {
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }

            //判断消息消息模型
            switch (this.defaultMQPushConsumer.getMessageModel()) {
                case BROADCASTING:
                    break;
                //对于集群类型注册消费分组对应的重试topic
                case CLUSTERING:
                    //获取当前消费分组重试topic
                    final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                    //构造消费分组重试topic订阅配置信息
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),
                            retryTopic, SubscriptionData.SUB_ALL);
                    //将消费分组重试topic 以及topic订阅配置信息注册到 RebalanceImpl 消息队列负载均衡服务
                    this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 获取所有订阅topic，更新到客户端实例属性中
     * <p>
     * 1 获取所有订阅topic以及topic订阅配置信息
     * 2 调用mQClientFactory.updateTopicRouteInfoFromNameServer
     * 从Namerser获取topic对应的路由信息，更新到客户端实例属性中
     */
    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }


    /**************** registerMessageListener 开始 ****************/


    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    /**************** subscribe 开始 ****************/

    /**
     * 基于topic订阅消息，消息过滤使用TAG过滤表达式类型
     *
     * @param topic         消息topic
     * @param subExpression TAG过滤表达式类型,仅支持或操作，例如“ tag1 || tag2 || tag3”，如果为null或*表达式，则表示全部订阅。
     * @throws MQClientException if there is any client error.
     */
    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            //获取订阅配置信息（通过topic过滤表达式）
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),
                    topic, subExpression);
            //注册订阅配置信息到消息队列负载均衡服务
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);

            //1 获取注册到 MQClientInstance客户端实例 的所有broker实例发送 MQClientInstance客户端实例 HeartbeatData心跳数据
            //2 获取注册到 MQClientInstance客户端实例 的所有MQConsumerInner，并对MQConsumerInner中支持filterServer订阅配置进行上传注册。
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 基于topic订阅消息，消息过滤使用类模式（已废弃）
     *
     * @param topic             消息topicc
     * @param fullClassName     class名称
     * @param filterClassSource class资源
     */
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        try {
            //获取构造订阅配置信息
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),
                    topic, "*");
            subscriptionData.setSubString(fullClassName);
            subscriptionData.setClassFilterMode(true);
            subscriptionData.setFilterClassSource(filterClassSource);

            //注册订阅配置信息到消息队列负载均衡服务
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);

            //1 获取注册到 MQClientInstance客户端实例 的所有broker实例发送 MQClientInstance客户端实例 HeartbeatData心跳数据
            //2 获取注册到 MQClientInstance客户端实例 的所有MQConsumerInner，并对MQConsumerInner中支持filterServer订阅配置进行上传注册。
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }

        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 基于topic订阅消息,消息过滤使用特定类型的消息选择器(支持TAG过滤，SLQ92过滤)
     *
     * @param topic           消息topic
     * @param messageSelector 消息选择器(支持TAG过滤，SLQ92过滤)
     */
    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        try {
            //如果messageSelector=null，调用subscribe，订阅当前topic，不过滤
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }
            //获取构造订阅配置信息（通过topic过滤表达式）
            SubscriptionData subscriptionData = FilterAPI.build(topic,
                    messageSelector.getExpression(), messageSelector.getExpressionType());

            //注册订阅配置信息到消息队列负载均衡服务
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);

            //1 获取注册到 MQClientInstance客户端实例 的所有broker实例发送 MQClientInstance客户端实例 HeartbeatData心跳数据
            //2 获取注册到 MQClientInstance客户端实例 的所有MQConsumerInner，并对MQConsumerInner中支持filterServer订阅配置进行上传注册。
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**************** unsubscribe 开始 ****************/

    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }


    /**************** updateCorePoolSize 开始 ****************/

    /**
     * 更新消费消息服务核心线程数
     */
    public void updateCorePoolSize(int corePoolSize) {
        this.consumeMessageService.updateCorePoolSize(corePoolSize);
    }

    /**************** suspend 开始 ****************/

    /**
     * 暂停
     */
    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    /**************** resume 开始 ****************/

    /**
     * 恢复
     */
    public void resume() {
        this.pause = false;
        doRebalance();
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }


    /**************** MQAdmin接口实现 ****************/


    /**
     * 创建Topic
     *
     * @param key      accesskey
     * @param newTopic topic 名称
     * @param queueNum topic queue(队列) number（数量）
     */
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    /**
     * 创建Topic
     *
     * @param key          accesskey
     * @param newTopic     topic 名称
     * @param queueNum     topic queue(队列) number（数量）
     * @param topicSysFlag topic 系统标识
     */
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    /**
     * 根据消息topic对应MessageQueue(消息队列列表)
     *
     * @param topic 消息topic
     * @return MessageQueue(消息队列列表)
     */
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }

        return parseSubscribeMessageQueues(result);
    }

    /**
     * 解析MessageQueue(获取原始topic,如果设置 Namespace 命名空间)
     *
     * @param messageQueueList
     * @return
     */
    public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> messageQueueList) {
        Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
        for (MessageQueue queue : messageQueueList) {
            String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.defaultMQPushConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
        }

        return resultQueues;
    }


    /**
     * 获取指定消息队最早的存储消息时间
     *
     * @param mq 消息队列
     * @return 最早的存储消息时间
     */
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    /**
     * 获取指定消息队列最大逻辑偏移量
     * broker内部AdminBrokerProcessor负责处理
     * 请求Code:RequestCode.GET_MAX_OFFSET
     *
     * @param mq 消息队列
     * @return 最大偏移
     */
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    /**
     * 获取指定消息队列最小逻辑偏移量
     * broker内部AdminBrokerProcessor负责处理
     * 请求Code:RequestCode.GET_MIN_OFFSET
     *
     * @param mq 消息队列
     * @return 最小偏移
     */
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    /**
     * 根据条件查询消息（条件包括指定topic+指定消息key+时间范围）的消息
     *
     * @param topic  消息topic
     * @param key    消息key
     * @param maxNum 返回满足消息的最大数量
     * @param begin  查询消息产生时间开始
     * @param end    查询消息产生时间结束
     * @return Instance of QueryResult
     */
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    /**
     * 根据消息ID查询消息
     * 1 优先通过消息ID查询消息
     * 2 如果1查询不到根据topic+消息key(key=msgId)查询消息
     */
    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException,
            InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }


    /**************** 属性相关方法 ****************/

    /**
     * 注册 filterMessageHookList 过滤消息钩子
     *
     * @param hook
     */
    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    /**
     * 消费消息钩子列表是否不为空
     */
    public boolean hasHook() {
        return !this.consumeMessageHookList.isEmpty();
    }

    /**
     * 注册ConsumeMessageHook 消费消息钩子
     *
     * @param hook 消费消息钩子
     */
    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }


    /**
     * 执行消费消息钩子（消费消息前操作）
     *
     * @param context 消费消息钩子上下文
     */
    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    /**
     * 执行消费消息钩子（消息消息后操作）
     *
     * @param context 消费消息钩子上下文
     */
    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    /**
     * 获取消费偏移量持久化
     */
    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    /**
     * 设置消费偏移量持久化
     *
     * @param offsetStore 消费偏移量持久化
     */
    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }


    /**
     * 获取DefaultMQPushConsumer
     *
     * @return DefaultMQPushConsumer
     */
    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }


    /**
     * 是否暂停
     */
    public boolean isPause() {
        return pause;
    }

    /**
     * 设置是否恢复
     *
     * @param pause 是否暂停
     */
    public void setPause(boolean pause) {
        this.pause = pause;
    }


    /**
     * 处理拉取请求
     * <p>
     * 每一个拉取请求对应了某个消费分组对指定消息队列拉取消息请求。
     *
     * @param pullRequest 处理拉取请求
     */
    public void pullMessage(final PullRequest pullRequest) {
        //获取拉取请求中消息处理队列
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        //判断当前队列是否被丢弃，
        /** 负载均衡服务会删除取消订阅关系的消息队列 {@link RebalanceImpl#truncateMessageQueueNotMyTopic}.**/
        if (processQueue.isDropped()) {
            log.info("the pull request[{}] is dropped.", pullRequest.toString());
            return;
        }
        //记录消息处理队列最后拉取时间
        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        //校验当前消费分组服务是否停止，停止抛出异常
        try {
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            //将指定拉取消息请求，延时一段时间添加到 pullRequestQueue
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            return;
        }
        //判断消费分组是否暂停
        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            //将指定拉取消息请求，延时一段时间添加到 pullRequestQueue
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }

        //获取消息处理队列中缓存消息数量
        long cachedMessageCount = processQueue.getMsgCount().get();
        //获取消息处理队列中缓存消息大小
        long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

        //判断消息处理队列中缓存消息数量是否大于流量控制阈值
        if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                        "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }
        //判断消息处理队列中缓存消息大小是否大于流量控制阈值
        if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                        "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }

        //是否顺序消费
        if (!this.consumeOrderly) {
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                    log.warn(
                            "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                            processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(),
                            pullRequest, queueMaxSpanFlowControlTimes);
                }
                return;
            }
        }
        //非顺序消费
        else {
            //如果息处理队列锁定
            if (processQueue.isLocked()) {
                //消息消息队列请求第一次被消费，标记锁定，并重新计算下次拉取消费队列逻辑偏移
                if (!pullRequest.isLockedFirst()) {
                    /**
                     * 获取消费队列下次拉取逻辑偏移量
                     * 集群模式
                     *    获取远程broker存储消费进度
                     * 广播模式,
                     *    如果消费队列非第一次消费，获取本地文件存储消费进度
                     *    如果消费队列第一次消费，通过ConsumeFromWhere 消费者消费策略获取消费进度
                     */
                    final long offset = this.rebalanceImpl.computePullFromWhere(pullRequest.getMessageQueue());

                    //判断拉取请求中记录的下次拉取消费队列逻辑偏移是否大于当前计算值，如果大于打印日志
                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                            pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                                pullRequest, offset);
                    }
                    //标记被锁定
                    pullRequest.setLockedFirst(true);
                    //设置下次拉取消费队列逻辑偏移
                    pullRequest.setNextOffset(offset);
                }
            }
            ///如果息处理队列未锁定
            else {
                //将指定拉取消息请求，延时一段时间添加到 pullRequestQueue，返回
                this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                log.info("pull message later because not locked in broker, {}", pullRequest);
                return;
            }
        }

        //如果拉取请求topic不在被订阅
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            //将指定拉取消息请求，延时一段时间添加到 pullRequestQueue
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        //记录准备开始拉取消息开始时间，已验证完毕
        final long beginTimestamp = System.currentTimeMillis();

        //创建异步拉取消息回调
        PullCallback pullCallback = new PullCallback() {

            /**
             * 异步拉取消成功回调
             *
             * @param pullResult 拉取消息结果
             */
            @Override
            public void onSuccess(PullResult pullResult) {
                //判断拉取消息结果
                if (pullResult != null) {
                    //使用pullAPIWrapper处理拉取结果
                    //1 更新指定消息队列从哪个broker实例类型拉取消息（来源于拉取结果）
                    //2 解析消息，并根据订阅配置信息按照tag过滤消息
                    //3 使用消息过滤钩子对拉取消息进行过滤
                    // 4 设置消息特殊属性
                    pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult,
                            subscriptionData);

                    //判断拉取结果状态
                    switch (pullResult.getPullStatus()) {
                        //拉取发现消息
                        case FOUND:
                            //获取当前请求当前拉取消费队列开始位置(逻辑偏移量)
                            long prevRequestOffset = pullRequest.getNextOffset();
                            //从拉取请求中获取新的拉取消费队列开始位置(逻辑偏移量),设置到拉取请求
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());


                            //计算拉取请求时间
                            long pullRT = System.currentTimeMillis() - beginTimestamp;
                            //记录统计拉取消息请求时间
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(),
                                    pullRequest.getMessageQueue().getTopic(), pullRT);

                            //记录拉取结果第一条消息在消息队列位置（逻辑偏移量）
                            long firstMsgOffset = Long.MAX_VALUE;
                            //如果拉取结果不存在消息，将拉取消息请求，添加到 pullRequestQueue
                            if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            }
                            //如果拉取结果不存在消息
                            else {
                                //记录拉取结果第一条消息在消息队列位置（逻辑偏移量）
                                firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();

                                //记录统计消费分组Tps
                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(),
                                        pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());

                                //将拉取结果中消息添加到消息处理队列,返回是否成功
                                boolean dispatchToConsume = processQueue.putMessage(pullResult.getMsgFoundList());

                                //提交消息请求
                                DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                                        pullResult.getMsgFoundList(),
                                        processQueue,
                                        pullRequest.getMessageQueue(),
                                        dispatchToConsume);

                                //根据客户端配置间隔时间，
                                if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                    //将指定拉取消息请求，延时一段时间添加到 pullRequestQueue
                                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                            DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                                } else {
                                    //将指定拉取消息请求，添加到 pullRequestQueue
                                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                }
                            }

                            //如果从拉取请求中获取新的拉取消费队列开始位置< 当前请求当前拉取消费队列开始位置(逻辑偏移量) 打印日志
                            //异常检查日志
                            if (pullResult.getNextBeginOffset() < prevRequestOffset
                                    || firstMsgOffset < prevRequestOffset) {
                                log.warn(
                                        "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                                        pullResult.getNextBeginOffset(),
                                        firstMsgOffset,
                                        prevRequestOffset);
                            }

                            break;
                        //没有新消息可以拉
                        case NO_NEW_MSG:
                            //从拉取请求中获取新的拉取消费队列开始位置(逻辑偏移量),设置到拉取请求
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                            //如果消息处理队列不存在消息，更新指定消费队列缓存中消费进度
                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);
                            //将指定拉取消息请求，添加到 pullRequestQueue
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;
                        //没有匹配消息（过滤）
                        case NO_MATCHED_MSG:
                            //从拉取请求中获取新的拉取消费队列开始位置(逻辑偏移量),设置到拉取请求
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                            //如果消息处理队列不存在消息，更新指定消费队列缓存中消费进度
                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);
                            //将指定拉取消息请求，添加到 pullRequestQueue
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;
                        //偏移量非法
                        case OFFSET_ILLEGAL:
                            log.warn("the pull request offset illegal, {} {}",
                                    pullRequest.toString(), pullResult.toString());
                            //从拉取请求中获取新的拉取消费队列开始位置(逻辑偏移量),设置到拉取请求
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                            //丢弃消息处理队列
                            pullRequest.getProcessQueue().setDropped(true);
                            //延时一段时间，执行某个线程任务，
                            DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        //更新指定消费队列缓存中消费进度
                                        DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(pullRequest.getMessageQueue(),
                                                pullRequest.getNextOffset(), false);

                                        //对消费队列进度进行持久化 （集群模式发送到远程到broker，广播模式写入本地文件）
                                        DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());
                                        //从 processQueueTable 删除指定消费队列
                                        DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());
                                        //打印日志
                                        log.warn("fix the pull request offset, {}", pullRequest);
                                    } catch (Throwable e) {
                                        log.error("executeTaskLater Exception", e);
                                    }
                                }
                            }, 10000);
                            break;
                        default:
                            break;
                    }
                }
            }

            /**
             * 异步拉取消失败回调
             *
             * @param e 异常
             */
            @Override
            public void onException(Throwable e) {
                //打印日志
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }
                //将指定拉取消息请求，延时一段时间添加到 pullRequestQueue
                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            }
        };


        //记录本地缓存消费进度是否大于0
        boolean commitOffsetEnable = false;
        //记录本地缓存消费进度，这里的消费进度是本地缓存，还未提交到远程borker进度
        long commitOffsetValue = 0L;
        //如果拉取请求消费分组消费模型是集群，从本地缓存中获取消费进度
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                commitOffsetEnable = true;
            }
        }

        //获取拉取请求消费队列topic的订阅配置，
        String subExpression = null;
        //记录是否支持filterServer
        boolean classFilter = false;
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }
            classFilter = sd.isClassFilterMode();
        }

        //创建拉取消息系统标识
        int sysFlag = PullSysFlag.buildSysFlag(
                commitOffsetEnable, // commitOffset
                true, // suspend
                subExpression != null, // subscription
                classFilter // class filter
        );

        try {
            //使用PullAPIWrapper 异步拉取消息
            this.pullAPIWrapper.pullKernelImpl(
                    pullRequest.getMessageQueue(),
                    subExpression,
                    subscriptionData.getExpressionType(),
                    subscriptionData.getSubVersion(),
                    pullRequest.getNextOffset(),
                    this.defaultMQPushConsumer.getPullBatchSize(),
                    sysFlag,
                    commitOffsetValue,
                    BROKER_SUSPEND_MAX_TIME_MILLIS,
                    CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
                    CommunicationMode.ASYNC,
                    pullCallback
            );
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
        }
    }

    /**
     * 校验当前消费分组服务是否停止，停止抛出异常
     *
     * @throws MQClientException
     */
    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
        }
    }

    /**
     * 将指定拉取消息请求，延时一段时间添加到 pullRequestQueue
     *
     * @param pullRequest 拉取消息请求
     * @param timeDelay   延时时间（毫秒）
     */
    private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }


    /**
     * 获取消费统计服务
     *
     * @return 消费统计服务
     */
    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }

    /**
     * 将指定拉取消息请求，添加到 pullRequestQueue
     *
     * @param pullRequest 拉取请求
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    /**
     * 如果消息处理队列不存在消息，更新指定消费队列缓存中消费进度
     *
     * @param pullRequest 拉取请求
     */
    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    /**
     * 延时一段时间，执行某个线程任务，
     *
     * @param r         线程任务
     * @param timeDelay 延时时间（毫秒）
     */
    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }

    /**
     * 如果consumer消费消息失败，consumer将延迟消耗一些时间，将消息将被发送回broker，
     * <p>
     * 在2020年4月5日之后，此方法将被删除或在某些版本中其可见性将更改，因此请不要使用此方法。
     *
     * @param msg        发送返回消息
     * @param delayLevel 延迟级别。
     * @throws RemotingException    远程调用异常
     * @throws MQBrokerException    broker异常
     * @throws InterruptedException 中断异常
     * @throws MQClientException    客户端异常
     */
    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                    : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg,
                    this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
        } catch (Exception e) {
            log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);

            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        } finally {
            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
        }
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: 16
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return 16;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }


    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }


    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }


    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset, false);
    }


    public MessageExt viewMessage(String msgId)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }

    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }

    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
    }

    public void resetOffsetByTimeStamp(long timeStamp)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
            if (mqs != null) {
                for (MessageQueue mq : mqs) {
                    long offset = searchOffset(mq, timeStamp);
                    offsetTable.put(mq, offset);
                }
                this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
            }
        }
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    @Override
    public String groupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

        return subSet;
    }

    /**
     * 当前MQ消费客户端实例，在当前消费分组（当前对象所属MQConsumer）所消topic费消费队列进行负载均衡
     */
    @Override
    public void doRebalance() {
        //判断是否消费暂停
        if (!this.pause) {
            //当前 MQClientInstance MQ消费客户端实例，在当前消费分组，负载均衡分配消息队列
            this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
        }
    }

    /**
     * 持久化当前 MQClientInstanceMQ消费客户端实例，在当前消费分组，负载均衡分配消息队的消费进度
     */
    @Override
    public void persistConsumerOffset() {
        try {
            //校验当前消费分组服务是否停止，停止抛出异常
            this.makeSureStateOK();
            //获取当前 MQClientInstance MQ消费客户端实例，在当前消费分组，负载均衡分配消息队列
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);

            //对获取当前 MQClientInstance MQ消费客户端实例，在当前消费分组，负载均衡分配消息队列度进行持久化
            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    /**
     * 更新topic订阅信息
     *
     * @param topic 消息topic
     * @param info  消费队列集合
     */
    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }

    /**
     * 如果topic本地和远程获取路由信息存在差异,需要更新
     * 通过客户端MQConsumerInner配置判断是否需要更新到本地
     */
    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    /**
     * 是否是单元化
     *
     * @return 是否是单元化
     */
    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

        prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
        prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, String.valueOf(this.consumeMessageService.getCorePoolSize()));
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

        info.setProperties(prop);

        Set<SubscriptionData> subSet = this.subscriptions();
        info.getSubscriptionSet().addAll(subSet);

        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            ProcessQueueInfo pqinfo = new ProcessQueueInfo();
            pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            pq.fillProcessQueueInfo(pqinfo);
            info.getMqTable().put(mq, pqinfo);
        }

        for (SubscriptionData sd : subSet) {
            ConsumeStatus consumeStatus = this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(), sd.getTopic());
            info.getStatusTable().put(sd.getTopic(), consumeStatus);
        }

        return info;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    //Don't use this deprecated setter, which will be removed soon.
    @Deprecated
    public synchronized void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public void adjustThreadPool() {
        long computeAccTotal = this.computeAccumulationTotal();
        long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

        long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

        long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

        if (computeAccTotal >= incThreshold) {
            this.consumeMessageService.incCorePoolSize();
        }

        if (computeAccTotal < decThreshold) {
            this.consumeMessageService.decCorePoolSize();
        }
    }

    private long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();
        Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue value = next.getValue();
            msgAccTotal += value.getMsgAccCnt();
        }

        return msgAccTotal;
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        List<QueueTimeSpan> queueTimeSpan = new ArrayList<QueueTimeSpan>();
        TopicRouteData routeData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String addr = brokerData.selectBrokerAddr();
            queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
        }

        return queueTimeSpan;
    }

    public void resetRetryAndNamespace(final List<MessageExt> msgs, String consumerGroup) {
        final String groupTopic = MixAll.getRetryTopic(consumerGroup);
        for (MessageExt msg : msgs) {
            String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                msg.setTopic(retryTopic);
            }

            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    public ConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

    public void setConsumeMessageService(ConsumeMessageService consumeMessageService) {
        this.consumeMessageService = consumeMessageService;

    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }
}
