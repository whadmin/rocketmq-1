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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.ConsumeMessageTraceHookImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Consumer客户端（对应一个消费分组）
 * DefaultMQPushConsumer 是Consumer客户端应用程序API路口.
 * DefaultMQPushConsumer 是对Consumer核心配置类。
 * DefaultMQPushConsume内部实现依赖于DefaultMQPushConsumerImpl
 */
public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {

    /**
     * 内部日志
     */
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * MQPushConsumer接口核心实现。DefaultMQPushConsumer大多数功能都委托给DefaultMQPushConsumerImpl实现
     */
    protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    /**
     * 消费者组
     */
    private String consumerGroup;

    /**
     * 消息模型
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * 【在消费模型为广播模式前提下】
     * 消费进度存储在本地。第一次启动MQ客户端实例消费消费队列时，可以配置消费者消费策略
     * //默认策略，从该队列最尾开始消费，即跳过历史消息
     * CONSUME_FROM_LAST_OFFSET,
     * //从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
     * CONSUME_FROM_FIRST_OFFSET,
     * //从某个时间点开始消费，和setConsumeTimestamp()配合使用，默认是半个小时以前
     * CONSUME_FROM_TIMESTAMP,
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * 【在消费模型为广播模式前提下】
     * consumeFromWhere.CONSUME_FROM_TIMESTAMP 时，默认消息回溯时间节点. 时间格式是* 2013 12 23 17 12 01
     */
    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));

    /**
     * 用来分配MessageQueue和 MQClientInstance MQ客户端实例 分配策略算法
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    /**
     * 消息topic以及消息topic过滤表达式
     */
    private Map<String /* topic */, String /* sub expression */> subscription = new HashMap<String, String>();

    /**
     * 消息监听器
     */
    private MessageListener messageListener;

    /**
     * 消费进度
     * 在消费模型为广播模式消费进度存储在本地
     * 在消费模型为广播模式消费进度存储在远程broker
     */
    private OffsetStore offsetStore;

    /**
     * ConsumeMessageService 消费线程池核心线程数
     */
    private int consumeThreadMin = 20;

    /**
     * ConsumeMessageService 消费线程池最大线程数
     */
    private int consumeThreadMax = 20;

    /**
     * 动态调整线程池数量的阈值
     */
    private long adjustThreadPoolNumsThreshold = 100000;

    /**
     * 并发消息消费时处理队列最大跨度
     */
    private int consumeConcurrentlyMaxSpan = 2000;

    /**
     * 队列级别的流量控制阈值，默认情况下每个消息队列最多会缓存1000条消息
     * 考虑{@code pullBatchSize}，瞬时值可能超过限制
     * 我们会将拉取消息发到消息处理队列中 {@link ProcessQueue}
     */
    private int pullThresholdForQueue = 1000;

    /**
     * 主题级别的流量控制阈值，默认值为-1（无限制）* <p> * {@code pullThresholdForQueue}的值将被覆盖并基于*
     * {@code pullThresholdForTopic}进行计算，如果它不是无限制的
     * 例如，如果pullThresholdForTopic的值为1000并且为此使用者分配了10个消息队列，则将pullThresholdForQueue设置为100
     * 我们会将拉取消息发到消息处理队列中 {@link ProcessQueue}
     */
    private int pullThresholdForTopic = -1;

    /**
     * 队列级别限制缓存的消息大小，默认情况下每个消息队列将缓存最多100条MiB邮件，
     * 考虑{@code pullBatchSize}，瞬时值可能超出限制
     * 消息大小仅由消息BODY测量，因此不准确
     * 我们会将拉取消息发到消息处理队列中 {@link ProcessQueue}
     */
    private int pullThresholdSizeForQueue = 100;

    /**
     * 主题级别限制缓存消息大小，默认值为-1 MiB（无限制）
     * {@code pullThresholdSizeForQueue}的值将被覆盖并基于* {@code pullThresholdSizeForTopic}计算，如果它不是无限制的,
     * 例如，如果pullThresholdSizeForTopic的值为1000 MiB且10个消息队列被分配给此消费者，则pullThresholdSizeForQueue将设置为100 MiB
     * 我们会将拉取消息发到消息处理队列中 {@link ProcessQueue}
     */
    private int pullThresholdSizeForTopic = -1;

    /**
     * 推模式下任务间隔时间
     */
    private long pullInterval = 0;

    /**
     * 推模式下,每次执行MessageListener#consumerMessage传入消息的数量
     */
    private int consumeMessageBatchMaxSize = 1;

    /**
     * 推模式下任务批量拉取的条数,默认32条
     */
    private int pullBatchSize = 32;

    /**
     * 每次拉动时是否更新订阅关系
     */
    private boolean postSubscriptionWhenPull = false;

    /**
     * 是否单元化
     */
    private boolean unitMode = false;

    /**
     * 最多重新消费次数。 -1表示16次。
     * 如果消息重新消费的次数超过{@link #maxReconsumeTimes}, 就可以进入死信队列
     */
    private int maxReconsumeTimes = -1;

    /**
     * 对于需要缓慢拉动的情况（例如流控制方案），暂停拉动时间。
     */
    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * 一条消息可能会阻塞使用线程的最长时间（以分钟为单位）。
     */
    private long consumeTimeout = 15;

    /**
     * 关闭Consumer实例时等待消息消费的最大时间, 0表示不等待。
     */
    private long awaitTerminationMillisWhenShutdown = 0;

    /**
     * 消息轨迹消息调度接口
     */
    private TraceDispatcher traceDispatcher = null;

    /**
     * DefaultMQPushConsumer 默认的构造器
     */
    public DefaultMQPushConsumer() {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, null, new AllocateMessageQueueAveragely());
    }

    /**
     * DefaultMQPushConsumer  构造函数
     *
     * @param consumerGroup 消费分组
     */
    public DefaultMQPushConsumer(final String consumerGroup) {
        this(null, consumerGroup, null, new AllocateMessageQueueAveragely());
    }

    /**
     * DefaultMQPushConsumer  构造函数
     *
     * @param rpcHook RPC钩子
     */
    public DefaultMQPushConsumer(RPCHook rpcHook) {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, rpcHook, new AllocateMessageQueueAveragely());
    }

    /**
     * DefaultMQPushConsumer  构造函数
     *
     * @param namespace     命名空间
     * @param consumerGroup 消费分组
     */
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup) {
        this(namespace, consumerGroup, null, new AllocateMessageQueueAveragely());
    }


    /**
     * DefaultMQPushConsumer  构造函数
     *
     * @param namespace     命名空间
     * @param consumerGroup 消费分组
     * @param rpcHook       RPC钩子
     */
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook) {
        this(namespace, consumerGroup, rpcHook, new AllocateMessageQueueAveragely());
    }

    /**
     * DefaultMQPushConsumer  构造函数
     *
     * @param consumerGroup                消费分组
     * @param rpcHook                      RPC钩子
     * @param allocateMessageQueueStrategy 分配MessageQueue和consumer实例clientID一对一关系的策略算法
     */
    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook,
                                 AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this(null, consumerGroup, rpcHook, allocateMessageQueueStrategy);
    }

    /**
     * DefaultMQPushConsumer  构造函数
     *
     * @param namespace                    命名空间
     * @param consumerGroup                消费分组
     * @param rpcHook                      RPC钩子
     * @param allocateMessageQueueStrategy 分配MessageQueue和consumer实例clientID一对一关系的策略算法
     */
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook,
                                 AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.consumerGroup = consumerGroup;
        this.namespace = namespace;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
    }

    /**
     * DefaultMQPushConsumer  构造函数
     *
     * @param consumerGroup  消费分组
     * @param enableMsgTrace 是否使用消费轨迹开关
     */
    public DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace) {
        this(null, consumerGroup, null, new AllocateMessageQueueAveragely(), enableMsgTrace, null);
    }

    /**
     * DefaultMQPushConsumer  构造函数
     *
     * @param consumerGroup        消费分组
     * @param enableMsgTrace       是否使用消费轨迹开关
     * @param customizedTraceTopic 消费轨迹消息存储topic
     */
    public DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(null, consumerGroup, null, new AllocateMessageQueueAveragely(), enableMsgTrace, customizedTraceTopic);
    }


    /**
     * DefaultMQPushConsumer  构造函数
     *
     * @param consumerGroup                消费分组
     * @param rpcHook                      RPC钩子
     * @param allocateMessageQueueStrategy 分配MessageQueue和consumer实例clientID一对一关系的策略算法
     * @param enableMsgTrace               是否使用消费轨迹开关
     * @param customizedTraceTopic         消费轨迹消息存储topic
     */
    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook,
                                 AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(null, consumerGroup, rpcHook, allocateMessageQueueStrategy, enableMsgTrace, customizedTraceTopic);
    }

    /**
     * DefaultMQPushConsumer  构造函数
     *
     * @param namespace                    命名空间
     * @param consumerGroup                消费分组
     * @param rpcHook                      RPC钩子
     * @param allocateMessageQueueStrategy 分配MessageQueue和consumer实例clientID一对一关系的策略算法
     * @param enableMsgTrace               是否使用消费轨迹开关
     * @param customizedTraceTopic         消费轨迹消息存储topic
     */
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook,
                                 AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMsgTrace, final String customizedTraceTopic) {
        this.consumerGroup = consumerGroup;
        this.namespace = namespace;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
        //是否开启消息轨迹
        if (enableMsgTrace) {
            try {
                //创建 AsyncTraceDispatcher（异步消息轨迹调度员）
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(consumerGroup, TraceDispatcher.Type.CONSUME, customizedTraceTopic, rpcHook);
                dispatcher.setHostConsumer(this.getDefaultMQPushConsumerImpl());
                traceDispatcher = dispatcher;
                //将 AsyncTraceDispatcher 注册到 DefaultMQPushConsumerImpl
                this.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(
                        new ConsumeMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    /*********************  MQAdmin接口实现  *********************/

    /**
     * 创建Topic
     *
     * @param key      accesskey
     * @param newTopic topic 名称
     * @param queueNum topic queue(队列) number（数量）
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, withNamespace(newTopic), queueNum, 0);
    }

    /**
     * 创建Topic
     *
     * @param key          accesskey
     * @param newTopic     topic 名称
     * @param queueNum     topic queue(队列) number（数量）
     * @param topicSysFlag topic 系统标识
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQPushConsumerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }

    /**
     * 根据某个时间（以毫秒为单位）获取消息队列偏移量<br> 由于更多的IO开销，请谨慎致电
     * broker内部AdminBrokerProcessor负责处理
     * 请求Code:RequestCode.SEARCH_OFFSET_BY_TIMESTAMP
     *
     * @param mq        消息队列
     * @param timestamp
     * @return offset
     */
    @Deprecated
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQPushConsumerImpl.searchOffset(queueWithNamespace(mq), timestamp);
    }

    /**
     * 获取指定消息队列最大逻辑偏移量
     * broker内部AdminBrokerProcessor负责处理
     * 请求Code:RequestCode.GET_MAX_OFFSET
     *
     * @param mq 消息队列
     * @return 最大偏移
     */
    @Deprecated
    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.maxOffset(queueWithNamespace(mq));
    }

    /**
     * 获取指定消息队列最小逻辑偏移量
     * broker内部AdminBrokerProcessor负责处理
     * 请求Code:RequestCode.GET_MIN_OFFSET
     *
     * @param mq 消息队列
     * @return 最小偏移
     */
    @Deprecated
    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.minOffset(queueWithNamespace(mq));
    }

    /**
     * 获取指定消息队最早的存储消息时间
     *
     * @param mq 消息队列
     * @return 最早的存储消息时间
     */
    @Deprecated
    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }

    /**
     * 根据消息ID查询消息
     *
     * @param offsetMsgId 消息id
     * @return message
     */
    @Deprecated
    @Override
    public MessageExt viewMessage(
            String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQPushConsumerImpl.viewMessage(offsetMsgId);
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
    @Deprecated
    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.defaultMQPushConsumerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    /**
     * 根据消息ID查询消息
     * 1 优先通过消息ID查询消息
     * 2 如果1查询不到根据topic+消息key(key=msgId)查询消息
     */
    @Deprecated
    @Override
    public MessageExt viewMessage(String topic,
                                  String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageDecoder.decodeMessageId(msgId);
            return this.viewMessage(msgId);
        } catch (Exception e) {
            // Ignore
        }
        return this.defaultMQPushConsumerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public int getConsumeConcurrentlyMaxSpan() {
        return consumeConcurrentlyMaxSpan;
    }

    public void setConsumeConcurrentlyMaxSpan(int consumeConcurrentlyMaxSpan) {
        this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }

    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    @Deprecated
    public DefaultMQPushConsumerImpl getDefaultMQPushConsumerImpl() {
        return defaultMQPushConsumerImpl;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }

    public int getPullThresholdForTopic() {
        return pullThresholdForTopic;
    }

    public void setPullThresholdForTopic(final int pullThresholdForTopic) {
        this.pullThresholdForTopic = pullThresholdForTopic;
    }

    public int getPullThresholdSizeForQueue() {
        return pullThresholdSizeForQueue;
    }

    public void setPullThresholdSizeForQueue(final int pullThresholdSizeForQueue) {
        this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
    }

    public int getPullThresholdSizeForTopic() {
        return pullThresholdSizeForTopic;
    }

    public void setPullThresholdSizeForTopic(final int pullThresholdSizeForTopic) {
        this.pullThresholdSizeForTopic = pullThresholdSizeForTopic;
    }

    public Map<String, String> getSubscription() {
        return subscription;
    }

    /**
     * 设置消息topic以及消息topic过滤表达式
     */
    @Deprecated
    public void setSubscription(Map<String, String> subscription) {
        Map<String, String> subscriptionWithNamespace = new HashMap<String, String>();
        for (String topic : subscription.keySet()) {
            subscriptionWithNamespace.put(withNamespace(topic), subscription.get(topic));
        }
        this.subscription = subscriptionWithNamespace;
    }

    /*********************  MQConsumer 接口实现  *********************/

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
    @Deprecated
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, null);
    }

    /**
     * 如果consumer消费消息失败，consumer将延迟消耗一些时间，将消息将被发送回broker，
     * <p>
     * 在2020年4月5日之后，此方法将被删除或在某些版本中其可见性将更改，因此请不要使用此方法。
     *
     * @param msg        发送返回消息
     * @param delayLevel 延迟级别。
     * @param brokerName broker节点名称
     * @throws RemotingException    远程调用异常
     * @throws MQBrokerException    broker异常
     * @throws InterruptedException 中断异常
     * @throws MQClientException    客户端异常
     */
    @Deprecated
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }

    /**
     * 根据消息topic对应MessageQueue(消息队列列表)
     *
     * @param topic 消息topic
     * @return MessageQueue(消息队列列表)
     */
    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPushConsumerImpl.fetchSubscribeMessageQueues(withNamespace(topic));
    }


    /*********************  MQPullConsumer 接口实现  *********************/


    /**
     * 启动
     *
     * @throws MQClientException 如果有任何客户端错误。
     */
    @Override
    public void start() throws MQClientException {
        setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
        //启动 defaultMQPushConsumerImpl
        this.defaultMQPushConsumerImpl.start();
        //启动 traceDispatcher
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    /**
     * 关闭
     */
    @Override
    public void shutdown() {
        //关闭 defaultMQPushConsumerImpl
        this.defaultMQPushConsumerImpl.shutdown(awaitTerminationMillisWhenShutdown);
        //关闭 traceDispatcher
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
    }

    /**
     * 注册消息监听器（已废弃）
     *
     * @param messageListener 消息监听器
     */
    @Override
    @Deprecated
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * 注册并发消息监听器
     *
     * @param messageListener 并发事件监听器
     */
    @Override
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * 注册顺序消息事件监听器
     *
     * @param messageListener 顺序消息事件监听器
     */
    @Override
    public void registerMessageListener(MessageListenerOrderly messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * 基于topic订阅消息，消息过滤使用TAG过滤表达式类型
     *
     * @param topic         消息topic
     * @param subExpression TAG过滤表达式类型,仅支持或操作，例如“ tag1 || tag2 || tag3”，如果为null或*表达式，则表示全部订阅。
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), subExpression);
    }

    /**
     * 基于topic订阅消息，消息过滤使用类模式（已废弃）
     *
     * @param topic             消息topic
     * @param fullClassName     class名称
     * @param filterClassSource class资源
     */
    @Override
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), fullClassName, filterClassSource);
    }

    /**
     * 基于topic订阅消息,消息过滤使用特定类型的消息选择器(支持TAG过滤，SLQ92过滤)
     *
     * @param topic           消息topic
     * @param messageSelector 消息选择器(支持TAG过滤，SLQ92过滤)
     */
    @Override
    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), messageSelector);
    }

    /**
     * 取消指定topic消息订阅
     *
     * @param topic 消息topic
     */
    @Override
    public void unsubscribe(String topic) {
        this.defaultMQPushConsumerImpl.unsubscribe(topic);
    }

    /**
     * 动态更新使用者线程池大小
     *
     * @param corePoolSize 核心线程数
     */
    @Override
    public void updateCorePoolSize(int corePoolSize) {
        this.defaultMQPushConsumerImpl.updateCorePoolSize(corePoolSize);
    }

    /**
     * 暂停消费
     */
    @Override
    public void suspend() {
        this.defaultMQPushConsumerImpl.suspend();
    }

    /**
     * 恢复消费
     */
    @Override
    public void resume() {
        this.defaultMQPushConsumerImpl.resume();
    }


    @Deprecated
    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    @Deprecated
    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }

    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }

    public boolean isPostSubscriptionWhenPull() {
        return postSubscriptionWhenPull;
    }

    public void setPostSubscriptionWhenPull(boolean postSubscriptionWhenPull) {
        this.postSubscriptionWhenPull = postSubscriptionWhenPull;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public long getAdjustThreadPoolNumsThreshold() {
        return adjustThreadPoolNumsThreshold;
    }

    public void setAdjustThreadPoolNumsThreshold(long adjustThreadPoolNumsThreshold) {
        this.adjustThreadPoolNumsThreshold = adjustThreadPoolNumsThreshold;
    }

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }

    public void setSuspendCurrentQueueTimeMillis(final long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }

    public long getConsumeTimeout() {
        return consumeTimeout;
    }

    public void setConsumeTimeout(final long consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }

    public long getAwaitTerminationMillisWhenShutdown() {
        return awaitTerminationMillisWhenShutdown;
    }

    public void setAwaitTerminationMillisWhenShutdown(long awaitTerminationMillisWhenShutdown) {
        this.awaitTerminationMillisWhenShutdown = awaitTerminationMillisWhenShutdown;
    }

    public TraceDispatcher getTraceDispatcher() {
        return traceDispatcher;
    }
}
