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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * MQ消费客户端分配消费队列负载均衡实现
 *
 * 1 每个RebalanceImpl对象都针对一个消费分组来说的。
 * PS: 每个消费分组对应一个MQConsumer，每一个MQConsumer都存在一个RebalanceImpl
 * <p>
 * 2 MQ消费客户端实例 和  消息队列 关系 ？
 * 2.1 MQ消费客户端实例 MQClientInstance（进程）和消费分组关系（多对多）
 * 一个消费分组可以存在多个消费者，每一个消费者对应到一个MQ客户端实例 MQClientInstance 一个进程
 * 一个MQ客户端实例 MQClientInstance，或一个进程内也可以可以配置多个消费分组
 * 2.2  消费分组 和 topic 关系 （多对多）
 * 一个消费分组可能订阅一个或多个topic
 * 一个topic可能被一个或多个消费分组订阅
 * 2.3  topic 和 消费队列关系 ？
 * 创建topic可以选择在一个broker节点分配多个消费队列
 * 因此MQ消费客户端实例 和  消息队列 关系 多对多
 * <p>
 * 3 RebalanceImpl 是一个抽象类，实现如下功能
 * 3.1 用来对当前MQ消费客户端实例，在当前消费分组，负载均衡分配消息队列，存储在本地
 * 3.2 通过allocateMessageQueueStrategy 支持多种负载均衡策略
 * 3.3 每一个分配给当前MQ消费客户端实例的消费队列都会转换成PullRequest，添加到 PullMessageService.pullRequestQueue
 * 3.4 如果分配消费队列发生变更，更新本地，重新添加到 PullMessageService.pullRequestQueue
 * <p>
 * RebalanceImpl 多个子类扩展模板方法实现
 * RebalanceLitePullImpl (org.apache.rocketmq.client.impl.consumer)
 * RebalancePushImpl (org.apache.rocketmq.client.impl.consumer)
 * RebalancePullImpl (org.apache.rocketmq.client.impl.consumer)
 *
 * <p>
 * 4 什么是负载均衡分配消息队列
 * <p>
 * 消费分组存在2个MQ消费客户端实例
 * 消费分组订阅topic 存在 5个消息队列
 * <p>
 * 如果是广播消费
 * <p>
 * 每个MQ消费客户端实例分配5个消费队列消费
 * 如果是集群消费
 * MQ消费客户端实例A 分配 3个消息队列
 * MQ消费客户端实例B 分配 2个消息队列
 * <p>
 * 5 什么是当前MQ消费客户端实例
 * 就是启动当前代码MQ消费客户端实例 如果是A，那么分配3个消息队列在processQueueTable
 * 就是启动当前代码MQ消费客户端实例 如果是A，那么分配2个消息队列在processQueueTable
 */
public abstract class RebalanceImpl {

    /**
     * 内部日志
     */
    protected static final InternalLogger log = ClientLogger.getLog();

    /**
     * 当前MQ消费客户端实例，在当前消费分组分配消息队列，以及消费队列对应消息处理队列Map
     */
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);

    /**
     * topic以及对topic消息队列
     * 该属性数据会从Namerser获取topic对应的路由信息，更新到MQClientInstance客户端实例 属性时同步更新
     */
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
            new ConcurrentHashMap<String, Set<MessageQueue>>();


    /**
     * 当前消费分组订阅topic，以及topic订阅配置信息
     * 该属性数据会在如下2个地方进行初始化：
     * <p>
     * 1 Consumer.start()启动时会读取当前Consumer所有订阅配置，使用FilterAPI转为SubscriptionData注册到rebalanceImpl
     * this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
     * <p>
     * 2 手动调用subscribe，使用FilterAPI转为SubscriptionData注册到rebalanceImpl
     * this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
     */
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
            new ConcurrentHashMap<String, SubscriptionData>();

    /**
     * 消费分组
     */
    protected String consumerGroup;

    /**
     * 消息模型
     */
    protected MessageModel messageModel;

    /**
     * MQ消费客户端实例分配MessageQueue策略
     */
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    /**
     * MQ客户端实例对象
     */
    protected MQClientInstance mQClientFactory;


    /**
     * 构造 RebalanceImpl
     *
     * @param consumerGroup                消费分组
     * @param messageModel                 消息模型
     * @param allocateMessageQueueStrategy 分配MessageQueue策略
     * @param mQClientFactory              MQ客户端实例对象
     */
    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
                         AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                         MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    /************************************** 加锁/解锁相关接口 **************************************/

    /**
     * 对指定消息队列解锁
     *
     * @param mq     消息队列
     * @param oneway 是否请求broker是单向
     */
    public void unlock(final MessageQueue mq, final boolean oneway) {
        //在MQClientINSTANCE 客户端实例中 指定broker节点，master broker实例类型的实例查询结果
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        //判断查询结果
        if (findBrokerResult != null) {
            //构造消息队列解锁请求
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);
            try {
                //使用MQ远程客户端实现向broker实例发送请求，对指定消息队列解锁
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                //打印日志
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                        this.consumerGroup,
                        this.mQClientFactory.getClientId(),
                        mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    /**
     * 获取当前MQ消费客户端实例，在当前消费分组（当前对象所属MQConsumer） 解锁
     *
     * @param oneway 是否请求broker是单向
     */
    public void unlockAll(final boolean oneway) {
        //获取当前MQ消费客户端实例，在当前消费分组（当前对象所属MQConsumer），负载均衡分配消息队列。按照broker节点分组
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();
        //遍历 brokerMqs
        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            //获取broker节点名称
            final String brokerName = entry.getKey();
            //获取该节点分配消息队列（对当前MQ消费客户端实例）
            final Set<MessageQueue> mqs = entry.getValue();

            //如果消息队列集合为空直接返回
            if (mqs.isEmpty())
                continue;

            //在MQClientINSTANCE 客户端实例中 broker节点，master broker实例类型的实例查询结果
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            //判断查询结果
            if (findBrokerResult != null) {
                //构造消息队列解锁请求
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);
                try {
                    //使用MQ远程客户端实现向broker实例发送请求，对当前MQ消费客户端实例 负载均衡分配消息队列 解锁
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    //对MessageQueue在processQueueTable对应的ProcessQueue解锁
                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }


    /**
     * 对指定消息队列加锁
     *
     * @param mq 消息队列
     * @return
     */
    public boolean lock(final MessageQueue mq) {
        //在MQClientINSTANCE 客户端实例中 指定broker节点，master broker实例类型的实例查询结果
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        //判断查询结果
        if (findBrokerResult != null) {
            //构造消息队列加锁请求
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                //使用MQ远程客户端实现向broker实例发送请求，对指定消息队列加锁
                Set<MessageQueue> lockedMq =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                //对MessageQueue在processQueueTable对应的ProcessQueue加锁
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                //判断返回已锁定消息队列中是否存在指定消费队列，存在则锁定成功
                boolean lockOK = lockedMq.contains(mq);
                //打印日志
                log.info("the message queue lock {}, {} {}",
                        lockOK ? "OK" : "Failed",
                        this.consumerGroup,
                        mq);
                //返回锁定成功
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }
        //返回锁定失败
        return false;
    }

    /**
     * 获取当前MQ消费客户端实例，在当前消费分组（当前对象所属MQConsumer） 负载均衡分配消息队列 加锁
     */
    public void lockAll() {
        //获取当前MQ消费客户端实例，在当前消费分组（当前对象所属MQConsumer），负载均衡分配消息队列。按照broker节点分组
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();
        //获取brokerMqs迭代器
        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        //对brokerMqs迭代
        while (it.hasNext()) {
            //获取Entry
            Entry<String, Set<MessageQueue>> entry = it.next();
            //获取broker节点
            final String brokerName = entry.getKey();
            //获取消费队列
            final Set<MessageQueue> mqs = entry.getValue();

            //如果消息队列集合为空直接返回
            if (mqs.isEmpty())
                continue;

            //在MQClientINSTANCE 客户端实例中 broker节点，master broker实例类型的实例查询结果
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            //判断查询结果
            if (findBrokerResult != null) {
                //构造消息队列加锁请求
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    //使用MQ远程客户端实现向broker实例发送请求，对当前MQ消费客户端实例 负载均衡分配消息队列 加锁
                    Set<MessageQueue> lockOKMQSet =
                            this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);


                    //对返回已加锁消费队列进行遍历
                    //对在processQueueTable对应的ProcessQueue加锁
                    for (MessageQueue mq : lockOKMQSet) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }
                            //设置枷锁
                            processQueue.setLocked(true);
                            //设置最后加锁时间戳
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    //对于没有加锁成功消费队列进行遍历
                    //对在processQueueTable对应的ProcessQueue解锁
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    /**
     * 获取当前MQ消费客户端实例，在当前消费分组（当前对象所属MQConsumer），负载均衡分配消息队列。按照broker节点分组
     *
     * @return 当前MQ消费客户端实例负载均衡分配消息队列, 按照broker节点分组
     */
    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        //记录当前MQ消费客户端实例负载均衡分配消息队列,按照broker节点分组
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
        //遍历 processQueueTable，将其中所有MessageQueue 按照照broker节点分组 添加到result
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.get(mq.getBrokerName());
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }
            mqs.add(mq);
        }
        //返回
        return result;
    }

    /************************************** 当前MQ消费客户端实例，在当前消费分组（当前对象所属MQConsumer），负载均衡分配消息队列 **************************************/

    /**
     * 1 当前MQ消费客户端实例，在当前消费分组（当前对象所属MQConsumer），负载均衡分配消息队列
     * <p>
     * 2 该方法由MQ消费客户端实例负载均衡服务定时调用
     * RebalanceImpl.doRebalance(boolean)  (org.apache.rocketmq.client.impl.consumer)
     * MQClientInstance.doRebalance()  (org.apache.rocketmq.client.impl.factory)
     * RebalanceService.run()  (org.apache.rocketmq.client.impl.consumer)
     * <p>
     * 3 每次调用会将当前MQ消费客户端实例，在当前消费分组分配消息队列记录到本地 processQueueTable
     * <p>
     * 4 在第一次调用或会分配消费队列发生变更时会创建一个PullRequest,添加到 PullMessageService.pullRequestQueue
     *
     * @param isOrder 是否顺序
     */
    public void doRebalance(final boolean isOrder) {
        //当前消费分组订阅topic，以及topic订阅配置信息
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        //判断是否为null
        if (subTable != null) {
            //遍历消费分组订阅topic以及topic订阅配置信息
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                //获取topic
                final String topic = entry.getKey();
                try {
                    //对指定topic的消费队列进行负载均衡（针对当前MQ消费客户端实例，在当前消费分组（当前对象所属MQConsumer））
                    this.rebalanceByTopic(topic, isOrder);
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }
        //从processQueueTable删除接触订阅关系的消息队列
        this.truncateMessageQueueNotMyTopic();
    }


    /**
     * 对指定topic的消费队列进行负载均衡（针对当前MQ消费客户端实例，在当前消费分组（当前对象所属MQConsumer））
     *
     * @param topic   消息topic
     * @param isOrder 是否顺序
     */
    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        //判断消费分组消息模型
        switch (messageModel) {
            //广播
            case BROADCASTING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}",
                                consumerGroup,
                                topic,
                                mqSet,
                                mqSet);
                    }
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            //集群
            case CLUSTERING: {
                //获取topic所有消息队列
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                //查询指定消费分组所有MQ消费客户端实例
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);

                //如果消息队列为null且不时重试队列,打印日志
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }
                //如果消费分组所有MQ消费客户端实例为null,打印日志
                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }
                //判断mqSet/cidAll都不为null
                if (mqSet != null && cidAll != null) {
                    //对消费队列排序
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                    mqAll.addAll(mqSet);
                    Collections.sort(mqAll);

                    //对MQ消费客户端实例列表排序
                    Collections.sort(cidAll);

                    //记录MQ消费客户端实例分配MessageQueue策略
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    //记录当前MQ消费客户端实例，在当前消费分组分配的消息队列列表
                    List<MessageQueue> allocateResult = null;
                    try {
                        //使用MQ消费客户端实例分配MessageQueue策略
                        //获取当前MQ消费客户端实例，在当前消费分组分配的消息队列
                        allocateResult = strategy.allocate(
                                this.consumerGroup,
                                this.mQClientFactory.getClientId(),
                                mqAll,
                                cidAll);
                    } catch (Throwable e) {
                        log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(),
                                e);
                        return;
                    }
                    //当前MQ消费客户端实例，在当前消费分组分配的消息队列放入集合
                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }

                    //判断当前分配消息队列和processQueueTable存储已分配消息队列是否发生变更
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);

                    //如果发生变更
                    if (changed) {
                        log.info(
                                "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                                strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                                allocateResultSet.size(), allocateResultSet);
                        //调用消费队列发生变更（模板方法）
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    /**
     * 从processQueueTable删除接触订阅关系的消息队列
     */
    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }

    /**
     * 判断当前分配消息队列和processQueueTable存储已分配消息队列是否发生变更
     *
     * @param topic   消息topic
     * @param mqSet   当前分配消息队列
     * @param isOrder 是否保证顺序消费
     * @return
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
                                                       final boolean isOrder) {
        //记录是否发生变更
        boolean changed = false;

        //获取processQueueTable迭代器
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        //对processQueueTable迭代
        while (it.hasNext()) {
            //获取Entry
            Entry<MessageQueue, ProcessQueue> next = it.next();
            //获取消息队列
            MessageQueue mq = next.getKey();
            //获取消息处理队列
            ProcessQueue pq = next.getValue();

            //判断原始消息队列topic是否相同
            if (mq.getTopic().equals(topic)) {
                //判断原始消息队列是否在当前分配队列中
                if (!mqSet.contains(mq)) {
                    //丢弃放弃当前消息队列
                    pq.setDropped(true);
                    //删除不必要的MessageQueue
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                        //从processQueueTable删除
                        it.remove();
                        //设置发生变更
                        changed = true;
                        //打印日志
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                }
                //判断原始消息处理队列已经超时指定最大空间时间（没有在拉取消息）
                else if (pq.isPullExpired()) {
                    //判断消费类型
                    switch (this.consumeType()) {
                        //拉
                        case CONSUME_ACTIVELY:
                            break;
                        //推
                        case CONSUME_PASSIVELY:
                            //丢弃放弃当前消息队列
                            pq.setDropped(true);
                            //删除不必要的MessageQueue
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                //从processQueueTable删除
                                it.remove();
                                //设置发生变更
                                changed = true;
                                //打印日志
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                        consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        //创建拉请求列表
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        //遍历新分配拉取消息队列
        for (MessageQueue mq : mqSet) {
            //如果不存在于本地 processQueueTable
            if (!this.processQueueTable.containsKey(mq)) {
                //如果是顺序消费，对消息队列加锁,如果失败跳过
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }
                //清空消息队列进度（集群，清理本地）
                this.removeDirtyOffset(mq);
                //为新添加消费队列创建消息处理队列
                ProcessQueue pq = new ProcessQueue();
                //获取消费队列下次拉取逻辑偏移量
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    //添加到processQueueTable，不存在添加成功，存在返回原始值
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        //创建拉取请求
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        //添加到pullRequestList
                        pullRequestList.add(pullRequest);
                        //设置发生变更
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }
        //添加到添加到 PullMessageService.pullRequestQueue
        this.dispatchPullRequest(pullRequestList);
        //返回是否发生变更
        return changed;
    }

    /**
     * 对当前消费客户端实例对当前消费分组分配消费队列发生变更后回调操作
     *
     * @param topic     消息topic
     * @param mqAll     新分配消费队列
     * @param mqDivided 原始消费队列
     */
    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
                                             final Set<MessageQueue> mqDivided);

    /**
     * 对从processQueueTable中删除消费队列进行后回调操作
     * 1 对消费队列进度进行持久化
     * 2 清空指定消息队列本地消费进度缓存
     * 3 如果是顺序消费，且是集群，对消费队列解锁
     *
     * @param mq 消息队列
     * @param pq 消息处理队列
     * @return
     */
    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    /**
     * 获取消费类型（模板方法）
     *
     * @return 消费类型
     */
    public abstract ConsumeType consumeType();

    /**
     * 清空消息队列进度 （模板方法）
     *
     * @param mq
     */
    public abstract void removeDirtyOffset(final MessageQueue mq);

    /**
     * 获取消费队列下次拉取逻辑偏移量 （模板方法）
     *
     * @param mq 消息队列
     * @return
     */
    public abstract long computePullFromWhere(final MessageQueue mq);

    /**
     * 添加到添加到 PullMessageService.pullRequestQueue （模板方法）
     *
     * @param pullRequestList 拉取请求列表
     */
    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);


    /**
     * 获取 processQueueTable
     *
     * @return 消息处理队列表格
     */
    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    /**
     * 从 processQueueTable 删除指定消费队列
     *
     * @param mq 消费队列
     */
    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            //标记丢弃
            prev.setDropped(true);
            //对从processQueueTable中删除消费队列进行后续操作
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    /**
     * 销毁 processQueueTable
     */
    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }


}
