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

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 拉取消费对 RebalanceImpl MQ消费客户端实例分配消费队列负载均衡实现扩展
 */
public class RebalancePushImpl extends RebalanceImpl {

    /**
     * 拉取解锁延迟时间戳
     */
    private final static long UNLOCK_DELAY_TIME_MILLS = Long.parseLong(System.getProperty("rocketmq.client.unlockDelayTimeMills", "20000"));


    /**
     * 消费分组客户端内部核心实现
     */
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    /**
     * RebalancePushImpl 构造函数
     *
     * @param defaultMQPushConsumerImpl 消费分组客户端内部核心实现
     */
    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }


    /**
     * RebalancePushImpl 构造函数
     *
     * @param consumerGroup                消费分组
     * @param messageModel                 消费模式
     * @param allocateMessageQueueStrategy 分配消费队列策略
     * @param mQClientFactory              MQ客户端实例
     * @param defaultMQPushConsumerImpl    消费分组客户端内部核心实现
     */
    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
                             AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                             MQClientInstance mQClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }

    /**
     * 对当前消费客户端实例对当前消费分组分配消费队列发生变更后回调操作
     * 1 变更订阅配置中版本
     * 2 更新配置主题级别的流量控制阈值
     * 2 更新配置主题级别限制缓存消息大小
     *
     * @param topic     消息topic
     * @param mqAll     新分配消费队列
     * @param mqDivided 原始消费队列
     */
    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        //当前消费分组订阅topic，以及topic订阅配置信息
        SubscriptionData subscriptionData = this.subscriptionInner.get(topic);
        long newVersion = System.currentTimeMillis();
        //变更订阅配置中版本
        log.info("{} Rebalance changed, also update version: {}, {}", topic, subscriptionData.getSubVersion(), newVersion);
        subscriptionData.setSubVersion(newVersion);

        //获取processQueueTable数量
        int currentQueueCount = this.processQueueTable.size();
        //如果不是0
        if (currentQueueCount != 0) {
            //判断是否配置主题级别的流量控制阈值
            int pullThresholdForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForTopic();
            if (pullThresholdForTopic != -1) {
                //重新计算
                int newVal = Math.max(1, pullThresholdForTopic / currentQueueCount);
                log.info("The pullThresholdForQueue is changed from {} to {}",
                        this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForQueue(), newVal);
                //更新
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdForQueue(newVal);
            }

            //判断是否配置主题级别限制缓存消息大小
            int pullThresholdSizeForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForTopic();
            if (pullThresholdSizeForTopic != -1) {
                //重新计算
                int newVal = Math.max(1, pullThresholdSizeForTopic / currentQueueCount);
                log.info("The pullThresholdSizeForQueue is changed from {} to {}",
                        this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForQueue(), newVal);
                //更新
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(newVal);
            }
        }

        //获取注册到 MQClientInstance客户端实例 的所有broker实例发送 MQClientInstance客户端实例 HeartbeatData心跳数据
        this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
    }

    /**
     * 对从processQueueTable中删除消费队列回调操作
     * 1 对消费队列进度进行持久化
     * 2 清空指定消息队列本地消费进度缓存
     * 3 如果是顺序消费，且是集群，对消费队列解锁
     *
     * @param mq 消息队列
     * @param pq 消息处理队列
     * @return
     */
    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        //对消费队列进度进行持久化 （集群模式发送到远程到broker，广播模式写入本地文件）
        this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
        //清空指定消息队列本地消费进度缓存
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
        //如果是顺序消费，且是集群，对消费队列解锁
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
                && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            try {
                //加锁
                if (pq.getLockConsume().tryLock(1000, TimeUnit.MILLISECONDS)) {
                    try {
                        //消息队列解锁
                        return this.unlockDelay(mq, pq);
                    } finally {
                        //解锁
                        pq.getLockConsume().unlock();
                    }
                } else {
                    //加锁失败情况打印 尝试解除消息处理队列锁次数
                    log.warn("[WRONG]mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",
                            mq,
                            pq.getTryUnlockTimes());
                    //尝试解除消息处理队列锁次数+1
                    pq.incTryUnlockTimes();
                }
            } catch (Exception e) {
                log.error("removeUnnecessaryMessageQueue Exception", e);
            }

            return false;
        }
        return true;
    }

    /**
     * 对消费队列解锁
     *
     * @param mq 消费队列
     * @param pq 消息处理队列
     * @return
     */
    private boolean unlockDelay(final MessageQueue mq, final ProcessQueue pq) {
        //如果消息处理队列存在消息，延时解锁
        if (pq.hasTempMessage()) {
            log.info("[{}]unlockDelay, begin {} ", mq.hashCode(), mq);
            this.defaultMQPushConsumerImpl.getmQClientFactory().getScheduledExecutorService().schedule(new Runnable() {
                @Override
                public void run() {
                    log.info("[{}]unlockDelay, execute at once {}", mq.hashCode(), mq);
                    //对指定消息队列解锁
                    RebalancePushImpl.this.unlock(mq, true);
                }
            }, UNLOCK_DELAY_TIME_MILLS, TimeUnit.MILLISECONDS);
        } else {
            //对指定消息队列解锁
            this.unlock(mq, true);
        }
        return true;
    }

    /**
     * 获取消费类型
     *
     * @return 消费类型
     */
    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    /**
     * 清空消息队列本地进度缓存
     * <p>
     * 这里如果当前消费模型为集群模式
     * 清空本地缓存
     * 这里如果当前消费模型为广播模式
     * 什么也不做(不存在，只从文件读)
     *
     * @param mq 消息队列
     */
    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    /**
     * 获取消费队列下次拉取逻辑偏移量
     * <p>
     * 这里如果当前消费模型为集群模式
     * 消费进度保存在远程broker，消息回溯依赖远程broker中消费进度
     * 这里如果当前消费模型为广播模式
     * 消费进度保存在本地，在第一次启动,消费队列是第一次消费可以使用客户端配置的ConsumeFromWhere 消费者消费策略进行消费
     * //默认策略，从该队列最尾开始消费，即跳过历史消息
     * CONSUME_FROM_LAST_OFFSET,
     * //从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
     * CONSUME_FROM_FIRST_OFFSET,
     * //从某个时间点开始消费，和setConsumeTimestamp()配合使用，默认是半个小时以前
     * CONSUME_FROM_TIMESTAMP,
     *
     * @param mq 消息队列
     * @return
     */
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1;
        //消费者消费策略
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        //获取消费进度
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        //判断消费者消费策略
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
            case CONSUME_FROM_MIN_OFFSET:
            case CONSUME_FROM_MAX_OFFSET:
                //从该队列最尾开始消费
            case CONSUME_FROM_LAST_OFFSET: {
                //获取消息队列消费进度
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                //如果存在消费进度，记录消费队列下次拉取逻辑偏移量为当前消息队列消费进度
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                //如果当前消费模型为广播模式且消费队列是第一次消费
                else if (-1 == lastOffset) {
                    //如果是重试队列，记录消费队列下次拉取逻辑偏移量为0
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        result = 0L;
                    }
                    //如果非重试队列，获取消息队列存储消息最大逻辑偏移量
                    else {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                }
                //其他情况，记录消费队列下次拉取逻辑偏移量为-1
                else {
                    result = -1;
                }
                break;
            }
            //从队列最开始开始消费
            case CONSUME_FROM_FIRST_OFFSET: {
                //获取消息队列消费进度（集群从broker）
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                //如果存在消费进度，记录消费队列下次拉取逻辑偏移量为当前消息队列消费进度
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                //如果当前消费模型为广播模式且消费队列是第一次消费
                else if (-1 == lastOffset) {
                    result = 0L;
                }
                //其他情况，记录消费队列下次拉取逻辑偏移量为-1
                else {
                    result = -1;
                }
                break;
            }
            //从某个时间点开始消费
            case CONSUME_FROM_TIMESTAMP: {
                //获取消息队列消费进度（广播模式读取本地文件，集群读取远程broker）
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                //如果存在消费进度，记录消费队列下次拉取逻辑偏移量为当前消息队列消费进度
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                //如果当前消费模型为广播模式且消费队列是第一次消费
                else if (-1 == lastOffset) {
                    //如果重试队列，获取消息队列存储消息最大逻辑偏移量
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                    //如果非重试队列，获取消息队列指定时间戳对应消息逻辑偏移量
                    else {
                        try {
                            long timestamp = UtilAll.parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(),
                                    UtilAll.YYYYMMDDHHMMSS).getTime();
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                }
                //其他情况，记录消费队列下次拉取逻辑偏移量为-1
                else {
                    result = -1;
                }
                break;
            }

            default:
                break;
        }
        //返回
        return result;
    }

    /**
     * 添加到添加到 PullMessageService.pullRequestQueue
     *
     * @param pullRequestList 拉取请求列表
     */
    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
        for (PullRequest pullRequest : pullRequestList) {
            this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest);
        }
    }
}
