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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * 消息处理队列
 * 1 存放{@link PullRequest}拉取请求拉取消息，缓存在msgTreeMap
 */
public class ProcessQueue {

    /**
     * 消息队列锁定时间超过最大锁定时间
     */
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
            Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));

    /**
     *
     */
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));

    /**
     * 消费队列拉取消息时间超多最大间隔
     */
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));

    /**
     * 内部日志
     */
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 本地缓存消息（待处理的消息）
     * 这里使用TreeMap,key在添加时候会排序
     * key 存放消息在消息队列位置（逻辑偏移量）
     * value 存放消息
     */
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();

    /**
     * msgTreeMap子集，顺序消息时使用
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();

    /**
     * msgTreeMap读写锁
     */
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();

    /**
     * msgTreeMap 本地缓存消息的总数量
     */
    private final AtomicLong msgCount = new AtomicLong();

    /**
     * msgTreeMap 本地缓存消息的总大小
     */
    private final AtomicLong msgSize = new AtomicLong();

    /**
     * 记录msgTreeMap 本地缓存最后一条添加的消息位置和当前消费队列最大位置差距
     */
    private volatile long msgAccCnt = 0;

    /**
     * 记录msgTreeMap 本地缓存中最后一条消息在消息队列中位置(逻辑偏移量)
     */
    private volatile long queueOffsetMax = 0L;

    /**
     * ProcessQueue 锁
     */
    private final Lock lockConsume = new ReentrantLock();

    /**
     * 尝试加锁次数
     */
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);


    /**
     * 标记丢弃，如果丢弃不在拉取消息
     */
    private volatile boolean dropped = false;

    /**
     * 最后一次拉取时间戳
     */
    private volatile long lastPullTimestamp = System.currentTimeMillis();

    /**
     * 最后一次消费时间戳
     */
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();

    /**
     * 是否锁定（一般消息队列需要重新负载均衡分配给MQ消费客户端实例时会锁定，具体实现在 RebalanceImpl 消费者实例负载均衡服务中）
     */
    private volatile boolean locked = false;

    /**
     * 最后锁定时间
     */
    private volatile long lastLockTimestamp = System.currentTimeMillis();

    /**
     * 是否正在消费
     */
    private volatile boolean consuming = false;


    /**
     * 消息队列锁定时间超过最大锁定时间
     *
     * @return
     */
    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    /**
     * 当前消费队列拉取消息时间超多最大间隔
     *
     * @return
     */
    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * 清理清理消息处理队列中缓存过期的消息
     *
     * @param pushConsumer
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        //如果是顺序消费直接返回
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }
        //计算遍历消息的数量，最多16条
        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
        //遍历本地消息位置最小的 16条消息
        for (int i = 0; i < loop; i++) {
            //记录过期的消息
            MessageExt msg = null;
            try {
                //获取lockTreeMap锁，可中断
                this.lockTreeMap.readLock().lockInterruptibly();
                try {
                    //判断消息是否过期
                    if (!msgTreeMap.isEmpty() && System.currentTimeMillis() - Long.parseLong(MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue())) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                        msg = msgTreeMap.firstEntry().getValue();
                    }
                    //没有过期跳过
                    else {
                        break;
                    }
                } finally {
                    //释放lockTreeMap锁
                    this.lockTreeMap.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {
                //将过期消息发送回broker
                pushConsumer.sendMessageBack(msg, 3);
                //打印日志
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    //获取lockTreeMap锁，可中断
                    this.lockTreeMap.writeLock().lockInterruptibly();
                    try {
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                //向消息处理队列中删除消息
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        //释放lockTreeMap锁
                        this.lockTreeMap.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    //打印中断异常日志
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                //打印异常日志
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * 向消息处理队列中添加消息
     *
     * @param msgs 消息
     * @return 是否派遣消费
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        boolean dispatchToConsume = false;
        try {
            //获取lockTreeMap锁，可中断
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                //记录新添加消息数量
                int validMsgCnt = 0;
                //遍历消息列表，
                for (MessageExt msg : msgs) {
                    //添加到消息处理队列缓存msgTreeMap
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    //判断消息是否已添加
                    if (null == old) {
                        //新添加消息数量+1
                        validMsgCnt++;

                        this.queueOffsetMax = msg.getQueueOffset();
                        //增加本地缓存消息的总大小
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }
                //增加本地缓存消息的总数量
                msgCount.addAndGet(validMsgCnt);


                //如果添加消息存在，且是第一次添加消息
                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    //设置派遣消费
                    dispatchToConsume = true;
                    //标记开始消费
                    this.consuming = true;
                }

                //记录msgTreeMap 本地缓存最后一条添加的消息位置和当前消费队列最大位置差距
                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                //释放lockTreeMap锁
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
        //是否派遣消费
        return dispatchToConsume;
    }

    /**
     * 获取缓存消息中最后一条消息位置和第一条消息位置（逻辑偏移量）的最大位置（逻辑偏移量）间隔
     *
     * @return 最大间隔
     */
    public long getMaxSpan() {
        try {
            //获取lockTreeMap锁
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                //获取缓存消息中最后一条消息位置和第一条消息位置（逻辑偏移量）的最大位置（逻辑偏移量）间隔
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                //释放lockTreeMap锁
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * 向消息处理队列中删除消息
     *
     * @param msgs 消息
     * @return 返回删除后本地缓存位置最小的消息位置
     */
    public long removeMessage(final List<MessageExt> msgs) {
        long result = -1;
        //记录开始时间
        final long now = System.currentTimeMillis();
        try {
            //获取lockTreeMap锁
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                //本地缓存中存在消息
                if (!msgTreeMap.isEmpty()) {
                    //设置删除后本地缓存位置最小的消息位置的初始值
                    result = this.queueOffsetMax + 1;

                    //记录本次删除消息数量
                    int removedCnt = 0;
                    //遍历消息列表，
                    for (MessageExt msg : msgs) {
                        //从本地缓存中删除
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        //如果缓存中存在
                        if (prev != null) {
                            //removedCnt-1
                            removedCnt--;
                            //重新计算设置msgTreeMap 本地缓存消息的总大小
                            msgSize.addAndGet(0 - msg.getBody().length);
                        }
                    }
                    ///重新计算设置msgTreeMap 本地缓存消息的总数量
                    msgCount.addAndGet(removedCnt);

                    //如果本地缓存中还在在消息，返回缓存中位置在在消息队列中最前消息位置
                    if (!msgTreeMap.isEmpty()) {
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                //释放lockTreeMap锁
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (Throwable t) {
            //打印异常日志
            log.error("removeMessage exception", t);
        }

        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }


    /********************* consumingMsgOrderlyTreeMap 相关  *********************/

    /**
     * 回滚
     * 1 将consumingMsgOrderlyTreeMap消息添加到msgTreeMap
     * 2 清空consumingMsgOrderlyTreeMap
     */
    public void rollback() {
        try {
            //获取lockTreeMap锁
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                //将consumingMsgOrderlyTreeMap消息添加到msgTreeMap
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                //清空consumingMsgOrderlyTreeMap
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                //释放lockTreeMap锁
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     * 提交
     * 1 重新计算设置msgTreeMap 本地缓存消息的总数量，减去consumingMsgOrderlyTreeMap中消息数量
     * 2 重新计算设置msgTreeMap 本地缓存消息的总大小，减去consumingMsgOrderlyTreeMap中消息大小
     * 2 清空consumingMsgOrderlyTreeMap
     *
     * @return 返回consumingMsgOrderlyTreeMap中位置最大消息位置
     */
    public long commit() {
        try {
            //获取lockTreeMap锁
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                //获取consumingMsgOrderlyTreeMap中位置最大消息位置
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                //重新计算设置msgTreeMap 本地缓存消息的总数量，减去consumingMsgOrderlyTreeMap中消息数量
                msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());
                //重新计算设置msgTreeMap 本地缓存消息的总大小，减去consumingMsgOrderlyTreeMap中消息大小
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    msgSize.addAndGet(0 - msg.getBody().length);
                }
                //清空consumingMsgOrderlyTreeMap
                this.consumingMsgOrderlyTreeMap.clear();
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                //释放lockTreeMap锁
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            //打印中断日志
            log.error("commit exception", e);
        }

        return -1;
    }

    /**
     * 标记指定的消息重新消费
     * 1  将消息从consumingMsgOrderlyTreeMap中删除
     * 2  将消息添加到msgTreeMap中
     * @param msgs
     */
    public void makeMessageToCosumeAgain(List<MessageExt> msgs) {
        try {
            //获取lockTreeMap锁
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    //将消息从consumingMsgOrderlyTreeMap中删除
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    //将消息添加到msgTreeMap中
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                //释放lockTreeMap锁
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            //打印中断日志
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    public List<MessageExt> takeMessages(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            result.add(entry.getValue());
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }

                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    /********************* lockTreeMap 相关  *********************/

    /**
     * 消息处理队列是否存在缓存消息
     *
     * @return 处理队列是否存在缓存消息
     */
    public boolean hasTempMessage() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }
        return true;
    }

    /**
     * 清空消息处理队列
     */
    public void clear() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getLockConsume() {
        return lockConsume;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    /**
     * 填充处理消费队列信息
     * @param info
     */
    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.lockTreeMap.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

}
