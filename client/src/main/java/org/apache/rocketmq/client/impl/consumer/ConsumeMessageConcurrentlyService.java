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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 并发消费消息服务
 * <p>
 * 1 负责消费{@link PullRequest}拉取请求拉取的消息
 * 1.1 每一个拉取请求对应了某个消费分组对指定消息队列拉取消息请求。处理拉取请求核心实现 {@link DefaultMQPushConsumerImpl#pullMessage(PullRequest)}
 * 1.2 每一次拉取请求拉取消息会提交到{@link ConsumeMessageService}
 * 1.3 提交到{@link ConsumeMessageService}消息会根据{@link DefaultMQPushConsumer#consumeMessageBatchMaxSize}
 * 配置将消息拆分到一个或多个{@link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest}消费请求任务
 * 提交到消息线程池消费
 * 1.4 {@link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest}
 * 将需要消费的消息回调{@link org.apache.rocketmq.client.consumer.listener.MessageListener}
 * 2 负责消费 broker接收管理后台重发的消息
 */
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {

    /**
     * 内部日志
     */
    private static final InternalLogger log = ClientLogger.getLog();

    /**
     * MQConsumerInner内部核心实现
     */
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    /**
     * Consumer客户端对象
     */
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    /**
     * 并发消息监听器
     */
    private final MessageListenerConcurrently messageListener;

    /**
     * 线程池阻塞队列
     */
    private final BlockingQueue<Runnable> consumeRequestQueue;

    /**
     * 消费消息线程池
     */
    private final ThreadPoolExecutor consumeExecutor;

    /**
     * 延时任务
     * 当{@link ConsumeRequest}消费请求任务失败时，延时一段时间提交到consumeExecutor
     */
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * 消费分组
     */
    private final String consumerGroup;

    /**
     * 清理消息处理队列中过期的消息服务
     */
    private final ScheduledExecutorService cleanExpireMsgExecutors;


    /**
     * @param defaultMQPushConsumerImpl
     * @param messageListener
     */
    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
                                             MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(
                this.defaultMQPushConsumer.getConsumeThreadMin(),
                this.defaultMQPushConsumer.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.consumeRequestQueue,
                new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    /***************** ConsumeMessageService 接口实现 *****************/

    /**
     * 启动
     */
    public void start() {
        //启动清理过期消息处理队列中过期的消息服务
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                cleanExpireMsg();
            }
        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    /**
     * 获取所有当前MQ消费客户端实例，在当前消费分组，消息处理队列
     * 清理所有清理消息处理队列中过期的消息
     */
    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
                this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    /**
     * 关闭
     *
     * @param awaitTerminateMillis 关闭线程池等待超时时间
     */
    public void shutdown(long awaitTerminateMillis) {
        //关闭定时任务
        this.scheduledExecutorService.shutdown();
        //关闭消费消息线程池
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        //关闭清理过期消息处理队列中过期的消息服务
        this.cleanExpireMsgExecutors.shutdown();
    }


    /**
     * 更新消费消息服务中核心线程数
     *
     * @param corePoolSize 核心线程数
     */
    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
                && corePoolSize <= Short.MAX_VALUE
                && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    /**
     * 核心线程数+1
     */
    @Override
    public void incCorePoolSize() {
    }

    /**
     * 核心线程数-1
     */
    @Override
    public void decCorePoolSize() {
    }

    /**
     * 获取消费消息服务中核心线程数
     *
     * @return 核心线程数
     */
    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }


    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }


    /**
     * 处理待消费的消息
     * 1 获取根据每次执行MessageListener#consumerMessage传入消息的数量配置
     * 2 判断提交待消息的消息数量如果小于配置数量，创建一个{@link ConsumeRequest}消费请求任务，提交到消费消息线程池执行
     * 3 判断提交待消息的消息数量如果小于配置数量，创建多个{@link ConsumeRequest}消费请求任务，提交到消费消息线程池执行
     * <p>
     * <p>
     * 消费 {@link DefaultMQPushConsumerImpl#pullMessage(PullRequest)}中获取的消息
     *
     * @param msgs              待消费的消息
     * @param processQueue      执行队列
     * @param messageQueue      消费队列
     * @param dispatchToConsume 派遣消费
     */
    @Override
    public void submitConsumeRequest(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final boolean dispatchToConsume) {
        //获取每次执行MessageListener#consumerMessage传入消息的数量配置
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        //判断提交待消息的消息数量如果小于配置数量，创建一个{@link ConsumeRequest}消费请求任务，提交到消费消息线程池执行
        if (msgs.size() <= consumeBatchSize) {
            //创建一个消费请求
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                //将消费请求任务提交到，消费消息线程池
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                //当{@link ConsumeRequest}消费请求任务失败时，延时一段时间提交到consumeExecutor
                this.submitConsumeRequestLater(consumeRequest);
            }
        }
        //判断提交待消息的消息数量如果小于配置数量，创建多个{@link ConsumeRequest}消费请求任务，提交到消费消息线程池执行
        else {
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }
                //创建一个消费请求
                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    //将消费请求任务提交到，消费消息线程池
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }
                    //当{@link ConsumeRequest}消费请求任务失败时，延时一段时间提交到consumeExecutor
                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    /**
     * 当{@link ConsumeRequest}消费请求任务失败时，延时一段时间提交到consumeExecutor
     *
     * @param consumeRequest 消费请求
     */
    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest
    ) {
        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }


    /**
     * 如果consumer消费消息失败，consumer将延迟消耗一些时间，将消息将被发送回broker，
     * <p>
     * 在2020年4月5日之后，此方法将被删除或在某些版本中其可见性将更改，因此请不要使用此方法。
     *
     * @param msg     发送返回消息
     * @param context 并发消费上下文
     * @throws RemotingException    远程调用异常
     * @throws MQBrokerException    broker异常
     * @throws InterruptedException 中断异常
     * @throws MQClientException    客户端异常
     */
    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // 在发送回消息之前，将主题与名称空间包装在一起。
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            // 如果consumer消费消息失败，consumer将延迟消耗一些时间，将消息将被发送回broker，
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }
        return false;
    }


    /***************** ConsumeRequest 相关方法 *****************/
    /**
     * 消费消息请求
     */
    class ConsumeRequest implements Runnable {
        /**
         * 待消费的消息
         */
        private final List<MessageExt> msgs;
        /**
         * 消息处理队列
         */
        private final ProcessQueue processQueue;
        /**
         * 消息队列
         */
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {
            //如果消息处理队列被丢弃直接返回
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }
            //获取并发消息监听器
            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            //创建并发消费上下文
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            //记录并发消费消息状态
            ConsumeConcurrentlyStatus status = null;
            //如果消息名称空间不为null，则重置没有名称空间的Topic。
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            /** 执行消费消息前钩子回调 **/
            //记录消费消息钩子上下文
            ConsumeMessageContext consumeMessageContext = null;
            //消费消息钩子列表是否不为空
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                //成功消费消息钩子上下文
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                //执行消费消息钩子（消费消息前操作）
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            //记录开始调用并发消息监听器时间戳
            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                //设置消息属性 消息开始提交给并发消息监听器消费的时间
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }
                //调用并发消息监听回调业务消费消息，获取消费消息状态
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                        RemotingHelper.exceptionSimpleDesc(e),
                        ConsumeMessageConcurrentlyService.this.consumerGroup,
                        msgs,
                        messageQueue);
                hasException = true;
            }
            //计算业务方并发消息监听器消响应时间
            long consumeRT = System.currentTimeMillis() - beginTimestamp;

            //判断消费消息状态
            if (null == status) {
                //如果发送异常设置返回类型
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                }
                //如果未发生异设置返回类型
                else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            }
            //如果消费消息状态超时
            else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;
            }
            //如果消费消息状态为重试
            else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;
            }
            //如果消费消息状态为重成功
            else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            //记录消费消息钩子上下文中添加消息在消息监听器返回类型
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            //如果消费消息状态为为null，则设置状态为重试
            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                        ConsumeMessageConcurrentlyService.this.consumerGroup,
                        msgs,
                        messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            /** 执行消费消息后钩子回调 **/
            //消费消息钩子列表是否不为空
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }
            //获取获取消费统计服务
            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                    .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            //如果消息处理队列未丢弃，处理消息消费结果
            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }

    /**
     * 处理消息消费结果
     *
     * @param status         消息并发状态
     * @param context        并发消费上下文
     * @param consumeRequest 消费消息请求
     */
    public void processConsumeResult(
            final ConsumeConcurrentlyStatus status,
            final ConsumeConcurrentlyContext context,
            final ConsumeRequest consumeRequest
    ) {
        //获取ACK索引
        int ackIndex = context.getAckIndex();

        //如果消息请求中消息为空直接返回
        if (consumeRequest.getMsgs().isEmpty())
            return;

        //判断消息消费状态
        switch (status) {
            //消息消费成功
            case CONSUME_SUCCESS:
                // 消费成功， 如过ack确认下标大于消息数量，则进行重置
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                // 成功数量
                int ok = ackIndex + 1;
                // 失败数量
                int failed = consumeRequest.getMsgs().size() - ok;
                //获取消费统计服务统计成功数量
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                //获取消费统计服务统计失败的数量
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            //消息消费重试
            case RECONSUME_LATER:
                ackIndex = -1;
                //获取消费统计服务统计失败的数量
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                        consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }
        //判断消费模型
        switch (this.defaultMQPushConsumer.getMessageModel()) {
            //广播消费
            case BROADCASTING:
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            //集群消费
            case CLUSTERING:
                //记录失败的消息
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
                //遍历消费请求中的下下哦
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    //获取消息
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }
                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);

                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }
        //从消息处理队列中删除消息请求的消息
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        //更新指定消费队列缓存中消费进度
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    private void submitConsumeRequestLater(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue
    ) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }


    /***************** 属性相关方法 *****************/

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }
}
