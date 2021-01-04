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
package org.apache.rocketmq.client.producer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.SendMessageTraceHookImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * DefaultMQProducer 是Producer客户端应用程序API路口.
 * DefaultMQProducer 是Producer核心配置。
 * DefaultMQProducer类实现了MQProducer接口，但并非MQProducer接口核心实现，
 * 其核心实现依赖于内部DefaultMQProducerImpl属性。
 * DefaultMQProducer更多职责是路口和配置
 * <p>
 * DefaultMQProducer使用：
 * <p>
 * 1 客户端通过 下面4个步骤发完消息的发送
 * 创建 DefaultMQProducer defaultMQProducer=new DefaultMQProducer()
 * 配置 DefaultMQProducer.setXXX()
 * 初始化 producer.start()
 * 发送消息  producer.send(msg)
 * 2 DefaultMQProducer发送消息的核心实现交由DefaultMQProducerImpl去实现【Producer核心类】
 * 3 DefaultMQProducer是ClientConfig的子类（提供了针对Producer配置）
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer {

    /**
     * MQProducer接口方法内部内部核心实现类, DefaultMQProducer负责配置功能，大多数功能都委托给其处理执行
     */
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;

    /**
     * 客户端日志
     */
    private final InternalLogger log = ClientLogger.getLog();
    /**
     * 生产者组
     */
    private String producerGroup;

    /**
     * 仅用于测试或演示程序自动生成topic
     */
    private String createTopicKey = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;

    /**
     * 每个Topic默认创建的队列数量
     */
    private volatile int defaultTopicQueueNums = 4;

    /**
     * 发送消息超时时间
     */
    private int sendMsgTimeout = 3000;

    /**
     * 压缩消息体阈值，即大于4k的消息体将默认压缩。
     */
    private int compressMsgBodyOverHowmuch = 1024 * 4;

    /**
     * 发送同步消息失败最大重试次数。
     * 这可能会导致消息重复，这取决于应用程序开发人员要解决的问题。
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * 发送异步消息失败最大重试次数。
     * 这可能会导致消息重复，这取决于应用程序开发人员要解决的问题。
     */
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * 指示是否重试另一个代理在内部发送失败。
     */
    private boolean retryAnotherBrokerWhenNotStoreOK = false;

    /**
     * 允许的最大消息大小(以字节为单位)。
     */
    private int maxMessageSize = 1024 * 1024 * 4; // 4M

    /**
     * 消息轨迹消息调度员接口
     */
    private TraceDispatcher traceDispatcher = null;

    /**
     * DefaultMQProducer默认构造函数。
     */
    public DefaultMQProducer() {
        this(null, MixAll.DEFAULT_PRODUCER_GROUP, null);
    }

    /**
     * DefaultMQProducer构造函数
     *
     * @param rpcHook RPC钩子
     */
    public DefaultMQProducer(RPCHook rpcHook) {
        this(null, MixAll.DEFAULT_PRODUCER_GROUP, rpcHook);
    }

    /**
     * DefaultMQProducer构造函数
     *
     * @param producerGroup 生产者分组
     */
    public DefaultMQProducer(final String producerGroup) {
        this(null, producerGroup, null);
    }

    /**
     * DefaultMQProducer构造函数
     *
     * @param producerGroup        消费分组
     * @param rpcHook              RPC钩子
     * @param enableMsgTrace       是否开启消息轨迹
     * @param customizedTraceTopic 消息轨迹指定的topic
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace,
                             final String customizedTraceTopic) {
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
        //是否开启消息轨迹
        if (enableMsgTrace) {
            try {
                //创建 AsyncTraceDispatcher（异步消息轨迹调度员）
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE, customizedTraceTopic, rpcHook);
                dispatcher.setHostProducer(this.defaultMQProducerImpl);
                traceDispatcher = dispatcher;
                //将 AsyncTraceDispatcher 注册到 DefaultMQProducerImpl
                this.defaultMQProducerImpl.registerSendMessageHook(
                        new SendMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    /**
     * DefaultMQProducer构造函数
     *
     * @param namespace     命名空间
     * @param producerGroup 生成者分组
     */
    public DefaultMQProducer(final String namespace, final String producerGroup) {
        this(namespace, producerGroup, null);
    }

    /**
     * DefaultMQProducer构造函数
     *
     * @param producerGroup 生成者分组
     * @param rpcHook       RPC钩子
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook) {
        this(null, producerGroup, rpcHook);
    }

    /**
     * DefaultMQProducer构造函数
     *
     * @param namespace     命名空间
     * @param producerGroup 生成者分组
     * @param rpcHook       RPC钩子
     */
    public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook) {
        this.namespace = namespace;
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
    }

    /**
     * DefaultMQProducer构造函数
     *
     * @param producerGroup  生成者分组
     * @param enableMsgTrace 是否使用消息轨迹
     */
    public DefaultMQProducer(final String producerGroup, boolean enableMsgTrace) {
        this(null, producerGroup, null, enableMsgTrace, null);
    }

    /**
     * DefaultMQProducer构造函数
     *
     * @param producerGroup        生成者分组
     * @param enableMsgTrace       是否使用消息轨迹
     * @param customizedTraceTopic 消息轨迹存储topic名称
     */
    public DefaultMQProducer(final String producerGroup, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(null, producerGroup, null, enableMsgTrace, customizedTraceTopic);
    }

    /**
     * DefaultMQProducer构造函数
     *
     * @param namespace            命名空间
     * @param producerGroup        生成者分组
     * @param rpcHook              RPC钩子
     * @param enableMsgTrace       是否使用消息轨迹
     * @param customizedTraceTopic 消息轨迹存储topic名称
     */
    public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook,
                             boolean enableMsgTrace, final String customizedTraceTopic) {
        this.namespace = namespace;
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
        //是否开启消息轨迹
        if (enableMsgTrace) {
            try {
                //创建 AsyncTraceDispatcher（异步消息轨迹调度员）
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE, customizedTraceTopic, rpcHook);
                dispatcher.setHostProducer(this.getDefaultMQProducerImpl());
                traceDispatcher = dispatcher;
                //将 AsyncTraceDispatcher 注册到 DefaultMQProducerImpl
                this.getDefaultMQProducerImpl().registerSendMessageHook(
                        new SendMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    /**
     * 启动DefaultMQProducer实例
     * <p>
     * 执行许多内部初始化过程来准备此实例，因此，在发送或查询消息之前必须调用此方法。
     *
     * @throws MQClientException 如果有任何意外错误。
     */
    @Override
    public void start() throws MQClientException {
        this.setProducerGroup(withNamespace(this.producerGroup));
        //启动DefaultMQProducerImpl
        this.defaultMQProducerImpl.start();
        //启动traceDispatcher
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    /**
     * 关闭DefaultMQProducer实例并且释放相关资源
     */
    @Override
    public void shutdown() {
        //关闭DefaultMQProducerImpl
        this.defaultMQProducerImpl.shutdown();
        //关闭traceDispatcher
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
    }

    /**
     * 获取存储指定topic的消息队列
     *
     * @param topic Topic to fetch.
     * @return List of message queues readily to send messages to
     * @throws MQClientException if there is any client error.
     */
    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        return this.defaultMQProducerImpl.fetchPublishMessageQueues(withNamespace(topic));
    }

    /*********************  同步消息接口开始   *********************/
    /**
     * 发送同步消息。
     * 同步消息需要等待客户端响应。
     * <p>
     * 发送同步消息内部具有内部重试机制，重试的次数参考{@link #retryTimesWhenSendFailed}次，因而会导致发送重复消息给broker
     *
     * @param msg 发送的消息
     * @return {@link SendResult}   发送消息结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws MQBrokerException    broker异步
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public SendResult send(
            Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        Validators.checkMessage(msg, this);
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg);
    }


    /**
     * 相对于{@link #send(Message)}设置超时时间，
     *
     * @param msg     发送的消息
     * @param timeout 同步消息超时时间
     * @return {@link SendResult}   发送消息结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws MQBrokerException    broker异步
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public SendResult send(Message msg,
                           long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    /**
     * 相对于{@link #send(Message, long)}请求采用了异步机制
     *
     * @param msg     发送的消息
     * @param timeout 超时时间
     * @return 回复消息
     * @throws MQClientException       客户端异常
     * @throws RemotingException       远端调用异常
     * @throws MQBrokerException       broker异步
     * @throws InterruptedException    线程中断异常
     * @throws RequestTimeoutException 请求超时
     */
    @Override
    public Message request(final Message msg, final long timeout) throws RequestTimeoutException, MQClientException,
            RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, timeout);
    }

    /**
     * 发送同步消息给指定消息队列
     * 同步消息需要等待客户端响应
     * <p>
     * 发送同步消息内部具有内部重试机制，重试的次数参考{@link #retryTimesWhenSendFailed}次，因而会导致发送重复消息给broker
     *
     * @param msg 发送的消息
     * @param mq  消息队列
     * @return {@link SendResult}   发送消息结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws MQBrokerException    broker异步
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq));
    }

    /**
     * 相对于{@link #send(Message, MessageQueue)}设置超时时间，
     *
     * @param msg     发送的消息
     * @param mq      消息队列
     * @param timeout 超时时间
     * @return {@link SendResult}   发送消息结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws MQBrokerException    broker异步
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), timeout);
    }

    /**
     * 相对于{@link #send(Message, MessageQueue, long)}请求采用了异步机制
     *
     * @param msg     请求的消息
     * @param mq      消息队列
     * @param timeout 超时时间
     * @return 回复消息
     * @throws MQClientException       客户端异常
     * @throws RemotingException       远端调用异常
     * @throws MQBrokerException       broker异步
     * @throws InterruptedException    线程中断异常
     * @throws RequestTimeoutException 请求超时
     */
    @Override
    public Message request(final Message msg, final MessageQueue mq, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, mq, timeout);
    }

    /*********************  异步消息接口开始   *********************/

    /**
     * 发送异步消息。异步消息不需要等待客户端响应。
     * 异步消息通过{@link SendCallback}处理broker回调，通知消息是否成功发送
     * <p>
     * 发送异步消息内部具有内部重试机制，重试的次数参考{@link #retryTimesWhenSendAsyncFailed}次，因而会导致发送重复消息给broker
     *
     * @param msg          发送的消息
     * @param sendCallback 处理broker回调，通知消息是否成功发送
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public void send(Message msg,
                     SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, sendCallback);
    }

    /**
     * 相对于{@link #send(Message, SendCallback)}设置超时时间，
     *
     * @param msg          发送的消息
     * @param sendCallback 处理broker回调，通知消息是否成功发送
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
    }

    /**
     * 相对于{@link #send(Message, SendCallback, long)}请求采用了异步机制
     *
     * @param msg     发送的消息
     * @param timeout 超时时间
     * @return 回复消息
     * @throws MQClientException       客户端异常
     * @throws RemotingException       远端调用异常
     * @throws MQBrokerException       broker异步
     * @throws InterruptedException    线程中断异常
     * @throws RequestTimeoutException 请求超时
     */
    @Override
    public void request(final Message msg, final RequestCallback requestCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, requestCallback, timeout);
    }


    /**
     * 发送异步消息给指定消息队列。
     * 异步消息不需要等待客户端响应。异步消息通过{@link SendCallback}处理broker回调，通知消息是否成功发送
     * <p>
     * 发送异步消息内部具有内部重试机制，重试的次数参考{@link #retryTimesWhenSendAsyncFailed}次，因而会导致发送重复消息给broker
     *
     * @param msg          发送的消息
     * @param mq           消息队列
     * @param sendCallback 处理broker回调，通知消息是否成功发送
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback);
    }

    /**
     * 相对于{@link #send(Message, MessageQueue, SendCallback)}设置超时时间，
     *
     * @param msg          发送的消息
     * @param mq           消息队列
     * @param sendCallback 处理broker回调，通知消息是否成功发送
     * @param timeout      超时时间
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback, timeout);
    }

    /**
     * 相对于{@link #send(Message, MessageQueue, SendCallback, long)} 请求采用了异步机制
     *
     * @param msg             发送的消息
     * @param requestCallback 处理broker回调，通知消息是否成功发送
     * @param timeout         超时时间
     * @return 回复消息
     * @throws MQClientException       客户端异常
     * @throws RemotingException       远端调用异常
     * @throws MQBrokerException       broker异步
     * @throws InterruptedException    线程中断异常
     * @throws RequestTimeoutException 请求超时
     */
    @Override
    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, mq, requestCallback, timeout);
    }


    /*********************  单向消息接口开始   *********************/

    /**
     * 发送单向消息
     * 单向消息不需要broker响应是一种不可靠的消息
     *
     * @param msg 发送的消息
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg);
    }

    /**
     * 发送单向消息给指定消息队列
     * 单向消息不需要broker响应是一种不可靠的消息
     *
     * @param msg 发送的消息
     * @param mq  消息队列
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public void sendOneway(Message msg,
                           MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg, queueWithNamespace(mq));
    }


    /*********************  顺序消息接口开始   *********************/

    /**
     * 发送同步消息
     * 消息按照MessageQueueSelector消息队列选择器规则分发到指定消息队列
     * 同一个订单消息会发送到同一个消息队列保证顺序消费
     *
     * @param msg      发送的消息
     * @param selector 消息队列选择器
     * @param arg      消息队列选择器参数
     * @return {@link SendResult}   发送消息结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws MQBrokerException    broker异步
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, selector, arg);
    }

    /**
     * 相对于{@link #send(Message, MessageQueueSelector, Object)}设置超时时间，
     *
     * @param msg      发送的消息
     * @param selector 消息队列选择器
     * @param arg      消息队列选择器参数
     * @param timeout  超时时间
     * @return {@link SendResult}   发送消息结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws MQBrokerException    broker异步
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
    }

    /**
     * 相对于{@link #send(Message, MessageQueueSelector, Object, long)}请求采用了异步机制
     *
     * @param msg      发送的消息
     * @param selector 消息队列选择器
     * @param arg      消息队列选择器参数
     * @param timeout  超时时间
     * @return reply message
     * @throws MQClientException       客户端异常
     * @throws RemotingException       远端调用异常
     * @throws MQBrokerException       broker异步
     * @throws InterruptedException    线程中断异常
     * @throws RequestTimeoutException 请求超时
     */
    @Override
    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
                           final long timeout) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException, RequestTimeoutException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, selector, arg, timeout);
    }

    /**
     * 发送异步消息
     * 消息按照MessageQueueSelector消息队列选择器规则分发到指定消息队列并指定超时时间
     * 异步消息通过{@link SendCallback}处理broker回调，通知消息是否成功发送
     * 同一个订单消息会发送到同一个消息队列保证顺序消费
     *
     * @param msg          发送的消息
     * @param selector     消息队列选择器
     * @param arg          消息队列选择器参数
     * @param sendCallback 处理broker回调，通知消息是否成功发送
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
    }

    /**
     * 相对于{@link #send(Message, MessageQueueSelector, Object, SendCallback)}设置超时时间，
     *
     * @param msg          发送的消息
     * @param selector     消息队列选择器
     * @param arg          消息队列选择器参数
     * @param sendCallback 处理broker回调，通知消息是否成功发送
     * @param timeout      超时时间
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
    }


    /**
     * 相对于{@link #send(Message, MessageQueueSelector, Object, SendCallback, long)}请求采用了异步机制
     *
     * @param msg      发送的消息
     * @param selector 消息队列选择器
     * @param arg      消息队列选择器参数
     * @param timeout  超时时间
     * @return 回复消息
     * @throws MQClientException       客户端异常
     * @throws RemotingException       远端调用异常
     * @throws MQBrokerException       broker异步
     * @throws InterruptedException    线程中断异常
     * @throws RequestTimeoutException 请求超时
     */
    @Override
    public void request(final Message msg, final MessageQueueSelector selector, final Object arg,
                        final RequestCallback requestCallback, final long timeout) throws MQClientException, RemotingException,
            InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, selector, arg, requestCallback, timeout);
    }

    /**
     * 发送单向消息给指定消息队列
     * 消息按照MessageQueueSelector消息队列选择器规则分发到指定消息队列并指定超时时间
     * 单向消息不需要broker响应是一种不可靠的消息
     *
     * @param msg      发送的消息
     * @param selector 消息队列选择器
     * @param arg      消息队列选择器参数
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
    }


    /*********************  事务消息接口开始   *********************/

    /**
     * 发送事务消息（已经取消使用TransactionMQProducer）
     *
     * @param msg          事务消息
     * @param tranExecuter 本地交易执行者。
     * @param arg          本地交易执行者参数
     * @return 事务结果
     * @throws MQClientException 客户端异常
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter,
                                                          final Object arg)
            throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }

    /**
     * 发送事务消息（已经取消使用TransactionMQProducer）
     *
     * @param msg 事务消息
     * @param arg 本地交易执行者参数
     * @return 事务结果
     * @throws MQClientException
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg,
                                                          Object arg) throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
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
        this.defaultMQProducerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
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
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQProducerImpl.searchOffset(queueWithNamespace(mq), timestamp);
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
        return this.defaultMQProducerImpl.maxOffset(queueWithNamespace(mq));
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
        return this.defaultMQProducerImpl.minOffset(queueWithNamespace(mq));
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
        return this.defaultMQProducerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
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
        return this.defaultMQProducerImpl.viewMessage(offsetMsgId);
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
        return this.defaultMQProducerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
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
            MessageId oldMsgId = MessageDecoder.decodeMessageId(msgId);
            return this.viewMessage(msgId);
        } catch (Exception e) {
        }
        return this.defaultMQProducerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
    }

    /*********************  批量消息接口  *********************/

    /**
     * 批量发送消息
     *
     * @param msgs 消息集合
     * @return 发送结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远程调用异常
     * @throws MQBrokerException    Broker异常
     * @throws InterruptedException 中断异常
     */
    @Override
    public SendResult send(
            Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs));
    }

    /**
     * 相对于{@link #send(Collection)}设置超时时间，
     *
     * @param msgs    消息集合
     * @param timeout 超时时间
     * @return
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远程调用异常
     * @throws InterruptedException 中断异常
     */
    @Override
    public SendResult send(Collection<Message> msgs,
                           long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), timeout);
    }

    /**
     * 批量发送消息到指定消息队列
     *
     * @param msgs         消息集合
     * @param messageQueue 消息队列
     * @return 发送结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远程调用异常
     * @throws MQBrokerException    Broker异常
     * @throws InterruptedException 中断异常
     */
    @Override
    public SendResult send(Collection<Message> msgs,
                           MessageQueue messageQueue) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue);
    }

    /**
     * 相对于{@link #send(Collection, MessageQueue)}设置超时时间，
     *
     * @param msgs         消息集合
     * @param messageQueue 消息队列
     * @param timeout      超时时间
     * @return
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远程调用异常
     * @throws MQBrokerException    Broker异常
     * @throws InterruptedException 中断异常
     */
    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue messageQueue,
                           long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue, timeout);
    }

    /**
     * Sets an Executor to be used for executing callback methods. If the Executor is not set, {@link
     * NettyRemotingClient#publicExecutor} will be used.
     *
     * @param callbackExecutor the instance of Executor
     */
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.defaultMQProducerImpl.setCallbackExecutor(callbackExecutor);
    }

    /**
     * Sets an Executor to be used for executing asynchronous send. If the Executor is not set, {@link
     * DefaultMQProducerImpl#defaultAsyncSenderExecutor} will be used.
     *
     * @param asyncSenderExecutor the instance of Executor
     */
    public void setAsyncSenderExecutor(final ExecutorService asyncSenderExecutor) {
        this.defaultMQProducerImpl.setAsyncSenderExecutor(asyncSenderExecutor);
    }

    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        MessageBatch msgBatch;
        try {
            msgBatch = MessageBatch.generateFromList(msgs);
            for (Message message : msgBatch) {
                Validators.checkMessage(message, this);
                MessageClientIDSetter.setUniqID(message);
                message.setTopic(withNamespace(message.getTopic()));
            }
            msgBatch.setBody(msgBatch.encode());
        } catch (Exception e) {
            throw new MQClientException("Failed to initiate the MessageBatch", e);
        }
        msgBatch.setTopic(withNamespace(msgBatch.getTopic()));
        return msgBatch;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }

    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }

    @Deprecated
    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return defaultMQProducerImpl;
    }

    public boolean isRetryAnotherBrokerWhenNotStoreOK() {
        return retryAnotherBrokerWhenNotStoreOK;
    }

    public void setRetryAnotherBrokerWhenNotStoreOK(boolean retryAnotherBrokerWhenNotStoreOK) {
        this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public boolean isSendMessageWithVIPChannel() {
        return isVipChannelEnabled();
    }

    public void setSendMessageWithVIPChannel(final boolean sendMessageWithVIPChannel) {
        this.setVipChannelEnabled(sendMessageWithVIPChannel);
    }

    public long[] getNotAvailableDuration() {
        return this.defaultMQProducerImpl.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.defaultMQProducerImpl.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.defaultMQProducerImpl.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.defaultMQProducerImpl.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.defaultMQProducerImpl.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.defaultMQProducerImpl.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(final int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    public TraceDispatcher getTraceDispatcher() {
        return traceDispatcher;
    }

}
