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

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

public interface MQProducer extends MQAdmin {

    /**
     * 启动
     *
     * @throws MQClientException 如果有任何意外错误。
     */
    void start() throws MQClientException;

    /**
     * 关闭
     */
    void shutdown();

    /**
     * 获取存储指定topic的消息队列
     *
     * @param topic Topic to fetch.
     * @return List of message queues readily to send messages to
     * @throws MQClientException if there is any client error.
     */
    List<MessageQueue> fetchPublishMessageQueues(final String topic) throws MQClientException;

    /**
     * 发送同步消息。
     * 同步消息需要等待客户端响应。
     * <p>
     *
     * @param msg 发送的消息
     * @return {@link SendResult}   发送消息结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws MQBrokerException    broker异步
     * @throws InterruptedException 线程中断异常
     */
    SendResult send(final Message msg) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

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
    SendResult send(final Message msg, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    /**
     * 发送异步消息。异步消息不需要等待客户端响应。
     * 异步消息通过{@link SendCallback}处理broker回调，通知消息是否成功发送
     * <p>
     *
     * @param msg          发送的消息
     * @param sendCallback 处理broker回调，通知消息是否成功发送
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    void send(final Message msg, final SendCallback sendCallback) throws MQClientException,
            RemotingException, InterruptedException;

    /**
     * 相对于{@link #send(Message, SendCallback)}设置超时时间，
     *
     * @param msg          发送的消息
     * @param sendCallback 处理broker回调，通知消息是否成功发送
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    void send(final Message msg, final SendCallback sendCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException;

    /**
     * 发送单向消息
     * 单向消息不需要broker响应是一种不可靠的消息
     *
     * @param msg 发送的消息
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    void sendOneway(final Message msg) throws MQClientException, RemotingException,
            InterruptedException;

    /**
     * 发送同步消息给指定消息队列
     * 同步消息需要等待客户端响应
     * <p>
     *
     * @param msg 发送的消息
     * @param mq  消息队列
     * @return {@link SendResult}   发送消息结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws MQBrokerException    broker异步
     * @throws InterruptedException 线程中断异常
     */
    SendResult send(final Message msg, final MessageQueue mq) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

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
    SendResult send(final Message msg, final MessageQueue mq, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 发送异步消息给指定消息队列。
     * 异步消息不需要等待客户端响应。异步消息通过{@link SendCallback}处理broker回调，通知消息是否成功发送
     * <p>
     *
     * @param msg          发送的消息
     * @param mq           消息队列
     * @param sendCallback 处理broker回调，通知消息是否成功发送
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端调用异常
     * @throws InterruptedException 线程中断异常
     */
    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException;

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
    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException;

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
    void sendOneway(final Message msg, final MessageQueue mq) throws MQClientException,
            RemotingException, InterruptedException;

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
    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

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
    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg,
                    final long timeout) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

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
    void send(final Message msg, final MessageQueueSelector selector, final Object arg,
              final SendCallback sendCallback) throws MQClientException, RemotingException,
            InterruptedException;

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
    void send(final Message msg, final MessageQueueSelector selector, final Object arg,
              final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException,
            InterruptedException;

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
    void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, InterruptedException;

    /**
     * 发送事务消息（已经取消使用TransactionMQProducer）
     *
     * @param msg          事务消息
     * @param tranExecuter 本地交易执行者。
     * @param arg          本地交易执行者参数
     * @return 事务结果
     * @throws MQClientException 客户端异常
     */
    TransactionSendResult sendMessageInTransaction(final Message msg,
                                                   final LocalTransactionExecuter tranExecuter, final Object arg) throws MQClientException;

    /**
     * 发送事务消息（已经取消使用TransactionMQProducer）
     *
     * @param msg 事务消息
     * @param arg 本地交易执行者参数
     * @return 事务结果
     * @throws MQClientException
     */
    TransactionSendResult sendMessageInTransaction(final Message msg,
                                                   final Object arg) throws MQClientException;

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
    SendResult send(final Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

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
    SendResult send(final Collection<Message> msgs, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    /**
     * 批量发送消息到指定消息队列
     *
     * @param msgs 消息集合
     * @param mq   消息队列
     * @return 发送结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远程调用异常
     * @throws MQBrokerException    Broker异常
     * @throws InterruptedException 中断异常
     */
    SendResult send(final Collection<Message> msgs, final MessageQueue mq) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    /**
     * 相对于{@link #send(Collection, MessageQueue)}设置超时时间，
     *
     * @param msgs    消息集合
     * @param mq      消息队列
     * @param timeout 超时时间
     * @return
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远程调用异常
     * @throws MQBrokerException    Broker异常
     * @throws InterruptedException 中断异常
     */
    SendResult send(final Collection<Message> msgs, final MessageQueue mq, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

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
    Message request(final Message msg, final long timeout) throws RequestTimeoutException, MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

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
    void request(final Message msg, final RequestCallback requestCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException, MQBrokerException;

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
    Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
                    final long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

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
    void request(final Message msg, final MessageQueueSelector selector, final Object arg,
                 final RequestCallback requestCallback,
                 final long timeout) throws MQClientException, RemotingException,
            InterruptedException, MQBrokerException;

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
    Message request(final Message msg, final MessageQueue mq, final long timeout)
            throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException;

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
    void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
}
