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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 拉取消息API网关，内嵌MQ客户端实例对象
 * <p>
 * 提供如下功能：
 * 1 拉取消息
 * 1.1 拉取消息选择borker实例类型（master实例，还是某个salve实例）
 * 1.1.1可以客户端指定，
 * 1.1.2可以按照broker拉取结果自动变更
 * 2 对拉取消息进行处理
 * 2.1 更新指定消息队列从哪个broker实例类型拉取消息（来源于拉取结果）
 * 2.2 解析消息，并根据订阅配置信息按照tag过滤消息
 * 2.3 使用消息过滤钩子对拉取消息进行过滤
 * 2.4 设置消息特殊属性
 */
public class PullAPIWrapper {

    /**
     * 内部日志
     */
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * MQ客户端实例对象
     */
    private final MQClientInstance mQClientFactory;

    /**
     * 消费分组
     */
    private final String consumerGroup;

    /**
     * 是否单元化
     */
    private final boolean unitMode;

    /**
     * 消息队列以及消息队列拉取消息对应broker实例类型（是从主还是指定从实例拉取）
     */
    private ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>(32);

    /**
     * 指定拉取消息只从 defaultBrokerId 实例类型实例拉取
     */
    private volatile boolean connectBrokerByUser = false;

    /**
     * 默认拉取消息broker实例类型
     */
    private volatile long defaultBrokerId = MixAll.MASTER_ID;

    /**
     * 随机数
     */
    private Random random = new Random(System.currentTimeMillis());

    /**
     * 过滤消息钩子列表
     */
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();


    /**
     * PullAPIWrapper 构造函数
     *
     * @param mQClientFactory MQ客户端实例对象
     * @param consumerGroup   消费分组
     * @param unitMode        是否单元化
     */
    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    /**
     * 调用路径
     * 拉取消息 PullCallback 回调中调用
     * <p>
     * <p>
     * 处理拉取结果
     * 1 更新指定消息队列从哪个broker实例类型拉取消息（来源于拉取结果）
     * 2 解析消息，并根据订阅配置信息按照tag过滤消息
     * 3 使用消息过滤钩子对拉取消息进行过滤
     * 4 设置消息特殊属性
     *
     * @param mq               消息队列
     * @param pullResult       拉取消息结果
     * @param subscriptionData 订阅配置信息
     * @return
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
                                        final SubscriptionData subscriptionData) {
        //获取原始拉拉取消息结果
        PullResultExt pullResultExt = (PullResultExt) pullResult;
        //更新指定消息队列从哪个broker实例类型拉取消息（来源于拉取结果）
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());

        //判断拉取状态是FOUND
        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            //将拉取消息放入字节缓冲区中
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            //从字节缓冲区中解码获取消息列表
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);
            //记录拉取消息列表
            List<MessageExt> msgListFilterAgain = msgList;

            //查询
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }
            //通过注册消息过滤钩子对拉取消息进行过滤
            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }
            //遍历拉取消息
            for (MessageExt msg : msgListFilterAgain) {
                //获取事务消息标识
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                //给消息设置事务id
                if (Boolean.parseBoolean(traFlag)) {
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                //给消息增加拉取消息消息最小逻辑偏移量
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                        Long.toString(pullResult.getMinOffset()));
                //给消息增加拉取消息消息最大逻辑偏移量
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                        Long.toString(pullResult.getMaxOffset()));
                //给消息设置broker节点
                msg.setBrokerName(mq.getBrokerName());
            }

            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        pullResultExt.setMessageBinary(null);

        return pullResult;
    }


    /**
     * 拉取消息核心方法
     * <p>
     * 调用路径：拉取入口 PullMessageService.run()
     * PullAPIWrapper.pullKernelImpl(MessageQueue, String, String, long, long, int, int, ...)  (org.apache.rocketmq.client.impl.consumer)
     * DefaultMQPushConsumerImpl.pullMessage(PullRequest)  (org.apache.rocketmq.client.impl.consumer)
     * PullMessageService.pullMessage(PullRequest)  (org.apache.rocketmq.client.impl.consumer)
     * PullMessageService.run()  (org.apache.rocketmq.client.impl.consumer)
     *
     * @param mq                         消息队列
     * @param subExpression              消息过滤过滤表达式
     * @param expressionType             消息过滤表达类型
     * @param subVersion                 订阅版本号
     * @param offset                     拉取队列开始位置（源至于broker消费进度）
     * @param maxNums                    拉取消息数量
     * @param sysFlag                    拉取请求系统标识
     * @param commitOffset               已提交消费进度(本地缓存消费进度,可能未同步到broker)
     * @param brokerSuspendMaxTimeMillis broker挂起请求最大时间
     * @param timeoutMillis              请求broker超时时长
     * @param communicationMode          发送消息模式
     * @param pullCallback               拉取回调
     * @return 拉取消息结果
     * @throws MQClientException    MQ客户端异常
     * @throws RemotingException    远程通信异常
     * @throws MQBrokerException    broker异常
     * @throws InterruptedException 中断异常
     */
    public PullResult pullKernelImpl(
            final MessageQueue mq,
            final String subExpression,
            final String expressionType,
            final long subVersion,
            final long offset,
            final int maxNums,
            final int sysFlag,
            final long commitOffset,
            final long brokerSuspendMaxTimeMillis,
            final long timeoutMillis,
            final CommunicationMode communicationMode,
            final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        //1 计算从哪个broker节点拉取消息（主..从从）
        //2 在MQClientINSTANCE 客户端实例中 指定broker节点，指定broker实例类型的实例查询结果
        FindBrokerResult findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                        this.recalculatePullFromWhichNode(mq), false);

        //判断broker实例查询结果是否存在
        if (null == findBrokerResult) {
            //1 从Namerser获取topic对应的路由信息
            //2 更新到 MQClientInstance客户端实例 属性中
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());

            //在MQClientINSTANCE 客户端实例中 指定broker节点，指定broker实例类型的实例查询结果
            findBrokerResult =
                    this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                            this.recalculatePullFromWhichNode(mq), false);
        }

        //判断broker实例查询结果是否存在
        if (findBrokerResult != null) {
            {
                //检查消息过滤表达类型，和broker版本号
                if (!ExpressionType.isTagType(expressionType)
                        && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                            + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }
            //设置系统标识
            int sysFlagInner = sysFlag;

            //设置sysFlagInner
            if (findBrokerResult.isSlave()) {
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            //创建拉取消息请求头部
            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(this.consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setQueueOffset(offset);
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setSysFlag(sysFlagInner);
            requestHeader.setCommitOffset(commitOffset);
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            requestHeader.setSubscription(subExpression);
            requestHeader.setSubVersion(subVersion);
            requestHeader.setExpressionType(expressionType);

            //获取broker实例地址
            String brokerAddr = findBrokerResult.getBrokerAddr();

            //是否通过FilterServer拉取消息
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                //计算指定broker实例FilterServer地址
                brokerAddr = computPullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            //通过MQ客户端 拉取消息
            PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                    brokerAddr,
                    requestHeader,
                    timeoutMillis,
                    communicationMode,
                    pullCallback);

            //返回拉取结果
            return pullResult;
        }
        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }


    /**
     * 计算指定broker实例FilterServer地址
     *
     * @param topic      消息topic
     * @param brokerAddr broker地址
     * @return
     * @throws MQClientException
     */
    private String computPullFromWhichFilterServer(final String topic, final String brokerAddr)
            throws MQClientException {
        //从MQClientINSTANCE 客户端实例获取所有topic和topic路由信息
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            //获取指定topic路由信息
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            //获取指定borker实例 FilterServer地址列表
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);
            //从FilterServer地址列表随机获取一台FilterServer地址
            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }
        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
                + topic, null);
    }

    /**
     * 获取随机数
     *
     * @return
     */
    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }


    /*************************** filterMessageHookList 属性相关方法   ***************************/

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    /**
     * 回调消息过滤钩子
     *
     * @param context 过滤消息上下文
     */
    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    /**
     * 注册FilterMessageHook
     *
     * @param filterMessageHookList 过滤消息钩子
     */
    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    /*************************** 其他属性相关方法   ***************************/

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }


    /**
     * 获取指定消息队列从哪个broker实例类型拉取消息（主..从从）
     *
     * @param mq 消息队列
     * @return broker实例类型
     */
    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        //判断拉是否只从 defaultBrokerId 实例类型拉取
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }
        //从pullFromWhichNodeTable配置中获取指定消息队列拉取消息的broker实例类型
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }
        //从master类型broker实例拉取
        return MixAll.MASTER_ID;
    }

    /**
     * 更新指定消息队列从哪个broker实例类型拉取消息
     *
     * @param mq
     * @param brokerId
     */
    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }
}
