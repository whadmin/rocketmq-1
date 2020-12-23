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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.pagecache.OneMessageTransfer;
import org.apache.rocketmq.broker.pagecache.QueryMessageTransfer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * 查询消息处理器
 * <p>
 * 1 根据条件查询消息（条件包括指定topic+指定消息key+时间范围）的消息
 *    1 获取DefaultMessageStore对象,通过DefaultMessageStore根据条件查询消息（条件包括指定topic+指定消息key+时间范围）的消息
 *    2 DefaultMessageStore内部通过indexService（索引服务）根据条件查询消息（条件包括指定topic+指定消息key+时间范围）的消息物理偏离量
 *    3 DefaultMessageStore内部通过commitLog 根据消息物理偏离量 查询消息
 *
 * 2 根据消息Id（消息Id可以解析到消息物理偏移）查询消息
 *    1 获取DefaultMessageStore对象,通过DefaultMessageStore根据消息物理偏移查询消息
 *    2 DefaultMessageStore内部通过commitLog 根据消息物理偏离量 查询消息
 *
 */
public class QueryMessageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * Broker控制器
     */
    private final BrokerController brokerController;

    /**
     * 创建QueryMessageProcessor
     */
    public QueryMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 处理查询消息请求
     *
     * @param ctx     通信处理器上下文
     * @param request RPC通信请求命令
     * @return RPC通信响应命令
     * @throws RemotingCommandException
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        //判断请求命令编码
        switch (request.getCode()) {
            //判断请求命令是否是根据条件查询消息（条件包括消息key,时间范围）
            case RequestCode.QUERY_MESSAGE:
                return this.queryMessage(ctx, request);
            //判断请求命令是否是根据消息Id查询消息
            case RequestCode.VIEW_MESSAGE_BY_ID:
                return this.viewMessageById(ctx, request);
            default:
                break;
        }

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 根据条件查询消息（条件包括指定topic+指定消息key+时间范围）的消息
     *
     * @param ctx     通信处理器上下文
     * @param request RPC通信请求命令
     * @return RPC通信响应命令
     * @throws RemotingCommandException
     */
    public RemotingCommand queryMessage(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        //创建响应命令
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(QueryMessageResponseHeader.class);
        //获取响应命令中响应头部
        final QueryMessageResponseHeader responseHeader =
                (QueryMessageResponseHeader) response.readCustomHeader();

        //请求命令进行解码获取请求头部
        final QueryMessageRequestHeader requestHeader =
                (QueryMessageRequestHeader) request
                        .decodeCommandCustomHeader(QueryMessageRequestHeader.class);

        //设置响应对应请求Id
        response.setOpaque(request.getOpaque());
        //获取请求查询消息key是否唯一，如果唯一设置消息返回最大数量来至于消息存储的配置
        String isUniqueKey = request.getExtFields().get(MixAll.UNIQUE_MSG_QUERY_FLAG);
        if (isUniqueKey != null && isUniqueKey.equals("true")) {
            requestHeader.setMaxNum(this.brokerController.getMessageStoreConfig().getDefaultQueryMaxNum());
        }

        //1 获取DefaultMessageStore对象,通过DefaultMessageStore根据条件查询消息（条件包括指定topic+指定消息key+时间范围）的消息
        //2 DefaultMessageStore内部通过indexService（索引服务）根据条件查询消息（条件包括指定topic+指定消息key+时间范围）的消息物理偏离量
        //3 DefaultMessageStore内部通过commitLog 根据消息物理偏离量 查询消息
        final QueryMessageResult queryMessageResult =
                this.brokerController.getMessageStore().queryMessage(requestHeader.getTopic(),
                        requestHeader.getKey(), requestHeader.getMaxNum(), requestHeader.getBeginTimestamp(),
                        requestHeader.getEndTimestamp());
        assert queryMessageResult != null;

        responseHeader.setIndexLastUpdatePhyoffset(queryMessageResult.getIndexLastUpdatePhyoffset());
        responseHeader.setIndexLastUpdateTimestamp(queryMessageResult.getIndexLastUpdateTimestamp());

        //判断查询消息结果中是否存在消息数据
        if (queryMessageResult.getBufferTotalSize() > 0) {
            // 查询消息结果中存在消息数据  **/

            //设置响应编码 ResponseCode.SUCCESS
            response.setCode(ResponseCode.SUCCESS);
            //设置响应配备注
            response.setRemark(null);

            try {
                //构造FileRegion
                FileRegion fileRegion =
                        new QueryMessageTransfer(response.encodeHeader(queryMessageResult
                                .getBufferTotalSize()), queryMessageResult);
                //写入FileRegion，通知客户端
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        queryMessageResult.release();
                        if (!future.isSuccess()) {
                            log.error("transfer query message by page cache failed, ", future.cause());
                        }
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                //释放queryMessageResult中资源（缓冲区）
                queryMessageResult.release();
            }

            return null;
        }
        // 查询消息结果中不否存在消息数据  **/

        //设置响应编码 ResponseCode.QUERY_NOT_FOUND 没有找到消息
        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        //设置响应配备注
        response.setRemark("can not find message, maybe time range not correct");
        return response;
    }

    /**
     * 根据消息Id（消息Id可以解析到消息物理偏移）查询消息
     *
     * @param ctx     通信处理器上下文
     * @param request RPC通信请求命令
     * @return RPC通信请求命令
     * @throws RemotingCommandException
     */
    public RemotingCommand viewMessageById(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        //创建响应命令
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        //请求命令进行解码获取请求头部
        final ViewMessageRequestHeader requestHeader =
                (ViewMessageRequestHeader) request.decodeCommandCustomHeader(ViewMessageRequestHeader.class);

        //设置响应对应请求Id
        response.setOpaque(request.getOpaque());

        //1 获取DefaultMessageStore对象,通过DefaultMessageStore根据消息物理偏移查询消息
        //2 DefaultMessageStore内部通过commitLog 根据消息物理偏离量 查询消息
        final SelectMappedBufferResult selectMappedBufferResult =
                this.brokerController.getMessageStore().selectOneMessageByOffset(requestHeader.getOffset());
        if (selectMappedBufferResult != null) {
            // 查询消息结果中存在消息数据  **/

            //设置响应编码 ResponseCode.SUCCESS
            response.setCode(ResponseCode.SUCCESS);
            //设置响应配备注
            response.setRemark(null);

            try {
                //构造FileRegion
                FileRegion fileRegion =
                        new OneMessageTransfer(response.encodeHeader(selectMappedBufferResult.getSize()),
                                selectMappedBufferResult);
                //写入FileRegion，通知客户端
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        selectMappedBufferResult.release();
                        if (!future.isSuccess()) {
                            log.error("Transfer one message from page cache failed, ", future.cause());
                        }
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                //释放queryMessageResult中资源（缓冲区）
                selectMappedBufferResult.release();
            }

            return null;
        } else {
            // 查询消息结果中不否存在消息数据  **/

            //设置响应编码 ResponseCode.SYSTEM_ERROR 系统错误
            response.setCode(ResponseCode.SYSTEM_ERROR);
            //设置响应配备注
            response.setRemark("can not find message by the offset, " + requestHeader.getOffset());
        }

        return response;
    }
}
