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

package org.apache.rocketmq.common.protocol.body;

/**
 * ConsumeStatus 表示指定消费实例消费状态（各种RT,TPS统计）
 */
public class ConsumeStatus {
    /**
     * 拉去RT（响应时间）
     */
    private double pullRT;
    /**
     * 拉取TPS（每秒钟request/事务 数量）
     */
    private double pullTPS;
    /**
     * 消费RT（响应时间）
     */
    private double consumeRT;
    /**
     * 消费成功TPS（每秒钟request/事务 数量）
     */
    private double consumeOKTPS;
    /**
     * 消费失败TPS（每秒钟request/事务 数量）
     */
    private double consumeFailedTPS;
    /**
     * 消费失败消息数量
     */
    private long consumeFailedMsgs;

    public double getPullRT() {
        return pullRT;
    }

    public void setPullRT(double pullRT) {
        this.pullRT = pullRT;
    }

    public double getPullTPS() {
        return pullTPS;
    }

    public void setPullTPS(double pullTPS) {
        this.pullTPS = pullTPS;
    }

    public double getConsumeRT() {
        return consumeRT;
    }

    public void setConsumeRT(double consumeRT) {
        this.consumeRT = consumeRT;
    }

    public double getConsumeOKTPS() {
        return consumeOKTPS;
    }

    public void setConsumeOKTPS(double consumeOKTPS) {
        this.consumeOKTPS = consumeOKTPS;
    }

    public double getConsumeFailedTPS() {
        return consumeFailedTPS;
    }

    public void setConsumeFailedTPS(double consumeFailedTPS) {
        this.consumeFailedTPS = consumeFailedTPS;
    }

    public long getConsumeFailedMsgs() {
        return consumeFailedMsgs;
    }

    public void setConsumeFailedMsgs(long consumeFailedMsgs) {
        this.consumeFailedMsgs = consumeFailedMsgs;
    }
}
