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

package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 *  CountDownLatch 基础上增加重置计数器功能
 */
public class CountDownLatch2 {
    private final Sync sync;

    /**
     * CountDownLatch2 构造函数
     */
    public CountDownLatch2(int count) {
        if (count < 0)
            throw new IllegalArgumentException("count < 0");
        this.sync = new Sync(count);
    }

    /**
     * 阻塞等待唤醒
     */
    public void await() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    /**
     * 阻塞等待唤醒，添加超时机制
     */
    public boolean await(long timeout, TimeUnit unit)
        throws InterruptedException {
        return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }
    /**
     * 计数器-1
     */
    public void countDown() {
        sync.releaseShared(1);
    }


    /**
     * 获取计数器
     */
    public long getCount() {
        return sync.getCount();
    }

    /**
     * 重置
     */
    public void reset() {
        sync.reset();
    }

    public String toString() {
        return super.toString() + "[Count = " + sync.getCount() + "]";
    }

    private static final class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = 4982264981922014374L;

        private final int startCount;

        /** 实例化Sync，设置同步状态 **/
        Sync(int count) {
            this.startCount = count;
            setState(count);
        }

        /** 获取同步状态 **/
        int getCount() {
            return getState();
        }

        /**
         * 获取同步状态
         * 同步状态为0时释放获取同步状态成功，否则失败
         */
        protected int tryAcquireShared(int acquires) {
            return (getState() == 0) ? 1 : -1;
        }

        /**
         * 释放同步状态
         *
         * 使用CAS+循环将同步状态-1
         */
        protected boolean tryReleaseShared(int releases) {
            // Decrement count; signal when transition to zero
            for (; ; ) {
                int c = getState();
                if (c == 0)
                    return false;
                int nextc = c - 1;
                if (compareAndSetState(c, nextc))
                    return nextc == 0;
            }
        }

        /**
         * 重置同步状态
         */
        protected void reset() {
            setState(startCount);
        }
    }
}
