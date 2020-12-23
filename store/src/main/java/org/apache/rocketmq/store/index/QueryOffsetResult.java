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
package org.apache.rocketmq.store.index;

import java.util.List;

/**
 * 通过索引服务获取满足查询条件消息在CommitLog中物理偏移量返回结果
 * （条件包括指定topic+指定消息key+时间范围）
 */
public class QueryOffsetResult {
    //满足消息CommitLog中物理偏移量列表
    private final List<Long> phyOffsets;
    //查询到最后一个indexFile文件存储查存储最后一次更新索引数据时间(查询从indexFile文件列表最后一个文件依次遍历所有的indexFile，)
    private final long indexLastUpdateTimestamp;
    //查询到最后一个indexFile文件存储查询到最后一次更新索引消息物理偏移(查询从indexFile文件列表最后一个文件依次遍历所有的indexFile，)
    private final long indexLastUpdatePhyoffset;

    public QueryOffsetResult(List<Long> phyOffsets, long indexLastUpdateTimestamp,
        long indexLastUpdatePhyoffset) {
        this.phyOffsets = phyOffsets;
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
        this.indexLastUpdatePhyoffset = indexLastUpdatePhyoffset;
    }

    public List<Long> getPhyOffsets() {
        return phyOffsets;
    }

    public long getIndexLastUpdateTimestamp() {
        return indexLastUpdateTimestamp;
    }

    public long getIndexLastUpdatePhyoffset() {
        return indexLastUpdatePhyoffset;
    }
}
