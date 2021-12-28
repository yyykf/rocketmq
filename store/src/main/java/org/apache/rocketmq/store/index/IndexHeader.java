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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 索引文件的头部信息
 */
public class IndexHeader {
    /** 头部大小，40字节 */
    public static final int INDEX_HEADER_SIZE = 40;
    /** 最小存储时间起始字节偏移量 */
    private static int beginTimestampIndex = 0;
    /** 最大存储时间起始字节偏移量 */
    private static int endTimestampIndex = 8;
    /** 最小物理偏移量起始字节偏移量 */
    private static int beginPhyoffsetIndex = 16;
    /** 最大物理偏移量起始字节偏移量 */
    private static int endPhyoffsetIndex = 24;
    /** 已用 hash 槽个数起始字节偏移量 */
    private static int hashSlotcountIndex = 32;
    /** 已有的 hash 槽个数起始字节偏移量 */
    private static int indexCountIndex = 36;
    private final ByteBuffer byteBuffer;
    /** 最小的消息存储时间，8字节 */
    private AtomicLong beginTimestamp = new AtomicLong(0);
    /** 最大的消息存储时间，8字节 */
    private AtomicLong endTimestamp = new AtomicLong(0);
    /** 最小的消息物理偏移量，8字节 */
    private AtomicLong beginPhyOffset = new AtomicLong(0);
    /** 最大的消息物理偏移量，8字节 */
    private AtomicLong endPhyOffset = new AtomicLong(0);
    /** 已用的 hash 槽个数，4字节 */
    private AtomicInteger hashSlotCount = new AtomicInteger(0);

    /** 已有的 index 条目数，4字节 todo 为什么默认值是1，应该是因为索引下标起始值为1，因此当某个索引槽的 prevIndexNo 为 0的时候，就可以代表是链表的首个元素了 */
    private AtomicInteger indexCount = new AtomicInteger(1);

    public IndexHeader(final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public void load() {
        this.beginTimestamp.set(byteBuffer.getLong(beginTimestampIndex));
        this.endTimestamp.set(byteBuffer.getLong(endTimestampIndex));
        this.beginPhyOffset.set(byteBuffer.getLong(beginPhyoffsetIndex));
        this.endPhyOffset.set(byteBuffer.getLong(endPhyoffsetIndex));

        this.hashSlotCount.set(byteBuffer.getInt(hashSlotcountIndex));
        this.indexCount.set(byteBuffer.getInt(indexCountIndex));

        if (this.indexCount.get() <= 0) {
            this.indexCount.set(1); // 默认值为1?
        }
    }

    public void updateByteBuffer() {
        this.byteBuffer.putLong(beginTimestampIndex, this.beginTimestamp.get());
        this.byteBuffer.putLong(endTimestampIndex, this.endTimestamp.get());
        this.byteBuffer.putLong(beginPhyoffsetIndex, this.beginPhyOffset.get());
        this.byteBuffer.putLong(endPhyoffsetIndex, this.endPhyOffset.get());
        this.byteBuffer.putInt(hashSlotcountIndex, this.hashSlotCount.get());
        this.byteBuffer.putInt(indexCountIndex, this.indexCount.get());
    }

    public long getBeginTimestamp() {
        return beginTimestamp.get();
    }

    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp.set(beginTimestamp);
        this.byteBuffer.putLong(beginTimestampIndex, beginTimestamp);
    }

    public long getEndTimestamp() {
        return endTimestamp.get();
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp.set(endTimestamp);
        this.byteBuffer.putLong(endTimestampIndex, endTimestamp);
    }

    public long getBeginPhyOffset() {
        return beginPhyOffset.get();
    }

    public void setBeginPhyOffset(long beginPhyOffset) {
        this.beginPhyOffset.set(beginPhyOffset);
        this.byteBuffer.putLong(beginPhyoffsetIndex, beginPhyOffset);
    }

    public long getEndPhyOffset() {
        return endPhyOffset.get();
    }

    public void setEndPhyOffset(long endPhyOffset) {
        this.endPhyOffset.set(endPhyOffset);
        this.byteBuffer.putLong(endPhyoffsetIndex, endPhyOffset);
    }

    public AtomicInteger getHashSlotCount() {
        return hashSlotCount;
    }

    public void incHashSlotCount() {
        int value = this.hashSlotCount.incrementAndGet();
        this.byteBuffer.putInt(hashSlotcountIndex, value);
    }

    public int getIndexCount() {
        return indexCount.get();
    }

    public void incIndexCount() {
        int value = this.indexCount.incrementAndGet();
        this.byteBuffer.putInt(indexCountIndex, value);
    }
}
