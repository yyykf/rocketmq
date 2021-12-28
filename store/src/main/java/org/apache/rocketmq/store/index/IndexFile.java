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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 每个哈希槽
 */
public class IndexFile {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /** hash 槽元素的大小，4字节 */
    private static int hashSlotSize = 4;
    /**  index 索引条目的大小，20字节 */
    private static int indexSize = 20;
    /** 最小的 index 索引条目下标 */
    private static int invalidIndex = 0;
    /** 最大的 hash 槽个数  */
    private final int hashSlotNum;
    /** 最大的 index 条目个数 */
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    /** 头部，记录了索引消息的最小/大存储时间，最小/大物理偏移量，已用的hash槽和index个数 */
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key);  // 得到 key 的正数的哈希值 todo 如果是2的整数次幂，那直接 & 就可以得到非负数了
            int slotPos = keyHash % this.hashSlotNum; // 直接取模？可以设计成2的整数次幂，直接 & ，最多50w个槽，那就是 [0, 499999]
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;    // 对应 hash 槽的偏移量 = indexHeader 长度 + 槽位 * hash 槽长度

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);  //todo 看起来是将文件中的某一段位置加锁，这里是给对应的 hash 槽加锁，不加锁的话，并发构建添加索引项的时候怎么办？
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);   // 获取 hash 槽中的数据，记录了该 hash 对应的最新的 index 下标
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) { // todo 默认值似乎为 0？
                    slotValue = invalidIndex;
                }

                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000; // todo ms 转 s?

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // fixme 索引下标是从1开始的，那这里的偏移量是不是会多出一个 index 的字节？
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize; // index 槽是按下标一个一个用的 todo 这里this.indexHeader.getIndexCount() 并发不会相同吗？改为 getAndIncrement 会不会更好？

                // hash 条目第一项：4字节的 key hash值
                this.mappedByteBuffer.putInt(absIndexPos, keyHash); // todo 可以考虑直接用 UNSAFE 操作 DirectByteBuffer，去除边界检查这些
                // 第二项：8字节的物理偏移量
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // 第三项：4字节的 commitlog 写入时间 - 当前文件中第一个消息写入时间 todo 还不知道有什么用
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // 第四项：4字节的上一下标，默认为0，代表链表首个元素
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                // 更新对应 hash 槽中的值为当前 index 下标
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);  // todo 只会在添加第一个 index 的时候设置起始偏移量和存储时间？哦对，因为commitlog顺序写，所以偏移量和存储时间肯定是在递增的
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 更新已用hash槽个数，已有index数，最大偏移量/存储时间
                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();   // 消息key的哈希值 todo 是否可以加入扰动函数来提高散列度
        int keyHashPositive = Math.abs(keyHash);    // 需要正数的 hash
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        // begin，end 是否超过了当前索引文件的范围，即 begin < file.begin < file.end < end，是的话说明整个索引文件中的消息都符合条件
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        // 否则判断是否条件部分包含于当前索引文件的时间范围中，即 file.begin <= begin <= file.end，是的话说明部分索引消息符合条件
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        // 否则判断是否条件部分包含于当前索引文件的时间范围中，即 file.begin <= end <= file.end，是的话说明部分索引消息符合条件
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     *
     * @param phyOffsets 符合条件的消息偏移量集合
     * @param key key
     * @param maxNum 条数
     * @param begin 起始时间
     * @param end 结束时间
     * @param lock 是否最后一个索引文件？
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        // todo 当前索引文件是否被持有?
        if (this.mappedFile.hold()) {
            // 计算 hash 槽下标
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            // 计算 hash 槽偏移量
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                // 获取 index 下标
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                    // 如果 hash 槽的值无效，说明该 key 在该文件中没有索引消息，todo 或者 index 个数 == 1，说明还没有索引?
                } else {
                    // nextIndexToRead 为要读取的 index 下标
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        // 定位
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        // hash
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        //物理偏移量
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;  // todo 什么时候会小于0
                        }

                        timeDiff *= 1000L;  // s 转 ms？

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;    // 得到存储时间
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end); // 消息是否符合查询时间范围

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);  // key 的 hash 值符合，存储时间也符合。todo 有个问题，不同的 key 的哈希值是有可能冲突的，只判断 hash 值的话不会有误吗？取出来后再判断key
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;    // 遍历上一个 index 条目
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
