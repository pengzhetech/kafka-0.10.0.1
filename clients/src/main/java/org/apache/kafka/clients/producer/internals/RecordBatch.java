/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A batch of records that is or will be sent.
 * <p>
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class RecordBatch {

    private static final Logger log = LoggerFactory.getLogger(RecordBatch.class);
    /**
     * 记录了保存的Record的个数
     */
    public int recordCount = 0;
    /**
     * 最大Record的字节数
     */
    public int maxRecordSize = 0;
    /**
     * 尝试发送当前RecordBatch的次数
     */
    public volatile int attempts = 0;
    public final long createdMs;
    public long drainedMs;
    /**
     * 最后一次尝试发送的时间戳
     */
    public long lastAttemptMs;
    /**
     * 指向用来存储数据的MemoryRecords对象
     */
    public final MemoryRecords records;
    /**
     * 当前RecordBatch中缓存的消息都会发送到此TopicPartition
     */
    public final TopicPartition topicPartition;
    /**
     * ProduceRequestResult类型,标识RecordBatch状态Future对象
     */
    public final ProduceRequestResult produceFuture;
    /**
     * 最后一次向RecordBatch追加消息的时间戳
     */
    public long lastAppendTime;
    /**
     * Thunk对象的集合(可以理解为消息的回调对象队列)
     * 为了保证Callback能被顺序执行,会将Callback顺序保存到list中
     * 在底层实现时将Callback对象与每次发送消息返回的FutureRecordMetadata对象封装成一个Thunk对象,在RecordBatch维护一个THunk的列表
     * 用于记录一批RecordBatch下每次发送的Record对应的Callback
     */
    private final List<Thunk> thunks;
    /**
     * 用来记录某消息在RecordBatch中的偏移量
     */
    private long offsetCounter = 0L;
    /**
     * 是否正在重试,如果RecordBatch中的数据发送失败,则会重新尝试发送
     */
    private boolean retry;

    public RecordBatch(TopicPartition tp, MemoryRecords records, long now) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.records = records;
        this.topicPartition = tp;
        this.produceFuture = new ProduceRequestResult();
        this.thunks = new ArrayList<Thunk>();
        this.lastAppendTime = createdMs;
        this.retry = false;
    }

    /**
     * RecordBatch最核心的方法tryAppend():尝试将消息添加到当前RecordBatch中缓存
     * Append the record to the current record set and return the relative offset within that record set
     *
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, long now) {
        //估算剩余空间不足(仅仅估算,不是准确值)
        if (!this.records.hasRoomFor(key, value)) {
            return null;
        } else {
            //向MemoryRecords中添加数据,offsetCounter是在RecordBatch中的偏移量
            long checksum = this.records.append(offsetCounter++, timestamp, key, value);
            //更新统计信息
            this.maxRecordSize = Math.max(this.maxRecordSize, Record.recordSize(key, value));
            this.lastAppendTime = now;
            //创建FutureRecordMetadata对象
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                    timestamp, checksum,
                    key == null ? -1 : key.length,
                    value == null ? -1 : value.length);
            //如果用户有自定义Callback,将用户自定义的Callback和FutureRecordMetadata
            //封装成Thunk,保存到thunks集合中
            if (callback != null)
                thunks.add(new Thunk(callback, future));
            //更新recordCount
            this.recordCount++;
            //返回FutureRecordMetadata对象
            return future;
        }
    }

    /**
     * 当RecordBatch成功收到正常响应,或超时,或关闭生产者时,都会调用RecordBatch的done()方法
     * 在done()方法中,会调用RecordBatch中全部消息的Callback回调,并调用produceFuture字段的done()方法
     * Complete the request
     *
     * @param baseOffset The base offset of the messages assigned by the server
     * @param timestamp  The timestamp returned by the broker.
     * @param exception  The exception that occurred (or null if the request was successful)
     */
    public void done(long baseOffset, long timestamp, RuntimeException exception) {
        log.trace("Produced messages to topic-partition {} with base offset offset {} and error: {}.",
                topicPartition,
                baseOffset,
                exception);
        // execute callbacks
        for (int i = 0; i < this.thunks.size(); i++) {
            try {
                //循环执行每个消息的Callback
                Thunk thunk = this.thunks.get(i);
                if (exception == null) {
                    //正常处理完成,将服务端返回的消息(offset,timestamp)和消息的其他信息封装成RecordMetadata
                    // If the timestamp returned by server is NoTimestamp, that means CreateTime is used. Otherwise LogAppendTime is used.
                    RecordMetadata metadata = new RecordMetadata(this.topicPartition, baseOffset, thunk.future.relativeOffset(),
                            timestamp == Record.NO_TIMESTAMP ? thunk.future.timestamp() : timestamp,
                            thunk.future.checksum(),
                            thunk.future.serializedKeySize(),
                            thunk.future.serializedValueSize());
                    //调用对应消息的Callback
                    thunk.callback.onCompletion(metadata, null);
                } else {
                    //处理过程中出现异常,注意 第一个参数为null,与上面情况相反
                    thunk.callback.onCompletion(null, exception);

                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition {}:", topicPartition, e);
            }
        }
        //标识整个RecordBatch都已经处理完成
        this.produceFuture.done(topicPartition, baseOffset, exception);
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    final private static class Thunk {
        //callback指向对应消息的callback对象
        final Callback callback;
        final FutureRecordMetadata future;

        public Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "RecordBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * A batch whose metadata is not available should be expired if one of the following is true:
     * <ol>
     * <li> the batch is not in retry AND request timeout has elapsed after it is ready (full or linger.ms has reached).
     * <li> the batch is in retry AND request timeout has elapsed after the backoff period ended.
     * </ol>
     */
    public boolean maybeExpire(int requestTimeoutMs, long retryBackoffMs, long now, long lingerMs, boolean isFull) {
        boolean expire = false;

        if (!this.inRetry() && isFull && requestTimeoutMs < (now - this.lastAppendTime))
            expire = true;
        else if (!this.inRetry() && requestTimeoutMs < (now - (this.createdMs + lingerMs)))
            expire = true;
        else if (this.inRetry() && requestTimeoutMs < (now - (this.lastAttemptMs + retryBackoffMs)))
            expire = true;

        if (expire) {
            this.records.close();
            this.done(-1L, Record.NO_TIMESTAMP, new TimeoutException("Batch containing " + recordCount + " record(s) expired due to timeout while requesting metadata from brokers for " + topicPartition));
        }

        return expire;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    public boolean inRetry() {
        return this.retry;
    }

    /**
     * Set retry to true if the batch is being retried (for send)
     */
    public void setRetry() {
        this.retry = true;
    }
}
