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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * The future result of a record send
 * * FutureRecordMetadata实现了 java.util.concurrent.Future接口
 * * 但是其实现基本上是委托ProduceRequestResult对应的方法,由此可知
 * * 消息应该是按照RecordBatch进行发送和确认的
 */
public final class FutureRecordMetadata implements Future<RecordMetadata> {
    /**
     * ProduceRequestResult类型,指向对应消息所在RecordBatch的produceFuture字段
     */
    private final ProduceRequestResult result;
    /**
     * long类型,记录了对应消息在RecordBatch中偏移量
     */
    private final long relativeOffset;
    private final long timestamp;
    private final long checksum;
    private final int serializedKeySize;
    private final int serializedValueSize;

    public FutureRecordMetadata(ProduceRequestResult result, long relativeOffset, long timestamp,
                                long checksum, int serializedKeySize, int serializedValueSize) {
        this.result = result;
        this.relativeOffset = relativeOffset;
        this.timestamp = timestamp;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
    }

    @Override
    public boolean cancel(boolean interrupt) {
        return false;
    }

    /**
     * 当生产者收到某消息的响应时,FutureRecordMetadata.get()方法就会返回RecordMetadata对象
     * 其中包含消息在Partition中的offset等其他元数据,用户可以自动以Callback使用
     *
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        this.result.await();
        return valueOrError();
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        boolean occurred = this.result.await(timeout, unit);
        if (!occurred)
            throw new TimeoutException("Timeout after waiting for " + TimeUnit.MILLISECONDS.convert(timeout, unit) + " ms.");
        return valueOrError();
    }

    RecordMetadata valueOrError() throws ExecutionException {
        if (this.result.error() != null)
            throw new ExecutionException(this.result.error());
        else
            return value();
    }

    RecordMetadata value() {
        return new RecordMetadata(result.topicPartition(), this.result.baseOffset(), this.relativeOffset,
                this.timestamp, this.checksum, this.serializedKeySize, this.serializedValueSize);
    }

    public long relativeOffset() {
        return this.relativeOffset;
    }

    public long timestamp() {
        return this.timestamp;
    }

    public long checksum() {
        return this.checksum;
    }

    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return this.result.completed();
    }

}
