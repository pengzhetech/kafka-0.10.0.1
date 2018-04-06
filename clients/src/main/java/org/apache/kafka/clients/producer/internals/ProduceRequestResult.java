/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;


/**
 * A class that models the future completion of a produce request for a single partition. There is one of these per
 * partition in a produce request and it is shared by all the {@link RecordMetadata} instances that are batched together
 * for the same partition in the request.
 * * ProduceRequestResult类并未实现java.util.concurrent.Future接口
 * * 但是其包含了一个count值为1的CountDownLatch对象,实现了类似Future的功能
 * * <p>
 * * 当RecordBatch中的全部消息被正常响,或超时,或关闭生产生产者时,会调用ProduceRequestResult.done()方法
 * * 将produceFuture标记为完成,并通过ProduceRequestResult.error字段区分"异常完成"还是"正常完成",之后调用
 * * CountDownLatch.countDown()方法,此时会唤醒阻塞在CountDownLatch对象的await()方法的线程(这些线程通过
 * * ProduceRequestResult对象的await()方法等待上述三个事件的发生)
 */
public final class ProduceRequestResult {

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile TopicPartition topicPartition;
    /**
     * 在服务端,broker会为分区的每条消息分配一个offset,并通过此offset维护消息顺序
     * <p>
     * baseOffset表示的事服务端为此RecordBatch种第一条消息分配的offset
     * 这样每个消息根据此offset以及自身在此RecordBatch的相对偏移量就可以计算出其在服务端分区中的偏移量了
     */
    private volatile long baseOffset = -1L;
    private volatile RuntimeException error;

    public ProduceRequestResult() {
    }

    /**
     * Mark this request as complete and unblock any threads waiting on its completion.
     *
     * @param topicPartition The topic and partition to which this record set was sent was sent
     * @param baseOffset     The base offset assigned to the record
     * @param error          The error that occurred if there was one, or null.
     */
    public void done(TopicPartition topicPartition, long baseOffset, RuntimeException error) {
        this.topicPartition = topicPartition;
        this.baseOffset = baseOffset;
        this.error = error;
        this.latch.countDown();
    }

    /**
     * Await the completion of this request
     */
    public void await() throws InterruptedException {
        latch.await();
    }

    /**
     * Await the completion of this request (up to the given time interval)
     *
     * @param timeout The maximum time to wait
     * @param unit    The unit for the max time
     * @return true if the request completed, false if we timed out
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    /**
     * The base offset for the request (the first offset in the record set)
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * The error thrown (generally on the server) while processing this request
     */
    public RuntimeException error() {
        return error;
    }

    /**
     * The topic and partition to which the record was appended
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * Has the request completed?
     */
    public boolean completed() {
        return this.latch.getCount() == 0L;
    }
}
