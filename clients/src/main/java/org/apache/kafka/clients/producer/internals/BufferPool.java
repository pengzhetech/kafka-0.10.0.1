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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 * <p>
 * * ByteBuffer的创建和释放是比较消耗系统资源的,为了实现内存的高效应用
 * * Kafka客户端使用BufferPool来实现ByteBuffer的复用
 * * 每个BufferPool对象只针对特定大小(由poolableSize字段指定)的ByteBuffer进行管理
 * * 对于其他大小的ByteBuffer并不会缓存进BufferPool
 * * 一般情况下,我们会调整MemoryRecords的大小(由RecordAccumulator.batchSize字段指定)
 * * 使每个MemoryRecords可以缓存多条消息,就不会复用BufferPool中缓存的ByteBuffer
 * * 而是额外分配ByteBuffer,在它被使用完后也不会方法BufferPool进行管理
 * * 而是直接丢弃由GC回收
 */
public final class BufferPool {

    /**
     * 记录了整个pool的大小
     */
    private final long totalMemory;
    private final int poolableSize;
    /**
     * 因为有多线程并发分配和回收ByteBuffer,所以使用锁控制并发,保证线程安全
     */
    private final ReentrantLock lock;
    /**
     * 一个Deque<ByteBuffer>队列,其中缓存了指定大小的ByteBuffer对象
     */
    private final Deque<ByteBuffer> free;
    /**
     * 记录因申请不到足够空间而阻塞的线程,此队列中实际记录的是阻塞线程对应的condition对象
     */
    private final Deque<Condition> waiters;
    /**
     * 记录了可用的空间大小,这个空间是totalMemory减去free列表中全部ByteBuffer的大小
     */
    private long availableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;

    /**
     * Create a new buffer pool
     *
     * @param memory        The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize  The buffer size to cache in the free list rather than deallocating
     * @param metrics       instance of Metrics
     * @param time          time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<ByteBuffer>();
        this.waiters = new ArrayDeque<Condition>();
        this.totalMemory = memory;
        this.availableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor("bufferpool-wait-time");
        MetricName metricName = metrics.metricName("bufferpool-wait-ratio",
                metricGrpName,
                "The fraction of time an appender waits for space allocation.");
        this.waitTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));
    }

    /**
     * allocate方法负责从缓存池中申请ByteBuffer,当缓冲池中空间不足时,就会阻塞调用线程
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     *
     * @param size             The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException     If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *                                  forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                    + " bytes, but there is a hard limit of "
                    + this.totalMemory
                    + " on memory allocations.");
        //加锁同步
        this.lock.lock();
        try {
            //请求的是poolableSize指定大小的ByteBuffer,且free中有空闲的yteBuffer
            // check if we have a free buffer of the right size pooled
            if (size == poolableSize && !this.free.isEmpty())
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            //当申请的空间不是poolableSize,执行下面的逻辑
            int freeListSize = this.free.size() * this.poolableSize;
            if (this.availableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request
                //为了让availableMemory>size,freeUp()方法会从free队列中不断释放ByteBuffer
                //直到availableMemory满足这次申请
                freeUp(size);
                //减少availableMemory
                this.availableMemory -= size;
                lock.unlock();
                return ByteBuffer.allocate(size);
            } else {
                //没有空间了,只有阻塞
                // we are out of memory and will have to block
                int accumulated = 0;
                ByteBuffer buffer = null;
                Condition moreMemory = this.lock.newCondition();
                long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                //将Condition添加到waiters中
                this.waiters.addLast(moreMemory);
                // loop over and over until we have a buffer or have reserved
                // enough memory to allocate one
                //循环等待
                while (accumulated < size) {
                    long startWaitNs = time.nanoseconds();
                    long timeNs;
                    boolean waitingTimeElapsed;
                    try {
                        //阻塞
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        //出现异常,移除此线程对应的Condition
                        this.waiters.remove(moreMemory);
                        throw e;
                    } finally {
                        //统计阻塞时间
                        long endWaitNs = time.nanoseconds();
                        timeNs = Math.max(0L, endWaitNs - startWaitNs);
                        this.waitTime.record(timeNs, time.milliseconds());
                    }

                    if (waitingTimeElapsed) {
                        //超时,报错
                        this.waiters.remove(moreMemory);
                        throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                    }

                    remainingTimeToBlockNs -= timeNs;
                    // check if we can satisfy this request from the free list,
                    // otherwise allocate memory
                    //请求的是poolableSize大小的ByteBuffer,且free中有空闲的ByteBuffer
                    if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                        // just grab a buffer from the free list
                        buffer = this.free.pollFirst();
                        accumulated = size;
                    } else {
                        // we'll need to allocate memory, but we may only get
                        // part of what we need on this iteration
                        //先分配一部分空间,并继续等待空闲时间
                        freeUp(size - accumulated);
                        int got = (int) Math.min(size - accumulated, this.availableMemory);
                        this.availableMemory -= got;
                        accumulated += got;
                    }
                }

                // remove the condition for this thread to let the next thread
                // in line start getting memory
                //已经成功分配空间,移除Condition
                Condition removed = this.waiters.removeFirst();
                if (removed != moreMemory)
                    throw new IllegalStateException("Wrong condition: this shouldn't happen.");

                // signal any additional waiters if there is more memory left
                // over for them
                //要是还有空间空闲,就唤醒下一个线程
                if (this.availableMemory > 0 || !this.free.isEmpty()) {
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                }

                // unlock and return the buffer
                lock.unlock();//解锁
                if (buffer == null)
                    return ByteBuffer.allocate(size);
                else
                    return buffer;
            }
        } finally {
            if (lock.isHeldByCurrentThread())
                lock.unlock();//解锁
        }
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.availableMemory < size)
            this.availableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     *
     * @param buffer The buffer to return
     * @param size   The size of the buffer to mark as deallocated, note that this maybe smaller than buffer.capacity
     *               since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();//加锁
        try {
            //释放的是ByteBuffer大小的poolableSize,放入free队列中进行管理
            if (size == this.poolableSize && size == buffer.capacity()) {
                buffer.clear();
                this.free.add(buffer);
            } else {
                //释放的ByteBuffer不是poolableSize,不会复用ByteBuffer,仅修改availableMemory的大小
                this.availableMemory += size;
            }
            //唤醒一个因空间不足而阻塞的线程
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.availableMemory + this.free.size() * this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.availableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
