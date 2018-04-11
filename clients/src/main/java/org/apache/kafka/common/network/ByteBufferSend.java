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
package org.apache.kafka.common.network;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * A send backed by an array of byte buffers
 */
public class ByteBufferSend implements Send {

    /**
     * 目的地:即kafka地址
     */
    private final String destination;
    /**
     * 发送的字节大小
     */
    private final int size;
    /**
     * 需要发送的buffers
     */
    protected final ByteBuffer[] buffers;
    /**
     * 剩余多少未发送
     */
    private int remaining;
    /**
     * pending 总是返回false
     * always returns false as there will be not be any
     * * pending writes since we directly write to socketChannel.
     */
    private boolean pending = false;

    public ByteBufferSend(String destination, ByteBuffer... buffers) {
        super();
        this.destination = destination;
        this.buffers = buffers;
        for (int i = 0; i < buffers.length; i++)
            remaining += buffers[i].remaining();
        this.size = remaining;
    }

    @Override
    public String destination() {
        return destination;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        remaining -= written;
        // This is temporary workaround(工作区,变通方案). As Send , Receive interfaces are being used by BlockingChannel.
        // Once BlockingChannel is removed we can make Send, Receive to work with transportLayer rather than
        // GatheringByteChannel or ScatteringByteChannel.
        if (channel instanceof TransportLayer)
            pending = ((TransportLayer) channel).hasPendingWrites();

        return written;
    }
}
