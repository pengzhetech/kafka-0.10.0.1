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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 * Sender将消息发送发送Kafka主要依赖:KafkaClient RecordAccumulator Metadata
 * 基本流程:首先从Metadata中获取集群元数据信息,然后从RecordAccumulator中取出以满足发送条件的RecordBatch,并构造相关网络层请求
 * 交由Network去执行 在这个过程中需要取出每个TopicPartition所对应的的分区Leader,有可能某个TopicPartition的Leader不存在
 * 则会触发请求Metadata更新操作.在发送过程中NetworkClient内部维护了一个InFlightRequests类型的inFlightRequests对象用于保存
 * 已发送但是还没收到响应的请求,在这个流程当中Sender很向一个任务调度器,而NetworkClient是网络请求的真正执行者
 * Sender不断的从RecordAccumulator取出数据构造请求交由NetworkClient去执行
 * <p>
 * 首先根据RecordAccumulator的缓存情况 筛选出可以向哪些节点发送消息(即RecordAccumulator.ready()方法)
 * 然后根据生产者与各个节点的连接情况(由NetworkClient管理)过滤node节点
 * 之后 生成相应的请求 这里需要特别注意: 每个Node节点只生成一个请求
 * 最后调用NetworkClient将请求发送出去
 */
public class Sender implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Sender.class);

    /* the state of each nodes connection */
    /**
     * kafka网络客户端
     */
    private final KafkaClient client;

    /* the record accumulator that batches records */
    /**
     * 消息累加器
     */
    private final RecordAccumulator accumulator;

    /* the metadata for the client */
    /**
     * 元数据
     */
    private final Metadata metadata;

    /**
     * 是否需要保证消息顺序的标志
     */
    /* the flag indicating whether the producer should guarantee the message order on the broker or not. */
    private final boolean guaranteeMessageOrder;

    /* the maximum request size to attempt to send to the server */
    /**
     * 发送到broker的消息最大大小
     */
    private final int maxRequestSize;

    /* the number of acknowledgements to request from the server */
    private final short acks;

    /* the number of times to retry a failed request before giving up */
    private final int retries;

    /* the clock instance used for getting the time */
    private final Time time;

    /* true while the sender thread is still running */
    private volatile boolean running;

    /* true when the caller wants to ignore all unsent/inflight messages and force close.  */
    private volatile boolean forceClose;

    /* metrics */
    private final SenderMetrics sensors;

    /* param clientId of the client */
    private String clientId;

    /* the max time to wait for the server to respond to the request*/
    private final int requestTimeout;

    public Sender(KafkaClient client,
                  Metadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  Metrics metrics,
                  Time time,
                  String clientId,
                  int requestTimeout) {
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.clientId = clientId;
        this.sensors = new SenderMetrics(metrics);
        this.requestTimeout = requestTimeout;
    }

    /**
     * The main run loop for the sender thread
     */
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");
        /**
         * KafkaProducer没有close之前,会一直运行
         */
        // main loop, runs until close is called
        while (running) {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be
        // requests in the accumulator or waiting for acknowledgment,
        // wait until these are completed.
        /**
         * KafkaProducer已经close 但没有forceClose会将accumulator中还未发送出去的消息接着发送出去
         */
        while (!forceClose && (this.accumulator.hasUnsent() || this.client.inFlightRequestCount() > 0)) {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }
        //强制关闭客户端 剩下的消息也直接抛弃
        if (forceClose) {
            // We need to fail all the incomplete batches and wake up the threads waiting on
            // the futures.
            this.accumulator.abortIncompleteBatches();
        }
        try {
            this.client.close();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * 1:从Metadata获取Kafka集群元数据
     * 2:调用RecordAccumulator.ready()方法根据RecordAccumulator缓存情况 选出可以向哪些Node节点发送信息 返回ReadyCheckResult对象
     * 3:如果ReadyCheckReault中有标识unknownLeadersExits 则调用Metadata的requestUpdate方法 标记需要更新kafka进群信息
     * 4:针对ReadyCheckResult中readyNodes集合 循环调用NetwotkClient.ready方法 目的是检查网络I/O方面是否符合发送消息的条件 不符合的条件的Node将会从readyNodes集合中删除
     * 5:针对经过步骤4处理后的readyNodes集合 调用RecordAccumulator.drain()方法 获取待发送的消息集合
     * 6:调用RecordAccumulator.abortExpireBatches()方法处理RecordAccumulator中超时的消息 其代码逻辑是 遍历RecordAccumulator中保存的全部RecordBatch 调用RecordBatch.maybeExpire方法进行处理 如果已超时 则调用RecordBatch.done方法 其中会触发自定义Callback 并将RecordBatch从队列中移除 释放ByteBuffer空间
     * 7:调用Sender.createProducerRecord()方法将待发送的消息封装成ClientRequest
     * 8:调用NetworkClient.send方法将ClientRequest写入KafkaChannel中的send字段
     * 9:调用NetworkClient.poll方法将KafkaChannel字段中的保存的ClientRequest发送出去 同时还会处理服务端的响应 处理超时的请求 调用用户自定义的Callback等
     * Run a single iteration of sending
     *
     * @param now The current POSIX time in milliseconds
     */
    void run(long now) {
        /**
         * 1:从metadata中获取Cluster元数据信息
         */
        Cluster cluster = metadata.fetch();
        // get the list of partitions with data ready to send
        /**
         * 从RecordAccumulator角度检测是否ready
         * 2:获取各TopicPartition分区的Leader节点集合
         */
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);

        // if there are any partitions whose leaders are not known yet, force metadata update
        /**
         * 3:根据第2步返回的结果ReadyCheckResult对象,进行以下处理
         * 若unknownLeadersExist为true即存在没有找到Leader的分区,则调用metadata.requestUpdate()请求更新元数据
         */
        if (result.unknownLeadersExist)
            this.metadata.requestUpdate();

        // remove any nodes we aren't ready to send to
        /**
         *  从网络角度检测是否ready(没有ready的节点移除)
         *
         * 4:检测ReadyCheckResult.readyNodes集合中节点连接状态,通过调用NetworkClient.ready()方法来完成检测工作,
         * 该方法除检测连接状态之外,同时根据一定条件决定是否为还未建立连接的节点创建连接.若与某个节点的连接还未就绪则将该节点从readyNodes中移除
         * 经过NetworkClient.ready()方法处理之后,readyNodes集合中的所有节点均已与NetworkClient建立了连接
         */
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Node node = iter.next();
            if (!this.client.ready(node, now)) {
                iter.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.connectionDelay(node, now));
            }
        }

        // create produce requests
        /**
         * 5:根据readyNodes中各节点Node的id进行分组,每个node对应一个List<RecordBatch>集合，先取出同一个Leader下的所有分区
         * 然后按序取出每个分区对应的双端队列deque,,从deque头部取出第一个RecordBatch，计算该RecordBatch的字节总数并累加到局部变量size中
         * 若size的值不大于${max.request.size}的值 则将该RecordBatch添加到对应Node的ready集合中，或者size的值大于${max.request.size}
         * 但此时ready为空,表示这是第一个且超过请求设置的最大阈值的RecordBatch,依然将该RecordBatch添加到ready集合中准备发送
         * 如果某个RecordBatch满足添加到与之对应的ready集合的条件,在添加之前需要将该RecordBatch关闭 保证该RecordBatch不在接收新的Record写入
         * 如果不满足将其添加到与之关联的ready集合的条件,则该节点的所有分区本次构造发送请求提前结束 继续迭代下一个节点进行同样的处理
         * 经过第5步的处理 为readyNodes集合中保存的各节点构造了一个Map<Integer，List<RecordBatch>>类型的集合batches
         * 该map对象以节点id为key,以该节点为leader节点的所有或部分分区对应双端队列的第一个RecordBatch构成的List集合作为Value
         */
        Map<Integer, List<RecordBatch>> batches = this.accumulator.drain(cluster,
                result.readyNodes,
                this.maxRequestSize,
                now);
        /**
         *6:如果要保证消息发送有序,则对第5步处理得到的Map取其values进行二重迭代
         * 对每个RecordBatch，调用accumulator.mutePartition()进行处理
         * 该方法会将每个RecordBatch的TopicPartition添加到一个HashSet类型的muted集合中,其实质是提取所有的RecordBatch的TopicPartition
         * 根据set特性对该TopicPartition去重
         */
        if (guaranteeMessageOrder) {
            // Mute all the partitions drained
            for (List<RecordBatch> batchList : batches.values()) {
                for (RecordBatch batch : batchList)
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }
        /**
         * 7:根据配置项${request.timeout.ms}的值,该配置项默认是30s 过滤掉请求已经超时的RecordBatch
         * 若已超时则将该RecordBatch添加到过期集合list中 并将该RecordBatch从双端队列中移除 同时释放内存空间
         * 然后将过期的RecordBatch交由SenderMetrics进行处理 更新和记录相应的metrics信息
         */
        List<RecordBatch> expiredBatches = this.accumulator.abortExpiredBatches(this.requestTimeout, now);
        // update sensors
        for (RecordBatch expiredBatch : expiredBatches)
            this.sensors.recordErrors(expiredBatch.topicPartition.topic(), expiredBatch.recordCount);

        sensors.updateProduceRequestMetrics(batches);
        /**
         * 8:遍历第5步得到的batches， 根据batches分组的Node 将每个Node转化为一个ClientRequest对象
         * 最终将batches转化为List<ClientRequest>集合
         */
        List<ClientRequest> requests = createProduceRequests(batches, now);
        // If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so this can immediately
        // loop and try sending more data. Otherwise, the timeout is determined by nodes that have partitions with data
        // that isn't yet sendable (e.g. lingering, backing off). Note that this specifically does not include nodes
        // with sendable data that aren't ready to send since they would cause busy looping.
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        if (result.readyNodes.size() > 0) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            log.trace("Created {} produce requests: {}", requests.size(), requests);
            pollTimeout = 0;
        }
        /**
         * 9:遍历第8步得到的List<ClientRequest>集合
         * 首先调用NetworkClient的send()方法执行网络层消息传输,向相应broker发送请求,在send()方法中首先将ClientRequest添加到
         * InFlightRequests队列中 该队列记录了一系列正在被发送或是已发送但是还未收到相应的ClientRequest 然后调用Selector.send(Send send)
         * 方法 但此时数据并没有真正发送出去 只是暂存在Selector内部对应的KafkaChannel里面.
         * 在Selector内部维护了一个Map<String，KafkaChannel>类型的channels 即每个Node对应一个KafkaChannel
         *
         * 一个KafkaChannel一次只能存放一个Send数据包 在当前的Send数据包没有完整的发出去之前,不能存放下一个Send 否则抛出异常
         *
         */
        for (ClientRequest request : requests)
            client.send(request, now);

        // if some partitions are already ready to be sent, the select time would be 0;
        // otherwise if some partition already has some data accumulated but not ready yet,
        // the select time will be the time difference between now and its linger expiry time;
        // otherwise the select time will be the time difference between now and the metadata expiry time;
        /**
         * 10:调用NetworkClient的poll方法真正进行读写操作
         * 该方法首先调用MetadataUpdate.maybeUpdate方法检查是否需要更新元数据信息,然后调用Selector.poll方法真正执行网络I/)操作
         * 最后对已经完成的请求对其相应结果response进行处理
         */
        this.client.poll(pollTimeout, now);
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        this.running = false;
        this.accumulator.close();
        this.wakeup();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        initiateClose();
    }

    /**
     * Handle a produce response
     * <p>
     * 经过NetworkClient一系列的handle*()方法之后,NetworkClient.poll()方法中产生的全部ClientResponse已经被收集到response列表中了
     * 之后遍历response调用每个ClientRequest中记录的回调,如果是异常响应则请求重发,如果是正常响应,则调用每个消息自定义的Callback
     * 这里调用的Callback对象,也就是RequestCompletionHandler对象,其onComplete()方法最终调用Sender.handleProduceResponse对象
     * <p>
     * Sender.handleProduceResponse()方法主要逻辑:
     * <p>
     * (1):如果是因为断开连接或异常而产生的响应:
     * a:遍历ClientRequest中的RecordBatch,则尝试将RecordBatch重新加入RecordAccumulator,重新发送
     * b:如果是异常类型不允许充实或重试次数达到上限,则执行RecordBatch.done()方法,此方法会循环调用RecordBatch中每个消息的Callback函数
     * 并将RecordBatch的producerFuture设置为"异常完成"后,最后释放RecordBatch底层的ByteBuffer
     * c:最后,根据异常类型,决定是否设置更新Metadata标志
     * (2):如果是服务端正常的响应或不需要响应的情况下:
     * a:解析响应
     * b:遍历ClientRequest中的RecordBatch,并执行RecordBatch的done()方法
     * c:释放RecordBatch底层的ByteBuffer
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, RecordBatch> batches, long now) {
        int correlationId = response.request().request().header().correlationId();
        //对于断开连接而产生的ClientResponse,会重试发送请求,若不能重试,则调用其中每条消息的Callback
        if (response.wasDisconnected()) {
            log.trace("Cancelled request {} due to node {} being disconnected", response, response.request()
                    .request()
                    .destination());
            for (RecordBatch batch : batches.values())
                completeBatch(batch, Errors.NETWORK_EXCEPTION, -1L, Record.NO_TIMESTAMP, correlationId, now);
        } else {
            log.trace("Received produce response from node {} with correlation id {}",
                    response.request().request().destination(),
                    correlationId);
            // if we have a response, parse it
            if (response.hasResponse()) {
                ProduceResponse produceResponse = new ProduceResponse(response.responseBody());
                for (Map.Entry<TopicPartition, ProduceResponse.PartitionResponse> entry : produceResponse.responses().entrySet()) {
                    TopicPartition tp = entry.getKey();
                    ProduceResponse.PartitionResponse partResp = entry.getValue();
                    Errors error = Errors.forCode(partResp.errorCode);
                    RecordBatch batch = batches.get(tp);
                    //调用completeBatch()方法处理
                    completeBatch(batch, error, partResp.baseOffset, partResp.timestamp, correlationId, now);
                }
                this.sensors.recordLatency(response.request().request().destination(), response.requestLatencyMs());
                this.sensors.recordThrottleTime(response.request().request().destination(),
                        produceResponse.getThrottleTime());
            } else {
                //不需要响应的请求,直接调用completeBatch()方法处理
                // this is the acks = 0 case, just complete all requests
                for (RecordBatch batch : batches.values())
                    completeBatch(batch, Errors.NONE, -1L, Record.NO_TIMESTAMP, correlationId, now);
            }
        }
    }

    /**
     * Complete or retry the given batch of records.
     *
     * @param batch         The record batch
     * @param error         The error (or null if none)
     * @param baseOffset    The base offset assigned to the records if successful
     * @param timestamp     The timestamp returned by the broker for this batch
     * @param correlationId The correlation id for the request
     * @param now           The current POSIX time stamp in milliseconds
     */
    private void completeBatch(RecordBatch batch, Errors error, long baseOffset, long timestamp, long correlationId, long now) {
        if (error != Errors.NONE && canRetry(batch, error)) {
            //对于可重试的RecordBatch,则重新添加到RecordAccumulator中,等待发送
            // retry
            log.warn("Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                    correlationId,
                    batch.topicPartition,
                    this.retries - batch.attempts - 1,
                    error);
            this.accumulator.reenqueue(batch, now);
            this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
        } else {
            //不能重试的话,会将RecordBatch都标记为"异常完成"
            RuntimeException exception;
            //获取异常
            if (error == Errors.TOPIC_AUTHORIZATION_FAILED)
                exception = new TopicAuthorizationException(batch.topicPartition.topic());
            else
                exception = error.exception();
            // tell the user the result of their request
            //调用RecordBatch.done方法,调用消息的回调函数
            batch.done(baseOffset, timestamp, exception);
            //释放空间
            this.accumulator.deallocate(batch);
            if (error != Errors.NONE)
                this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);
        }
        if (error.exception() instanceof InvalidMetadataException)
            //标识需要更新Metadata中记录的集群元数据
            metadata.requestUpdate();
        // Unmute the completed partition.
        if (guaranteeMessageOrder)
            this.accumulator.unmutePartition(batch.topicPartition);
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed
     */
    private boolean canRetry(RecordBatch batch, Errors error) {
        return batch.attempts < this.retries && error.exception() instanceof RetriableException;
    }

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     * Sender.createProduceRequests()方法的主要功能：
     * 将待发送的消封装成ClientRequest,不管一个Node对应有多少个RecordBatch,也不管这些RecordBatch是发给几个分区的,
     * 每个Node至多生成一个ClientRequest对象,核心逻辑：
     * 1:将一个NodeId对应的RecordBatch集合,重新整理成为produceRecordsByPartition(Map<TopicPartition, ByteBuffer> )和recordsByPartition(Map<TopicPartition, RecordBatch>)两个集合
     * 2:创建RequestSend,RequestSend是真正执行网络I/O发送的对象,其格式符合Produce Request(Version:2)协议,其中有效的负载就是produceRecordsByPartition中的数据
     * 3:创建RequestCompletionHandler作为回调对象
     * 4:将RequestSend对象和RequestCompletionHandler对象封装进ClientRequest对象,并将其返回
     */
    private List<ClientRequest> createProduceRequests(Map<Integer, List<RecordBatch>> collated, long now) {
        List<ClientRequest> requests = new ArrayList<ClientRequest>(collated.size());
        for (Map.Entry<Integer, List<RecordBatch>> entry : collated.entrySet())
            //调用produceRequest()方法,将发往同一Node的RecordBatch封装成一个ClientRequest对象
            requests.add(produceRequest(now, entry.getKey(), acks, requestTimeout, entry.getValue()));
        return requests;
    }

    /**
     * Create a produce request from the given record batches
     */
    private ClientRequest produceRequest(long now, int destination, short acks, int timeout, List<RecordBatch> batches) {
        /**
         * produceRecordsByPartition与recordsByPartition的value是不一样的,一个是ByteBuffer,一个是RecordBatch
         */
        Map<TopicPartition, ByteBuffer> produceRecordsByPartition = new HashMap<TopicPartition, ByteBuffer>(batches.size());
        final Map<TopicPartition, RecordBatch> recordsByPartition = new HashMap<TopicPartition, RecordBatch>(batches.size());
        //步骤1:将RecordBatch按partition进行分类,整合成上面两个集合
        for (RecordBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            produceRecordsByPartition.put(tp, batch.records.buffer());
            recordsByPartition.put(tp, batch);
        }
        //步骤2:创建ProduceRequest对象和RequestSend对象
        ProduceRequest request = new ProduceRequest(acks, timeout, produceRecordsByPartition);
        RequestSend send = new RequestSend(Integer.toString(destination),
                this.client.nextRequestHeader(ApiKeys.PRODUCE),
                request.toStruct());
        //步骤3:创建RequestCompletionHandler回调对象,
        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                handleProduceResponse(response, recordsByPartition, time.milliseconds());
            }
        };
        //创建ClientRequest对象,注意第二个参数,根据生产者ask配置,决定请求是否需要获取响应时间
        return new ClientRequest(now, acks != 0, send, callback);
    }

    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.client.wakeup();
    }

    /**
     * A collection of sensors for the sender
     */
    private class SenderMetrics {

        private final Metrics metrics;
        public final Sensor retrySensor;
        public final Sensor errorSensor;
        public final Sensor queueTimeSensor;
        public final Sensor requestTimeSensor;
        public final Sensor recordsPerRequestSensor;
        public final Sensor batchSizeSensor;
        public final Sensor compressionRateSensor;
        public final Sensor maxRecordSizeSensor;
        public final Sensor produceThrottleTimeSensor;

        public SenderMetrics(Metrics metrics) {
            this.metrics = metrics;
            String metricGrpName = "producer-metrics";

            this.batchSizeSensor = metrics.sensor("batch-size");
            MetricName m = metrics.metricName("batch-size-avg", metricGrpName, "The average number of bytes sent per partition per-request.");
            this.batchSizeSensor.add(m, new Avg());
            m = metrics.metricName("batch-size-max", metricGrpName, "The max number of bytes sent per partition per-request.");
            this.batchSizeSensor.add(m, new Max());

            this.compressionRateSensor = metrics.sensor("compression-rate");
            m = metrics.metricName("compression-rate-avg", metricGrpName, "The average compression rate of record batches.");
            this.compressionRateSensor.add(m, new Avg());

            this.queueTimeSensor = metrics.sensor("queue-time");
            m = metrics.metricName("record-queue-time-avg", metricGrpName, "The average time in ms record batches spent in the record accumulator.");
            this.queueTimeSensor.add(m, new Avg());
            m = metrics.metricName("record-queue-time-max", metricGrpName, "The maximum time in ms record batches spent in the record accumulator.");
            this.queueTimeSensor.add(m, new Max());

            this.requestTimeSensor = metrics.sensor("request-time");
            m = metrics.metricName("request-latency-avg", metricGrpName, "The average request latency in ms");
            this.requestTimeSensor.add(m, new Avg());
            m = metrics.metricName("request-latency-max", metricGrpName, "The maximum request latency in ms");
            this.requestTimeSensor.add(m, new Max());

            this.produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
            m = metrics.metricName("produce-throttle-time-avg", metricGrpName, "The average throttle time in ms");
            this.produceThrottleTimeSensor.add(m, new Avg());
            m = metrics.metricName("produce-throttle-time-max", metricGrpName, "The maximum throttle time in ms");
            this.produceThrottleTimeSensor.add(m, new Max());

            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            m = metrics.metricName("record-send-rate", metricGrpName, "The average number of records sent per second.");
            this.recordsPerRequestSensor.add(m, new Rate());
            m = metrics.metricName("records-per-request-avg", metricGrpName, "The average number of records per request.");
            this.recordsPerRequestSensor.add(m, new Avg());

            this.retrySensor = metrics.sensor("record-retries");
            m = metrics.metricName("record-retry-rate", metricGrpName, "The average per-second number of retried record sends");
            this.retrySensor.add(m, new Rate());

            this.errorSensor = metrics.sensor("errors");
            m = metrics.metricName("record-error-rate", metricGrpName, "The average per-second number of record sends that resulted in errors");
            this.errorSensor.add(m, new Rate());

            this.maxRecordSizeSensor = metrics.sensor("record-size-max");
            m = metrics.metricName("record-size-max", metricGrpName, "The maximum record size");
            this.maxRecordSizeSensor.add(m, new Max());
            m = metrics.metricName("record-size-avg", metricGrpName, "The average record size");
            this.maxRecordSizeSensor.add(m, new Avg());

            m = metrics.metricName("requests-in-flight", metricGrpName, "The current number of in-flight requests awaiting a response.");
            this.metrics.addMetric(m, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return client.inFlightRequestCount();
                }
            });
            m = metrics.metricName("metadata-age", metricGrpName, "The age in seconds of the current producer metadata being used.");
            metrics.addMetric(m, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return (now - metadata.lastSuccessfulUpdate()) / 1000.0;
                }
            });
        }

        public void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                Map<String, String> metricTags = new LinkedHashMap<String, String>();
                metricTags.put("topic", topic);
                String metricGrpName = "producer-topic-metrics";

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName m = this.metrics.metricName("record-send-rate", metricGrpName, metricTags);
                topicRecordCount.add(m, new Rate());

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                m = this.metrics.metricName("byte-rate", metricGrpName, metricTags);
                topicByteRate.add(m, new Rate());

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                m = this.metrics.metricName("compression-rate", metricGrpName, metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                m = this.metrics.metricName("record-retry-rate", metricGrpName, metricTags);
                topicRetrySensor.add(m, new Rate());

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                m = this.metrics.metricName("record-error-rate", metricGrpName, metricTags);
                topicErrorSensor.add(m, new Rate());
            }
        }

        public void updateProduceRequestMetrics(Map<Integer, List<RecordBatch>> batches) {
            long now = time.milliseconds();
            for (List<RecordBatch> nodeBatch : batches.values()) {
                int records = 0;
                for (RecordBatch batch : nodeBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topicPartition.topic();
                    maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Utils.notNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Utils.notNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.records.sizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Utils.notNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.records.compressionRate());

                    // global metrics
                    this.batchSizeSensor.record(batch.records.sizeInBytes(), now);
                    this.queueTimeSensor.record(batch.drainedMs - batch.createdMs, now);
                    this.compressionRateSensor.record(batch.records.compressionRate());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }

        public void recordThrottleTime(String node, long throttleTimeMs) {
            this.produceThrottleTimeSensor.record(throttleTimeMs, time.milliseconds());
        }

    }

}
