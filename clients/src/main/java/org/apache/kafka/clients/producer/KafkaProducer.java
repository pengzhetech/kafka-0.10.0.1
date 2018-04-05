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
package org.apache.kafka.clients.producer;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Kafka client that publishes records to the Kafka cluster.
 * <p>
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("retries", 0);
 * props.put("batch.size", 16384);
 * props.put("linger.ms", 1);
 * props.put("buffer.memory", 33554432);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for(int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * <p>
 * send()方法是异步的!,将消息发送到RecordAccumulator就会返回
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * <p>
 * 生产者配置参数
 * <p>
 * 1:acks  这个配置是控制消息是否完成的标准
 * 必须有多少个分区副本收到消息,生产者才认为消息时写入成功的,这个参数对消息丢失的可能性有重要影响
 * acks=0
 * 生产者在成功写入消息之前不会有任何来自服务器的响应,也就是说如果当中任何环节出现问题,导致服务器没有收到消息
 * 那么生产者就无从得知,也就是消息丢失了
 * 不过因为生产者不需要等待服务器的响应,所以他能以网络能支持的最大速度发送消息,从而达到很高的吞吐率
 * acks=1
 * 只要集群的leader节点收到此消息,生产者就会收到一个来自服务器的成功响应
 * 如果消息无法到达leader节点(比如leader崩溃,新的leader节点还没有被选举出来)生产者会收到一个错误响应
 * 为了避免消息丢失,生产者会重发消息,不过如果一个没有收到消息的节点成为新leader,消息还是会丢失
 * 这时候生产者的吞吐量率取决于使用的是同步发送还是异步发送
 * 如果让客户端等待服务器的响应(通过调用Future对象的get()方法),显然会增加延迟(在网络上传输一个来回的延迟)
 * 如果客户端使用的回调,延迟问题就会得到缓解,不过吞吐量还是会受到发送消息数量的限制(比如生产者在收到服务器之前可以发送多少个消息)
 * acks=all
 * 只有当所有参与复制的节点全部收到消息时,生产者才会收到一个来自服务器的成功响应
 * 这种模式是最安全的,它可以保证不止一个服务器收到消息,就算有服务器发生崩溃,整个集群任然可以运行
 * 不过他的延迟是最高的,因为我们要等待的不只一个服务器节点接收消息然后响应(服务端接收的消息需要落盘)
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The "all" setting
 * we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
 * <p>
 * <p>
 * 2:retries
 * <p>
 * 生产者从服务器收到的错误有可能是临时性错误(比如分区找不到leader),在这种情况下,retries参数的值决定了生产者可以重发消息的次数
 * 默认情况下,生产者会在每次重试之间等待100ms,不过可以通过retry.backoff.ms参数来改变这个时间间隔
 * 建议在设置重试次数和重试时间间隔之前,先测试一下恢复一个崩溃节点需要多少时间(比如所有分区选举出leader需要多长时间)
 * 让总的重试时间比kafka集群从崩溃中恢复的时间长,否则生产者会过早的放弃重试
 * 不过有些错误不是临时性的,没办法通过重试来解决(比如"消息太大错误")
 * 一般情况下,因为生产者会自动进行重试,所有就没必要再代码逻辑里处理那些可重试的错误,只需处理那些不可重试的错误或重试次数超过上限的情况
 * 默认值是0,即不重试
 * kafka自带的客户端设置的发送失败重试次数是3次
 * <p>
 * 如果发送失败,会自动重发,可能会造成消息重复
 * If the request fails, the producer can automatically retry, though since we have specified <code>retries</code>
 * as 0 it won't. Enabling retries also opens up the possibility of duplicates (see the documentation on
 * <a href="http://kafka.apache.org/documentation.html#semantics">message delivery semantics</a> for details).
 * <p>
 * 生产者为每个partition缓冲消息
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * <p>
 * <p>
 * <p>
 * 3:linger.ms
 * 该参数指定了生产者在发送批次之前等待更多的消息加入批次的时间
 * kafkaProducer会在批次填满或linger.ms到达上线时就会把批次发送出去,默认情况下,只要有可用的线程,生产者就会把消息发送出去
 * 即使批次里只有一条消息,把linger.ms设置成比0大的数,让生产者在发送批次之前等待一会儿,是更多的消息加入到这个批次
 * 虽然这样会增加延迟,但也会提升吞吐率(因为一次可以发送更多消息,每个消息的开销就变小了)
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous(类似) to Nagle's algorithm(算法) in TCP. For example, in the code snippet(片段) above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
 * would add 1 millisecond of latency(潜伏) to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code> so under heavy load
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * <p>
 * 4:buffer.memory参数用来设置生产者缓冲区的大小
 * 生成者用他来缓冲要发送到服务器的消息,如果应用程序发送消息的速度超过发送到服务器的速度,会导致生产者空间不足,这个时候
 * send()方法要么被阻塞,要么抛出异常,这取决于如何设置max.block.ms(即发送消息时最大的阻塞时间)
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
 * a TimeoutException.
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the user provides with
 * their <code>ProducerRecord</code> into bytes. You can use the included {@link org.apache.kafka.common.serialization.ByteArraySerializer} or
 * {@link org.apache.kafka.common.serialization.StringSerializer} for simple string or byte types.
 * 5:batch.size
 * 当有多个消息需要被发送到同一个分区时,生产者会把他们放到同一个批次里
 * 该参数指定了一个批次可以使用的内存大小(按照字节数计算,不是消息个数)
 * 当批次被填满时,批次里的所有消息会被发送出去,不过生产者也并不一定会等到批次填满才发送
 * 半满的批次,甚至只包含一个消息的批次也有可能被发送
 * 所以就算把批次设置的很,也不会造成延迟,只是会占用更多的内存而已,
 * 但是如果设置的太小,因为生产者需要更频繁的发送消息,会增加一些额外的开销
 * 6:compression.type
 * 默认情况下,消息发送时不会被压缩,该参数可以设置为snappy,gzip,lz4,他指定了消息发送到broker之前使用哪一种压缩算法进行压缩
 * snappy算法由Google发明,它占用较小的cpu,却能提供较好的性能和相当可观的压缩比,如果比较关注性能和网络带宽,可以使用
 * gzip算法一般会占用较多的cpu资源,但会提供更高的压缩比,如果网络带宽资源有限,可以考虑使用这种算法
 * 使用压缩可以降低网络传输开销和存储开销,这往往是kafka发送消息的瓶颈
 * 7:max.in.flight.requests.per.connection
 * 该参数指定了生产者在收到服务器响应之前可以发送多少个消息
 * 它的值越高,就会占用越多的内存，不过也会提高吞吐量,把他设置为1,可以保证消息是按照发送的顺序写入服务器的，即使发生了重试
 * 8:max.request.size
 * 该参数用于控制生产者发送的请求大小(最大的字节数),它可以指能发送的单个消息的最大值,也可以指单个请求里所有消息总的大小
 * 另外,broker对可接受的消息的最大值也有自己的限制(message.max.byte)两边最好匹配防止消息被broker拒收
 * 9:max.block.ms
 * 该参数指定了在调用send()方法或使用partitionsFor()方法获取元数据是生产者的阻塞时间,当生产者的发送缓冲区已满,或者没有可用的元数据时
 * 这些方法就会阻塞,在阻塞时间达到max.block.ms时生产者会抛出异常
 * 10:timeout.ms  request.timeout.ms  metadata.fetch.timeout.ms
 * request.timeout.ms指定了生产者在发送数据事等待服务器返回响应的时间
 * metadata.fetch.timeout.ms指定了生产者在获取元数据时(比如目标分区的leader是谁)时等待服务器返回响应的时间,
 * 如果等待超时那么生产者要么重新发送数据,要么返回一个错误(抛出异常或执行回调)
 * timeout.ms 指定了broker等待同步副本返回消息确认的时间与asks配置相匹配--如果在指定时间内没有收到同步副本的确认,那么broker就会返回一个错误
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    //ClientId生成器,如果没有明确指定client的id,则使用此字段生成一个ID
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.producer";
    //此生产者的唯一标识,可以是任意的字符串,服务器用他来识别消息的来源,还可以在日志和配额指标里
    private String clientId;
    //分区选择器,根据一定的策略,将消息路由到合适的分区
    private final Partitioner partitioner;
    //消息的最大长度,这个长度包含了消息头,序列化后的key,和序列化后的value的长度
    private final int maxRequestSize;
    private final long totalMemorySize; //发送单个消息的缓冲区大小
    private final Metadata metadata;  //整个kafka集群的元数据
    private final RecordAccumulator accumulator; //用于收集并缓存消息,等待Sender线程发送
    private final Sender sender; //发送消息的Sender任务,实现了Runnable接口,在ioThread的线程中执行
    private final Metrics metrics;
    private final Thread ioThread;//执行Sender任务发送消息的线程,成为Sender线程
    //压缩算法,可选项有none,gzip,snappy,lz4.这是针对RecordAccumulator中多条消息进行的压缩,消息越多,压缩效果越明显
    private final CompressionType compressionType;
    private final Sensor errors;
    private final Time time;
    private final Serializer<K> keySerializer; //key的序列化器
    private final Serializer<V> valueSerializer; //value的序列化器
    private final ProducerConfig producerConfig;  //配置对象,使用反射初始化KafkaProducer配置的相关对象
    private final long maxBlockTimeMs;//等待更新kafka集群元数据的最大时长
    private final int requestTimeoutMs; //消息的超时时间,也就是从消息发送到收到ACK相应的最长时长
    //ProducerInterceptors集合,ProducerInterceptor可以在消息发送之前对消息进行拦截或修改,
    // 也可优先于用户的Callback,对ACK相应进行预处理
    private final ProducerInterceptors<K, V> interceptors;

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     *
     * @param configs The producer configs
     */
    public KafkaProducer(Map<String, Object> configs) {
        this(new ProducerConfig(configs), null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     *
     * @param configs         The producer configs
     * @param keySerializer   The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                        called in the producer when the serializer is passed in directly.
     * @param valueSerializer The serializer for value that implements {@link Serializer}. The configure() method won't
     *                        be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
                keySerializer, valueSerializer);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     *
     * @param properties The producer configs
     */
    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties), null, null);
    }

    /**
     * kafkaProducer在实例化时首先会加载和解析生产者相关的配置信息并封装成ProducerConfig对象
     * 然后根据配置项主要完成以下对象或数据结构的实例化
     * <p>
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     *
     * @param properties      The producer configs
     * @param keySerializer   The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                        called in the producer when the serializer is passed in directly.
     * @param valueSerializer The serializer for value that implements {@link Serializer}. The configure() method won't
     *                        be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
                keySerializer, valueSerializer);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    private KafkaProducer(ProducerConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        try {
            log.trace("Starting the Kafka producer");
            Map<String, Object> userProvidedConfigs = config.originals();
            this.producerConfig = config;
            this.time = new SystemTime();
            /**
             * 1:从配置项中解析出clientId，客户端指定该配置项的值以便追踪程序运行情况,在同一个进程内，当有多个KafkaProducer时
             * 若没有配置client.id则clientId以前缀 "producer-"后加一个从1递增的整数
             */
            clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
            if (clientId.length() <= 0)
                clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
            /**
             * 2:根据配置项创建和主责用于KafkaMetrics指标收集的相关对象,用于对Kafka集群相关指标的追踪
             */
            Map<String, String> metricTags = new LinkedHashMap<String, String>();
            metricTags.put("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .tags(metricTags);
            List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class);
            reporters.add(new JmxReporter(JMX_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, time);
            //通过反射机制实例化配置的Partitioner类,KeySerializer,ValueSerializer类
            /**
             * 3:实例化分区器
             * 分区器用于指定分区,客户端可以通过实现Partitioner接口自定义分配分区的规则.若用户没有自定义分区器,则在KafkaProducer实例化时
             * 会使用默认的DefaultPartitioner 该分区器分配分区的规则是:
             * 若消息自定了key,则对key取hash值，然后与可用的分区总数求模
             * 若没有指定key,则通过一个随机数与可用的总分区取模
             */
            this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
            /**
             *
             */
            //获取retry.backoff.ms(默认是100ms)
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            //创建kafka集群的元数据
            /**
             * 4:实例化用于消息发送相关元数据信息的Metadata对象
             * Metadata对象被客户端线程共享 所以必须是线程安全的
             * Metadata的主要数据结构由两部分组成 一类是用户控制Metadata进程更新操作的相关配置信息
             * 另一类就是集群信息Cluster，Cluster保存了进群中所有topic以及所有topic对应的partition信息列表
             * 可用的partition,集群的broker信息等
             * 在kafkaProducer实例化过程中会根据指定的broker列表初始化Cluster，并第一次更新Metadata
             */
            this.metadata = new Metadata(retryBackoffMs, config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG));
            //获取max.request.size大小
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            //获取buffer.memory大小
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            //消息压缩策略(默认none)
            this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            //检查用户配置,提示两个在0.9版本可能会移除的配置
            /* check for user defined settings.
             * If the BLOCK_ON_BUFFER_FULL is set to true,we do not honor METADATA_FETCH_TIMEOUT_CONFIG.
             * This should be removed with release 0.9 when the deprecated configs are removed.
             */
            if (userProvidedConfigs.containsKey(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG)) {
                log.warn(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG + " config is deprecated and will be removed soon. " +
                        "Please use " + ProducerConfig.MAX_BLOCK_MS_CONFIG);
                boolean blockOnBufferFull = config.getBoolean(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG);
                if (blockOnBufferFull) {
                    this.maxBlockTimeMs = Long.MAX_VALUE;
                } else if (userProvidedConfigs.containsKey(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG)) {
                    log.warn(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG + " config is deprecated and will be removed soon. " +
                            "Please use " + ProducerConfig.MAX_BLOCK_MS_CONFIG);
                    this.maxBlockTimeMs = config.getLong(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
                } else {
                    this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
                }
            } else if (userProvidedConfigs.containsKey(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG)) {
                log.warn(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG + " config is deprecated and will be removed soon. " +
                        "Please use " + ProducerConfig.MAX_BLOCK_MS_CONFIG);
                this.maxBlockTimeMs = config.getLong(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
            } else {
                this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            }
            //检查用户配置,提示两个在0.9版本可能会移除的配置
            /* check for user defined settings.
             * If the TIME_OUT config is set use that for request timeout.
             * This should be removed with release 0.9
             */
            if (userProvidedConfigs.containsKey(ProducerConfig.TIMEOUT_CONFIG)) {
                log.warn(ProducerConfig.TIMEOUT_CONFIG + " config is deprecated and will be removed soon. Please use " +
                        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
                this.requestTimeoutMs = config.getInt(ProducerConfig.TIMEOUT_CONFIG);
            } else {
                this.requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            }
            //创建RecordAccumulator
            /**
             * 实例化用户存储消息的RecordAccumulator
             * RecordAccumulator的作用类似于一个队列,这里成为消息累加器
             * KafkaProducer发送的消息都会被追加到消息累加器的一个双端队列Deque中
             * 在消息累加器的内部每个主题的每个分区TopicPartition对应一个双端队列
             * 队列中的元素是RecordBatch，RecordBatch是由同一个主题发往同一个分区的多条消息Record组成
             * 并将结果以每个TopicPartition作为key,该TopicPartition所对应的双端队列作为Value保存到一个ConcurrentMap类型的bathes中
             * 采用双端队列是为了当消息发送失败需要重试时,将消息优先插入到队列的头部,而最新的消息总是插入队列的尾部
             * 只有当消息重试发送时才在队列头部插入，发送消息是从头部获取RecordBatch 这样就实现了对发送失败的消息进行重新发送
             * 但是双端队列只是指定了RecordBatch的顺序存储方法 并未定义存储空间大小
             * 在消息累加器中有一个BufferPool缓存数据结构 用于存储消息Record 在KafkaProducer初始化时需要根据指定的BufferPool大小初始化一个BufferPool,引用名为free
             */
            this.accumulator = new RecordAccumulator(config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
                    this.totalMemorySize,
                    this.compressionType,
                    config.getLong(ProducerConfig.LINGER_MS_CONFIG),
                    retryBackoffMs,
                    metrics,
                    time);
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

            /**
             * //更新kafka集群的元数据
             */
            this.metadata.update(Cluster.bootstrap(addresses), time.milliseconds());
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config.values());
            //创建NetworkClient,这是KafkaProducer的网络I/O核心,
            NetworkClient client = new NetworkClient(
                    new Selector(config.getLong(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), this.metrics, time, "producer", channelBuilder),
                    this.metadata,
                    clientId,
                    config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
                    config.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),
                    this.requestTimeoutMs, time);
            this.sender = new Sender(client,
                    this.metadata,
                    this.accumulator,
                    config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION) == 1,
                    config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                    (short) parseAcks(config.getString(ProducerConfig.ACKS_CONFIG)),
                    config.getInt(ProducerConfig.RETRIES_CONFIG),
                    this.metrics,
                    new SystemTime(),
                    clientId,
                    this.requestTimeoutMs);
            String ioThreadName = "kafka-producer-network-thread" + (clientId.length() > 0 ? " | " + clientId : "");
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            //启动Sender对应的线程
            this.ioThread.start();

            this.errors = this.metrics.sensor("errors");
            /**
             * 4:实例化消息key和value进行序列化操作的Serializer
             * Kafka实现了7中基本类型的Serializer,如BytesSerializer,LongSerializer
             * 用户也可以自定义序列化方式(kafka推荐使用Avro)，默认key和value使用ByteArraySerializer
             */
            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        Serializer.class);
                this.keySerializer.configure(config.originals(), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = keySerializer;
            }
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        Serializer.class);
                this.valueSerializer.configure(config.originals(), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = valueSerializer;
            }

            // load interceptors and make sure they get clientId
            userProvidedConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            /**
             * 5:根据配置实例化一组拦截器(ProducerInterceptor),用户可以指定多个拦截器
             * 如果我们希望在消息发送前，消息发送到broker并ack,，消息还未到达broker而失败或调用send()方法失败这几种情景下进行相应处理
             * 就可以通过自定义拦截器实现该接口中相应的方法.多个拦截器会被顺序执行
             */
            List<ProducerInterceptor<K, V>> interceptorList = (List) (new ProducerConfig(userProvidedConfigs)).getConfiguredInstances(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ProducerInterceptor.class);
            this.interceptors = interceptorList.isEmpty() ? null : new ProducerInterceptors<>(interceptorList);

            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId);
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed
            // this is to prevent resource leak. see KAFKA-2121
            close(0, TimeUnit.MILLISECONDS, true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    private static int parseAcks(String acksString) {
        try {
            return acksString.trim().equalsIgnoreCase("all") ? -1 : Integer.parseInt(acksString.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
     * See {@link #send(ProducerRecord, Callback)} for details.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * <p>
     * The send is asynchronous and this method will return immediately once the record has been stored in the buffer of
     * records waiting to be sent. This allows sending many records in parallel without blocking to wait for the
     * response after each one.
     * <p>
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to, the offset
     * it was assigned and the timestamp of the record. If
     * {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime} is used by the topic, the timestamp
     * will be the user provided timestamp or the record send time if the user did not specify a timestamp for the
     * record. If {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime} is used for the
     * topic, the timestamp will be the Kafka broker local time when the message is appended.
     * <p>
     * Since the send call is asynchronous it returns a {@link java.util.concurrent.Future Future} for the
     * {@link RecordMetadata} that will be assigned to this record. Invoking {@link java.util.concurrent.Future#get()
     * get()} on this future will block until the associated request completes and then return the metadata for the record
     * or throw any exception that occurred while sending the record.
     * <p>
     * If you want to simulate a simple blocking call you can call the <code>get()</code> method immediately:
     *
     * <pre>
     * {@code
     * byte[] key = "key".getBytes();
     * byte[] value = "value".getBytes();
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value)
     * producer.send(record).get();
     * }</pre>
     * <p>
     * Fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     *
     * <pre>
     * {@code
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("the-topic", key, value);
     * producer.send(myRecord,
     *               new Callback() {
     *                   public void onCompletion(RecordMetadata metadata, Exception e) {
     *                       if(e != null)
     *                           e.printStackTrace();
     *                       System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                   }
     *               });
     * }
     * </pre>
     * <p>
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     *
     * <pre>
     * {@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * }
     * </pre>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     *
     * @param record   The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *                 indicates no callback)
     * @throws InterruptException     If the thread is interrupted while blocked
     * @throws SerializationException If the key or value are not valid objects given the configured serializers
     * @throws TimeoutException       if the time taken for fetching metadata or allocating memory for the record has surpassed <code>max.block.ms</code>.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // intercept the record, which can be potentially modified; this method does not throw exceptions
        ProducerRecord<K, V> interceptedRecord = this.interceptors == null ? record : this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }

    /**
     * Implementation of asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
     * See {@link #send(ProducerRecord, Callback)} for details.
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            // first make sure the metadata for the topic is available
            long waitedOnMetadataMs = waitOnMetadata(record.topic(), this.maxBlockTimeMs);
            long remainingWaitMs = Math.max(0, this.maxBlockTimeMs - waitedOnMetadataMs);
            byte[] serializedKey;
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer");
            }
            byte[] serializedValue;
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer");
            }
            int partition = partition(record, serializedKey, serializedValue, metadata.fetch());
            int serializedSize = Records.LOG_OVERHEAD + Record.recordSize(serializedKey, serializedValue);
            ensureValidRecordSize(serializedSize);
            tp = new TopicPartition(record.topic(), partition);
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
            log.trace("Sending record {} with callback {} to topic {} partition {}", record, callback, record.topic(), partition);
            // producer callback will make sure to call both 'callback' and interceptor callback
            Callback interceptCallback = this.interceptors == null ? callback : new InterceptorCallback<>(callback, this.interceptors, tp);
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey, serializedValue, interceptCallback, remainingWaitMs);
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), partition);
                this.sender.wakeup();
            }
            return result.future;
            // handling exceptions and record the errors;
            // for API exceptions return them in the future,
            // for other exceptions throw directly
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            if (callback != null)
                callback.onCompletion(null, e);
            this.errors.record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw new InterruptException(e);
        } catch (BufferExhaustedException e) {
            this.errors.record();
            this.metrics.sensor("buffer-exhausted-records").record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (KafkaException e) {
            this.errors.record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw e;
        }
    }

    /**
     * Wait for cluster metadata including partitions for the given topic to be available.
     * 此方法负责触发Kafka集群元数据的更新,并阻塞主线程等待更新完毕,它的主要步骤:
     * * 1):检测Metadata中是否包含制动Topic的元数据,若不包含
     * * 则将topic添加到topics集合中,下次更新时会从服务端获取指定topic的元数据
     * * 2):尝试获取Topic中分区的详细信息,失败后会调用requestUpdate()方法这只MetaData.needUpdate字段,并得到当前元数据版本号
     * * 3):唤醒Sender线程,由Sender线程更新MetaData中保存的Kafka集群元数据
     * * 4):主线程调用awaitUpdate()方法,等待Sender线程完成更新
     * * 5):从Metadata中获取指定Topic分区的详细信息(即PartitionInfo集合),若失败则回到步骤2继续尝试,若等待时间超时,则抛出异常
     *
     * @param topic     The topic we want metadata for
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     * @return The amount of time we waited in ms
     */
    private long waitOnMetadata(String topic, long maxWaitMs) throws InterruptedException {
        // add topic to metadata topic list if it is not there already.
        //检测topic集合中是否包含指定的topic
        if (!this.metadata.containsTopic(topic))
            this.metadata.add(topic);
        //成功获取分区的详细信息
        if (metadata.fetch().partitionsForTopic(topic) != null)
            return 0;

        long begin = time.milliseconds();
        long remainingWaitMs = maxWaitMs;
        while (metadata.fetch().partitionsForTopic(topic) == null) {
            log.trace("Requesting metadata update for topic {}.", topic);
            //设置needUpdate,获取当前元数据的版本号
            int version = metadata.requestUpdate();
            //唤醒Sender线程
            sender.wakeup();
            //阻塞等待元数据更新完成
            metadata.awaitUpdate(version, remainingWaitMs);
            long elapsed = time.milliseconds() - begin;
            //检测超时时间
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            //检测权限
            if (metadata.fetch().unauthorizedTopics().contains(topic))
                throw new TopicAuthorizationException(topic);
            remainingWaitMs = maxWaitMs - elapsed;
        }
        return time.milliseconds() - begin;
    }

    /**
     * Validate that the record size isn't too large
     */
    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the maximum request size you have configured with the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG +
                    " configuration.");
        if (size > this.totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed (e.g. <code>Future.isDone() == true</code>).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commit();
     * }
     * </pre>
     * <p>
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void flush() {
        log.trace("Flushing accumulated records in producer.");
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
    }

    /**
     * Get the partition metadata for the give topic. This can be used for custom partitioning.
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        try {
            waitOnMetadata(topic, this.maxBlockTimeMs);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
        return this.metadata.fetch().partitionsForTopic(topic);
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     *
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(0, TimeUnit.MILLISECONDS)</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout  The maximum time to wait for producer to complete any pending requests. The value should be
     *                 non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @param timeUnit The time unit for the <code>timeout</code>
     * @throws InterruptException       If the thread is interrupted while blocked
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     */
    @Override
    public void close(long timeout, TimeUnit timeUnit) {
        close(timeout, timeUnit, false);
    }

    private void close(long timeout, TimeUnit timeUnit, boolean swallowException) {
        if (timeout < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeUnit.toMillis(timeout));
        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeout > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.", timeout);
            } else {
                // Try to close gracefully.
                if (this.sender != null)
                    this.sender.initiateClose();
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeUnit.toMillis(timeout));
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, t);
                        log.error("Interrupted while joining ioThread", t);
                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                    "within timeout {} ms.", timeout);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, e);
                }
            }
        }

        ClientUtils.closeQuietly(interceptors, "producer interceptors", firstException);
        ClientUtils.closeQuietly(metrics, "producer metrics", firstException);
        ClientUtils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        ClientUtils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId);
        log.debug("The Kafka producer has closed.");
        if (firstException.get() != null && !swallowException)
            throw new KafkaException("Failed to close kafka producer", firstException.get());
    }

    /**
     * computes partition for given record.
     * if the record has partition returns the value otherwise
     * calls configured partitioner class to compute the partition.
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        if (partition != null) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(record.topic());
            int lastPartition = partitions.size() - 1;
            // they have given us a partition, use it
            if (partition < 0 || partition > lastPartition) {
                throw new IllegalArgumentException(String.format("Invalid partition given with record: %d is not in the range [0...%d].", partition, lastPartition));
            }
            return partition;
        }
        return this.partitioner.partition(record.topic(), record.key(), serializedKey, record.value(), serializedValue,
                cluster);
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

    /**
     * A callback called when producer request is complete. It in turn calls user-supplied callback (if given) and
     * notifies producer interceptors about the request completion.
     */
    private static class InterceptorCallback<K, V> implements Callback {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final TopicPartition tp;

        public InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors,
                                   TopicPartition tp) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            this.tp = tp;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (this.interceptors != null) {
                if (metadata == null) {
                    this.interceptors.onAcknowledgement(new RecordMetadata(tp, -1, -1, Record.NO_TIMESTAMP, -1, -1, -1),
                            exception);
                } else {
                    this.interceptors.onAcknowledgement(metadata, exception);
                }
            }
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }
    }
}
