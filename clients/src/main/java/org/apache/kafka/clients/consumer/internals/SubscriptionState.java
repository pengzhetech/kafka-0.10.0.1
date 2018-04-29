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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * KafkaConsumer从Kafka拉取消息时发送的请求是FetchRequest,在其中需要指定消费者希望拉取消息的Offset,为了消费者快速获取这个值
 * KafkaConsumer使用SubscriptionState来追踪TopicPartition和offset对应关系
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Collection)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 * <p>
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seek(TopicPartition, long)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 * <p>
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 * <p>
 * This class also maintains a cache of the latest commit position for each of the assigned
 * partitions. This is updated through {@link #committed(TopicPartition, OffsetAndMetadata)} and can be used
 * to set the initial fetch position (e.g. {@link Fetcher#resetOffset(TopicPartition)}.
 */
public class SubscriptionState {

    private enum SubscriptionType {
        /**
         * SubscriptionState.SubscriptionType的初始值
         */
        NONE,
        /**
         * 根据指定的Topic名字进行订阅,自动分配分区
         */
        AUTO_TOPICS,
        /**
         * 按照指定的正则表达式匹配Topic进行订阅,自动分配分区
         */
        AUTO_PATTERN,
        /**
         * 用户手动指定消费者消费的Topic及分区编号
         */
        USER_ASSIGNED
    }


    /**
     * SubscriptionType枚举类型,表示订阅模式
     */
    /* the type of subscription */
    private SubscriptionType subscriptionType;
    /**
     * 使用 AUTO_PATTERN模式时,是按照此字段记录的正则表达式对所有的Topic进行匹配
     * 对匹配符合的Topic进行订阅
     */
    /* the pattern user has requested */
    private Pattern subscribedPattern;
    /**
     * 使用AUTO_PATTERN,AUTO_TOPICS则使用此集合记录所有订阅的Topic,向subscription集合中添加数据只有
     * changeSubscription方法
     */
    /* the list of topics the user has requested */
    private final Set<String> subscription;
    /**
     * ConsumerGroup会选举出一个Leader,Leader使用该集合记录ConsumerGroup中所有订阅者订阅的Topic
     * 而其他Follower的集合中只保存了其自身的订阅的Topic
     */
    /* the list of topics the group has subscribed to (set only for the leader on join group completion) */
    private final Set<String> groupSubscription;
    /**
     * 如果使用USER_ASSIGNED模式,则此集合记录了分配给当前消费者的TopicPartition集合
     * SubscriptionType模式是互斥的,所有subscription与userAssignment集合也是互斥的
     */
    /* the list of partitions the user has requested */
    private final Set<TopicPartition> userAssignment;

    /**
     * 无论使用什么订阅模式,都用此集合记录每个TopicPartition的消费状态
     */
    /* the list of partitions currently assigned */
    private final Map<TopicPartition, TopicPartitionState> assignment;
    /**
     * 标记是否需要进行一次分区分配
     */
    /* do we need to request a partition assignment from the coordinator? */
    private boolean needsPartitionAssignment;
    /**
     * 标记是否需要从GroupCoordinator获取最近提交的offset
     * 当出现异步提交offset操作或是Reblance操作刚完成时会将其置位true
     * 成功获取最近提交offset之后会置位false
     */
    /* do we need to request the latest committed offsets from the coordinator? */
    private boolean needsFetchCommittedOffsets;
    /**
     * 默认的OffsetResetStrategy策略
     */
    /* Default offset reset strategy */
    private final OffsetResetStrategy defaultResetStrategy;
    /**
     * ConsumerRebalanceListener类型,用于监听分区分配操作
     */
    /* Listener to be invoked when assignment changes */
    private ConsumerRebalanceListener listener;

    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     *
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        //如果是NONE,则可以指定其他模式
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
            //如果已经指定了其他模式,则会报错
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
    }

    public SubscriptionState(OffsetResetStrategy defaultResetStrategy) {
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = new HashSet<>();
        this.userAssignment = new HashSet<>();
        this.assignment = new HashMap<>();
        this.groupSubscription = new HashSet<>();
        this.needsPartitionAssignment = false;
        this.needsFetchCommittedOffsets = true; // initialize to true for the consumers to fetch offset upon starting up
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        /**
         * 用户未使用ConsumerRebalanceListener时,默认使用NoConsumerRebalanceListener,所有方法都是空实现
         */
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        //使用AUTO_TOPICS模式
        setSubscriptionType(SubscriptionType.AUTO_TOPICS);

        this.listener = listener;

        changeSubscription(topics);
    }

    public void changeSubscription(Collection<String> topicsToSubscribe) {
        //订阅的Topic有变化
        if (!this.subscription.equals(new HashSet<>(topicsToSubscribe))) {
            //清空subscription集合
            this.subscription.clear();
            //添加订阅的Topic
            this.subscription.addAll(topicsToSubscribe);
            //将消费者自身订阅的Topic添加到groupSubscription集合
            this.groupSubscription.addAll(topicsToSubscribe);
            //将needsPartitionAssignment设置为true是因为消费者订阅的Topic发生了变化
            this.needsPartitionAssignment = true;
            //同步assignment与subscription集合
            // Remove any assigned partitions which are no longer subscribed to
            for (Iterator<TopicPartition> it = assignment.keySet().iterator(); it.hasNext(); ) {
                TopicPartition tp = it.next();
                if (!subscription.contains(tp.topic()))
                    it.remove();
            }
        }
    }

    /**
     * Add topics to the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     *
     * @param topics The topics to add to the group subscription
     */
    public void groupSubscribe(Collection<String> topics) {
        if (this.subscriptionType == SubscriptionType.USER_ASSIGNED)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        //在Leader收到JoinGroupResponse时调用,在JoinGroupResponse中包含了全部消费者订阅的Topic
        //在此时将Topic信息添加到groupSubscription集合
        this.groupSubscription.addAll(topics);
    }

    public void needReassignment() {
        //将groupSubscribe中其他消费者订阅的Topic删除,只留下自身订阅的Topic(即subscription集合)
        // 这是groupSubscription集合收缩的场景
        this.groupSubscription.retainAll(subscription);
        /**
         * 此处将needsPartitionAssignment置位true主要是因为在某些请求响应中出现了ILLEGAL_GENERATION等异常
         * 或者是订阅的Topic出现了分区数量的变化
         */
        this.needsPartitionAssignment = true;
    }

    /**
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     */
    public void assignFromUser(Collection<TopicPartition> partitions) {
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        this.userAssignment.clear();
        this.userAssignment.addAll(partitions);

        for (TopicPartition partition : partitions)
            if (!assignment.containsKey(partition))
                addAssignedPartition(partition);

        this.assignment.keySet().retainAll(this.userAssignment);
        //将needsPartitionAssignment设置为false是因为使用USER_ASSIGNED模式,所以不需要分区分配操作
        this.needsPartitionAssignment = false;
        this.needsFetchCommittedOffsets = true;
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator,
     * note this is different from {@link #assignFromUser(Collection)} which directly set the assignment from user inputs
     */
    public void assignFromSubscribed(Collection<TopicPartition> assignments) {
        for (TopicPartition tp : assignments)
            if (!this.subscription.contains(tp.topic()))
                throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic.");
        this.assignment.clear();
        for (TopicPartition tp : assignments)
            addAssignedPartition(tp);
        //成功得到SyncGroupResponse中的分区分配结果时的操作,此时Rebalance操作结束,将needsPartitionAssignment置位false
        this.needsPartitionAssignment = false;
    }

    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        setSubscriptionType(SubscriptionType.AUTO_PATTERN);

        this.listener = listener;
        this.subscribedPattern = pattern;
    }

    public boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public void unsubscribe() {
        this.subscription.clear();
        this.userAssignment.clear();
        this.assignment.clear();
        //将needsPartitionAssignment设置为true是因为消费者订阅的Topic发生了变化
        this.needsPartitionAssignment = true;
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }


    public Pattern getSubscribedPattern() {
        return this.subscribedPattern;
    }

    public Set<String> subscription() {
        return this.subscription;
    }

    public Set<TopicPartition> pausedPartitions() {
        HashSet<TopicPartition> paused = new HashSet<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final TopicPartitionState state = entry.getValue();
            if (state.paused) {
                paused.add(tp);
            }
        }
        return paused;
    }

    /**
     * Get the subscription for the group. For the leader, this will include the union of the
     * subscriptions of all group members. For followers, it is just that member's subscription.
     * This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     *
     * @return The union of all subscribed topics in the group if this member is the leader
     * of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    public Set<String> groupSubscription() {
        return this.groupSubscription;
    }

    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.get(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    public void committed(TopicPartition tp, OffsetAndMetadata offset) {
        assignedState(tp).committed(offset);
    }

    public OffsetAndMetadata committed(TopicPartition tp) {
        return assignedState(tp).committed;
    }

    public void needRefreshCommits() {
        this.needsFetchCommittedOffsets = true;
    }

    public boolean refreshCommitsNeeded() {
        return this.needsFetchCommittedOffsets;
    }

    public void commitsRefreshed() {
        this.needsFetchCommittedOffsets = false;
    }

    public void seek(TopicPartition tp, long offset) {
        assignedState(tp).seek(offset);
    }

    public Set<TopicPartition> assignedPartitions() {
        return this.assignment.keySet();
    }

    public Set<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> fetchable = new HashSet<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet()) {
            if (entry.getValue().isFetchable())
                fetchable.add(entry.getKey());
        }
        return fetchable;
    }

    public boolean partitionsAutoAssigned() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public void position(TopicPartition tp, long offset) {
        assignedState(tp).position(offset);
    }

    public Long position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    public Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet()) {
            TopicPartitionState state = entry.getValue();
            if (state.hasValidPosition())
                allConsumed.put(entry.getKey(), new OffsetAndMetadata(state.position));
        }
        return allConsumed;
    }

    public void needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).awaitReset(offsetResetStrategy);
    }

    public void needOffsetReset(TopicPartition partition) {
        needOffsetReset(partition, defaultResetStrategy);
    }

    public boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    public OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy;
    }

    public boolean hasAllFetchPositions() {
        for (TopicPartitionState state : assignment.values())
            if (!state.hasValidPosition())
                return false;
        return true;
    }

    public Set<TopicPartition> missingFetchPositions() {
        Set<TopicPartition> missing = new HashSet<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet())
            if (!entry.getValue().hasValidPosition())
                missing.add(entry.getKey());
        return missing;
    }

    public boolean partitionAssignmentNeeded() {
        return this.needsPartitionAssignment;
    }

    public boolean isAssigned(TopicPartition tp) {
        return assignment.containsKey(tp);
    }

    public boolean isPaused(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).paused;
    }

    public boolean isFetchable(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).isFetchable();
    }

    public void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    public void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    private void addAssignedPartition(TopicPartition tp) {
        this.assignment.put(tp, new TopicPartitionState());
    }

    public ConsumerRebalanceListener listener() {
        return listener;
    }

    /**
     * TopicPartition的消费状态
     */
    private static class TopicPartitionState {
        /**
         * 记录了下次要从Kafka服务端获取消息的Offset
         */
        private Long position; // last consumed position
        /**
         * 最近一次提交的Offset
         */
        private OffsetAndMetadata committed;  // last committed position
        /**
         * 记录当前TopicPartition是否处于暂停状态,与Consumer接口的pause()方法相关
         */
        private boolean paused;  // whether this partition has been paused by the user
        /**
         * OffsetResetStrategy枚举类型,充值position的策略
         * 同时,此字段是否为空,也表示了是否需要充值position的值
         */
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting

        public TopicPartitionState() {
            this.paused = false;
            this.position = null;
            this.committed = null;
            this.resetStrategy = null;
        }

        private void awaitReset(OffsetResetStrategy strategy) {
            this.resetStrategy = strategy;
            this.position = null;
        }

        public boolean awaitingReset() {
            return resetStrategy != null;
        }

        public boolean hasValidPosition() {
            return position != null;
        }

        private void seek(long offset) {
            this.position = offset;
            this.resetStrategy = null;
        }

        private void position(long offset) {
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = offset;
        }

        private void committed(OffsetAndMetadata offset) {
            this.committed = offset;
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

    }

}
