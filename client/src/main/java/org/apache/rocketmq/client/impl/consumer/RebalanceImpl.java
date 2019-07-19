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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * Base class for rebalance algorithm
 */
public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();
    //每个消息队列对应的缓存process Queue
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
        new ConcurrentHashMap<String, Set<MessageQueue>>();//topic对应的消息队列列表
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
        new ConcurrentHashMap<String, SubscriptionData>();//topic对应的订阅信息
    protected String consumerGroup;//消费组
    protected MessageModel messageModel;//消息模式
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;//消息队列负载策略
    protected MQClientInstance mQClientFactory;//mq client instance

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.get(mq.getBrokerName());
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    public boolean lock(final MessageQueue mq) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("the message queue lock {}, {} {}",
                    lockOK ? "OK" : "Failed",
                    this.consumerGroup,
                    mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    public void lockAll() {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    for (MessageQueue mq : lockOKMQSet) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            processQueue.setLocked(true);
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    public void doRebalance(final boolean isOrder) {
        //每个DefaultMQPushConsumerImpl 都持有一个单独的RebalanceImpl对象，获取当前consumer的订阅信息，可订阅多个topic。
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    this.rebalanceByTopic(topic, isOrder);//遍历订阅信息对每个topic的队列进行重新负载
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    /*
    引用自《RocketMQ技术内幕》
    问题l : PullRequest 对象在什么时候创建并加入到pullRequestQueue 中以便唤醒PullMessageService 线程。
    RebalanceService 线程每隔20s 对消费者订阅的主题进行一次队列重新分配， 每一次分配都会获取主题的所有队列、
    从Broker 服务器实时查询当前该主题该消费组内消费者列表， 对新分配的消息队列会创建对应的PullRequest对象。
    在一个JVM进程中，同一个消费组同一个队列只会存在一个PullRequest 对象。
    问题2 ： 集群内多个消费者是如何负载主题下的多个消费队列，并且如果有新的消费者加入时，消息队列又会如何重新分布。
    由于每次进行队列重新负载时会从Broker 实时查询出当前消费组内所有消费者，并且
    对消息队列、消费者列表进行排序，这样新加入的消费者就会在队列重新分布时分配到消费队列从而消费消息。
     */
    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        switch (messageModel) {
            case BROADCASTING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}",
                            consumerGroup,
                            topic,
                            mqSet,
                            mqSet);
                    }
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            case CLUSTERING: {//KKEY 集群模式
                /*
                引用自《RocketMQ技术内幕》
                从主题订阅信息缓存表中获取主题的队列信息；发送请求从Broker中该消费组内当前所有的消费者客户端ID，
                topic的队列可能分布在多个Broker 上， 那请求发往哪个Broker 呢？ RocketeMQ 从topic的路由信息表中随机选择一个Broker 。
                Broker 为什么会存在消费组内所有消费者的信息呢？ 我们不妨回忆一下消费者在启动的时候会向MQClientInstance中注册消费者，
                然后MQClientInstance会向所有的Broker 发送心跳包，心跳包中包含MQClientInstance的消费者信息。
                如果mqSet 、cidA!l 任意一个为空则忽略本次消息队列负载。
                 */

                //根据当前consumer以及topic获取订阅信息中的消息队列列表，这个TOPIC分配给这个consumer的队列列表
                //例如topic=tp,八个队列，订阅tp的是一个consumerGroup，有两个consumer，A和B，各四个队列
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                // 从broker端获取消费该消费组的所有客户端clientId
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>(mqSet);

                    /*
                    对Topic下的消息消费队列、消费者Id排序，
                    首先对cidAll,mqAll 排序，这个很重要，同一个消费组内看到的视图保持一致，
                    确保同一个消费队列不会被多个消费者分配
                     */
                    Collections.sort(mqAll);
                    Collections.sort(cidAll);

                    //不特意指定的情况下，默认是AllocateMessageQueueAveragely
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    List<MessageQueue> allocateResult;
                    try {
                        /*KKEY 根据负载策略，获取当前client分配的消息队列
                        KKEY NOTE 用消息队列分配策略算法（默认为：消息队列的平均分配算法），计算出待拉取的消息队列。
                        引用自design.md
                        这里的平均分配算法，类似于分页的算法，将所有MessageQueue排好序类似于记录，将所有消费端Consumer排好序类似页数，
                        并求出每一页需要包含的平均size和每个页面记录的范围range，最后遍历整个range而计算出当前Consumer端应该分配到的记录（这里即为：MessageQueue）

                        引用自《RocketMQ技术内幕》
                        RocketMQ 默认提供5 种分配算法。
                        1 ) AllocateMessageQueueAveragely ：平均分配，推荐指数为5 颗星。
                        举例来说，如果现在有8 个消息消费队列ql, q2, q3, q4, q5, q6 ,q7 ,q8，有3个消费者cl,c2,c3 ，
                        那么根据该负载算法，消息队列分配如下：
                        c1: ql,q2,q3
                        c2: q4,q5,q6
                        c3: q7,q8
                        2 ) AllocateMessageQueueAveragelyByCircle ：平均轮询分配，推荐指数为5 颗星。
                        举例来说，如果现在有8 个消息消费队列ql, q2, q3, q4, q5, q6 ,q7 ,q8，有3个消费者cl,c2,c3 ，
                        那么根据该负载算法，消息队列分配如下：
                        cl : ql, q4, q7
                        c2 : q2, q5, q8
                        c3 : q3, q6
                        3 ) AllocateMessageQueueConsistentHash ： 一致性hash 。不推荐使用，因为消息队列负载信息不容易跟踪。
                        4 ) AllocateMessageQueueByConfig ：根据配置，为每一个消费者配置固定的消息队列。
                        5 ) AllocateMessageQueueByMachineRoom ：根据Broker部署机房名，对每个消费者负责不同的Broker 上的队列。
                        负载算法如果没有特殊的要求，尽量使用AllocateMessageQueueAveragely 、AllocateMessageQueueAveragelyByCircle，
                        因为分配算法比较直观。消息队列分配遵循一个消费者可以分配多个消息队列，但同一个消息队列只会分配给一个消费者，
                        故如果消费者个数大于消息队列数量，则有些消费者无法消费消息。
                         */
                        allocateResult = strategy.allocate(
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mqAll,
                            cidAll);
                    } catch (Throwable e) {
                        log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(), e);
                        return;
                    }

                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }

                    //调用updateProcessQueueTableInRebalance()方法，先将分配到的消息队列集合（mqSet）与processQueueTable做一个过滤比对。
                    /*
                    引用自《RocketMQ技术内幕》
                    allocateResultSet为当前消费者负载的消息队列缓存表，如果缓存表中的MessageQueue不包含在mqSet中，说明经过本次消息队列负载后，
                    该mq 被分配给其他消费者，故需要暂停该消息队列消息的消费，方法是将ProcessQueue的状态设置为draped＝true，该ProcessQueue中的消息将不会再被消费，
                    调用removeUnnecessaryMessageQueue方法判断是否将MessageQueue、ProcessQueue缓存表中移除。
                    removeUnnecessaryMessageQueue在Rebalancelmple 定义为抽象方法。removeUnnecessaryMessageQueue方法主要持久化待移除MessageQueue消息消费进度。
                    在Push模式下，如果是集群模式并且是顺序消息消费时，还需要先解锁队列，
                     */
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                            "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }
    /**
    * 负载均衡后，更新消息处理队列
     * 移除 在processQueueTable && 不存在于 mqSet 里的消息队列
     * 增加 不在processQueueTable && 存在于mqSet 里的消息队列
    * @param mqSet 负载均衡结果后的消息队列数组
    */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        boolean changed = false;

        /*
         引用自《RocketMQ技术内幕》
         allocateResultSet为当前消费者负载的消息队列缓存表，如果缓存表中的MessageQueue不包含在mqSet中，说明经过本次消息队列负载后，
         该mq 被分配给其他消费者，故需要暂停该消息队列消息的消费，方法是将ProcessQueue的状态设置为draped＝true，该ProcessQueue中的消息将不会再被消费，
         调用removeUnnecessaryMessageQueue方法判断是否将MessageQueue、ProcessQueue缓存表中移除。
         removeUnnecessaryMessageQueue在Rebalancelmple 定义为抽象方法。removeUnnecessaryMessageQueue方法主要持久化待移除MessageQueue消息消费进度。
         在Push模式下，如果是集群模式并且是顺序消息消费时，还需要先解锁队列，
         */
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            if (mq.getTopic().equals(topic)) {
                //匹配processQueue和当前topic相等的记录
                if (!mqSet.contains(mq)) {//筛选出的消息队列不在负载之后的消息队列中，直接将processQueue
                    pq.setDropped(true);
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                        it.remove();
                        changed = true;
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                } else if (pq.isPullExpired()) {
                    //如果当前队列的topic和当前topic匹配，且队列在负载之后的队列里，则判断是否超时，超时也要删除
                    switch (this.consumeType()) {
                        case CONSUME_ACTIVELY:
                            break;
                        case CONSUME_PASSIVELY:
                            pq.setDropped(true);
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                it.remove();
                                changed = true;
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                    consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        /*
         引用自《RocketMQ技术内幕》
        遍历本次负载分配到的队列集合，如果processQueueTable中没有包含该消息队列，表明这是本次新增加的消息队列，
        首先从内存中移除该消息队列的消费进度，然后从磁盘中读取该消息队列的消费进度，创建PullRequest 对象。
        这里有一个关键，如果读取到的消费进度小于0 ，则需要校对消费进度。
        RocketMQ 提供CONSUME_FROM_LAST_OFFSET 、CONSUME_FROM_F IRST OFFSET 、CONSUME_FROM_TIMESTAMP 方式，
        在创建消费者时可以通过调用DefaultMQPushConsumer#setConsumeFromWhere 方法设置。
        PullRequest 的nextOffset计算逻辑位于：RebalancePushlmpl#computePullFromWhere
         */
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        for (MessageQueue mq : mqSet) {
            if (!this.processQueueTable.containsKey(mq)) {
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }

                this.removeDirtyOffset(mq);
                ProcessQueue pq = new ProcessQueue();
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        //当前队列已经在processQueueTable中了，表明上一次拉取还没有处理完
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        //创建拉取请求
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);//消费者组
                        pullRequest.setNextOffset(nextOffset);//拉取消息开始偏移量
                        pullRequest.setMessageQueue(mq);//待拉取消息队列
                        pullRequest.setProcessQueue(pq);//拉取消息临时存放process queue等待消费业务线程处理
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }

        this.dispatchPullRequest(pullRequestList);//TODO 发起消息拉取请求

        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }
}
