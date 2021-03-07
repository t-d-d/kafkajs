// eslint-disable-next-line node/no-extraneous-require
const { parallelMerge } = require('streaming-iterables')

const flatten = require('../utils/flatten')
const sleep = require('../utils/sleep')
const websiteUrl = require('../utils/websiteUrl')
const arrayDiff = require('../utils/arrayDiff')
const createRetry = require('../retry')
const sharedPromiseTo = require('../utils/sharedPromiseTo')

const OffsetManager = require('./offsetManager')
const Batch = require('./batch')
const SeekOffsets = require('./seekOffsets')
const SubscriptionState = require('./subscriptionState')
const {
  events: { GROUP_JOIN, HEARTBEAT, CONNECT, RECEIVED_UNSUBSCRIBED_TOPICS, FETCH_START, FETCH },
} = require('./instrumentationEvents')
const { MemberAssignment } = require('./assignerProtocol')
const {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSStaleTopicMetadataAssignment,
} = require('../errors')

const { keys } = Object

const STALE_METADATA_ERRORS = [
  'LEADER_NOT_AVAILABLE',
  // Fetch before v9 uses NOT_LEADER_FOR_PARTITION
  'NOT_LEADER_FOR_PARTITION',
  // Fetch after v9 uses {FENCED,UNKNOWN}_LEADER_EPOCH
  'FENCED_LEADER_EPOCH',
  'UNKNOWN_LEADER_EPOCH',
  'UNKNOWN_TOPIC_OR_PARTITION',
]

const isRebalancing = e =>
  e.type === 'REBALANCE_IN_PROGRESS' || e.type === 'NOT_COORDINATOR_FOR_GROUP'

const PRIVATE = {
  JOIN: Symbol('private:ConsumerGroup:join'),
  SYNC: Symbol('private:ConsumerGroup:sync'),
  getBrokerAsyncGenerator: Symbol('private:ConsumerGroup:createBrokerAsyncIterator'),
  emitBrokerFetchStart: Symbol('private:ConsumerGroup:emitBrokerFetchStart'),
  sharedHeartbeat: Symbol('private:ConsumerGroup:sharedHeartbeat'),
  sharedJoinAndSync: Symbol('private:ConsumerGroup:sharedJoinAndSync'),
  sharedRefreshMetadata: Symbol('private:ConsumerGroup:sharedRefreshMetadata'),
  sharedRefreshMetadataIfNecessary: Symbol(
    'private:ConsumerGroup:sharedRefreshMetadataIfNecessary'
  ),
}

module.exports = class ConsumerGroup {
  constructor({
    retry,
    cluster,
    groupId,
    topics,
    topicConfigurations,
    logger,
    instrumentationEmitter,
    assigners,
    sessionTimeout,
    rebalanceTimeout,
    maxBytesPerPartition,
    minBytes,
    maxBytes,
    maxWaitTimeInMs,
    autoCommit,
    autoCommitInterval,
    autoCommitThreshold,
    isolationLevel,
    rackId,
    metadataMaxAge,
  }) {
    /** @type {import("../../types").Cluster} */
    this.cluster = cluster
    this.groupId = groupId
    this.topics = topics
    this.topicsSubscribed = topics
    this.topicConfigurations = topicConfigurations
    this.logger = logger.namespace('ConsumerGroup')
    this.instrumentationEmitter = instrumentationEmitter
    this.retrier = createRetry(Object.assign({}, retry))
    this.assigners = assigners
    this.sessionTimeout = sessionTimeout
    this.rebalanceTimeout = rebalanceTimeout
    this.maxBytesPerPartition = maxBytesPerPartition
    this.minBytes = minBytes
    this.maxBytes = maxBytes
    this.maxWaitTime = maxWaitTimeInMs
    this.autoCommit = autoCommit
    this.autoCommitInterval = autoCommitInterval
    this.autoCommitThreshold = autoCommitThreshold
    this.isolationLevel = isolationLevel
    this.rackId = rackId
    this.metadataMaxAge = metadataMaxAge

    this.seekOffset = new SeekOffsets()
    this.coordinator = null
    this.generationId = null
    this.leaderId = null
    this.memberId = null
    this.members = null
    this.groupProtocol = null

    this.partitionsPerSubscribedTopic = null
    /**
     * Preferred read replica per topic and partition
     *
     * Each of the partitions tracks the preferred read replica (`nodeId`) and a timestamp
     * until when that preference is valid.
     *
     * @type {{[topicName: string]: {[partition: number]: {nodeId: number, expireAt: number}}}}
     */
    this.preferredReadReplicasPerTopicPartition = {}
    this.offsetManager = null
    this.subscriptionState = new SubscriptionState()

    this.lastRequest = Date.now()

    this.running = true

    this.brokerAsyncIterators = {}

    this.rebalanceRequired = true
    this[PRIVATE.JOINANDSYNC] = sharedPromiseTo(async () => {
      const startJoin = Date.now()
      return this.retrier(async bail => {
        try {
          await this[PRIVATE.JOIN]()
          await this[PRIVATE.SYNC]()

          const memberAssignment = this.assigned().reduce(
            (result, { topic, partitions }) => ({ ...result, [topic]: partitions }),
            {}
          )

          const payload = {
            groupId: this.groupId,
            memberId: this.memberId,
            leaderId: this.leaderId,
            isLeader: this.isLeader(),
            memberAssignment,
            groupProtocol: this.groupProtocol,
            duration: Date.now() - startJoin,
          }

          this.instrumentationEmitter.emit(GROUP_JOIN, payload)
          this.logger.info('Consumer has joined the group', payload)
          this.rebalanceRequired = false

          await this.offsetManager.resolveOffsets() // TODO Is this correct place?
        } catch (e) {
          if (isRebalancing(e)) {
            // Rebalance in progress isn't a retriable protocol error since the consumer
            // has to go through find coordinator and join again before it can
            // actually retry the operation. We wrap the original error in a retriable error
            // here instead in order to restart the join + sync sequence using the retrier.
            throw new KafkaJSError(e)
          }

          bail(e)
        }
      })
    })

    this[PRIVATE.sharedHeartbeat] = sharedPromiseTo(async ({ interval }) => {
      const { groupId, generationId, memberId } = this
      const now = Date.now()

      if (memberId && now >= this.lastRequest + interval) {
        const payload = {
          groupId,
          memberId,
          groupGenerationId: generationId,
        }

        await this.coordinator.heartbeat(payload)
        this.instrumentationEmitter.emit(HEARTBEAT, payload)
        this.lastRequest = Date.now()
      }
    })

    this[PRIVATE.sharedRefreshMetadata] = sharedPromiseTo(() => this.cluster.refreshMetadata())
    this[PRIVATE.sharedRefreshMetadataIfNecessary] = sharedPromiseTo(() =>
      this.cluster.refreshMetadataIfNecessary()
    )
  }

  isLeader() {
    return this.leaderId && this.memberId === this.leaderId
  }

  async connect() {
    await this.cluster.connect()
    this.instrumentationEmitter.emit(CONNECT)

    const { brokers } = await this.cluster.metadata()
    const batchIterator = parallelMerge(
      ...brokers.map(({ nodeId }) => this[PRIVATE.getBrokerAsyncGenerator](nodeId)())
    )
    this.running = true

    await this.joinAndSync()

    return batchIterator
  }

  stop() {
    this.running = false
  }

  async disconnect() {
    this.stop()
    await this.leave()
  }

  async [PRIVATE.JOIN]() {
    const { groupId, sessionTimeout, rebalanceTimeout } = this

    this.coordinator = await this.cluster.findGroupCoordinator({ groupId })

    const groupData = await this.coordinator.joinGroup({
      groupId,
      sessionTimeout,
      rebalanceTimeout,
      memberId: this.memberId || '',
      groupProtocols: this.assigners.map(assigner =>
        assigner.protocol({
          topics: this.topicsSubscribed,
        })
      ),
    })

    this.generationId = groupData.generationId
    this.leaderId = groupData.leaderId
    this.memberId = groupData.memberId
    this.members = groupData.members
    this.groupProtocol = groupData.groupProtocol
  }

  async leave() {
    const { groupId, memberId } = this
    if (memberId) {
      await this.coordinator.leaveGroup({ groupId, memberId })
      this.memberId = null
    }
  }

  async [PRIVATE.SYNC]() {
    let assignment = []
    const {
      groupId,
      generationId,
      memberId,
      members,
      groupProtocol,
      topics,
      topicsSubscribed,
      coordinator,
    } = this

    if (this.isLeader()) {
      this.logger.debug('Chosen as group leader', { groupId, generationId, memberId, topics })
      const assigner = this.assigners.find(({ name }) => name === groupProtocol)

      if (!assigner) {
        throw new KafkaJSNonRetriableError(
          `Unsupported partition assigner "${groupProtocol}", the assigner wasn't found in the assigners list`
        )
      }

      await this.cluster.refreshMetadata()
      assignment = await assigner.assign({ members, topics: topicsSubscribed })

      this.logger.debug('Group assignment', {
        groupId,
        generationId,
        groupProtocol,
        assignment,
        topics: topicsSubscribed,
      })
    }

    // Keep track of the partitions for the subscribed topics
    this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()
    const { memberAssignment } = await this.coordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupAssignment: assignment,
    })

    const decodedMemberAssignment = MemberAssignment.decode(memberAssignment)
    const decodedAssignment =
      decodedMemberAssignment != null ? decodedMemberAssignment.assignment : {}

    this.logger.debug('Received assignment', {
      groupId,
      generationId,
      memberId,
      memberAssignment: decodedAssignment,
    })

    const assignedTopics = keys(decodedAssignment)
    const topicsNotSubscribed = arrayDiff(assignedTopics, topicsSubscribed)

    if (topicsNotSubscribed.length > 0) {
      const payload = {
        groupId,
        generationId,
        memberId,
        assignedTopics,
        topicsSubscribed,
        topicsNotSubscribed,
      }

      this.instrumentationEmitter.emit(RECEIVED_UNSUBSCRIBED_TOPICS, payload)
      this.logger.warn('Consumer group received unsubscribed topics', {
        ...payload,
        helpUrl: websiteUrl(
          'docs/faq',
          'why-am-i-receiving-messages-for-topics-i-m-not-subscribed-to'
        ),
      })
    }

    // Remove unsubscribed topics from the list
    const safeAssignment = arrayDiff(assignedTopics, topicsNotSubscribed)
    const currentMemberAssignment = safeAssignment.map(topic => ({
      topic,
      partitions: decodedAssignment[topic],
    }))

    // Check if the consumer is aware of all assigned partitions
    for (const assignment of currentMemberAssignment) {
      const { topic, partitions: assignedPartitions } = assignment
      const knownPartitions = this.partitionsPerSubscribedTopic.get(topic)
      const isAwareOfAllAssignedPartitions = assignedPartitions.every(partition =>
        knownPartitions.includes(partition)
      )

      if (!isAwareOfAllAssignedPartitions) {
        this.logger.warn('Consumer is not aware of all assigned partitions, refreshing metadata', {
          groupId,
          generationId,
          memberId,
          topic,
          knownPartitions,
          assignedPartitions,
        })

        // If the consumer is not aware of all assigned partitions, refresh metadata
        // and update the list of partitions per subscribed topic. It's enough to perform
        // this operation once since refresh metadata will update metadata for all topics
        await this.cluster.refreshMetadata()
        this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()
        break
      }
    }

    this.topics = currentMemberAssignment.map(({ topic }) => topic)
    this.subscriptionState.assign(currentMemberAssignment)
    this.offsetManager = new OffsetManager({
      cluster: this.cluster,
      topicConfigurations: this.topicConfigurations,
      instrumentationEmitter: this.instrumentationEmitter,
      memberAssignment: currentMemberAssignment.reduce(
        (partitionsByTopic, { topic, partitions }) => ({
          ...partitionsByTopic,
          [topic]: partitions,
        }),
        {}
      ),
      autoCommit: this.autoCommit,
      autoCommitInterval: this.autoCommitInterval,
      autoCommitThreshold: this.autoCommitThreshold,
      coordinator,
      groupId,
      generationId,
      memberId,
    })
  }

  setRebalanceRequired() {
    this.rebalanceRequired = true
  }

  /**
   * @param {import("../../types").TopicPartition} topicPartition
   */
  resetOffset({ topic, partition }) {
    this.offsetManager.resetOffset({ topic, partition })
  }

  /**
   * @param {import("../../types").TopicPartitionOffset} topicPartitionOffset
   */
  resolveOffset({ topic, partition, offset }) {
    this.offsetManager.resolveOffset({ topic, partition, offset })
  }

  /**
   * Update the consumer offset for the given topic/partition. This will be used
   * on the next fetch. If this API is invoked for the same topic/partition more
   * than once, the latest offset will be used on the next fetch.
   *
   * @param {import("../../types").TopicPartitionOffset} topicPartitionOffset
   */
  seek({ topic, partition, offset }) {
    this.seekOffset.set(topic, partition, offset)
  }

  pause(topicPartitions) {
    this.logger.info(`Pausing fetching from ${topicPartitions.length} topics`, {
      topicPartitions,
    })
    this.subscriptionState.pause(topicPartitions)
  }

  resume(topicPartitions) {
    this.logger.info(`Resuming fetching from ${topicPartitions.length} topics`, {
      topicPartitions,
    })
    this.subscriptionState.resume(topicPartitions)
  }

  assigned() {
    return this.subscriptionState.assigned()
  }

  paused() {
    return this.subscriptionState.paused()
  }

  async commitOffsetsIfNecessary() {
    await this.offsetManager.commitOffsetsIfNecessary()
  }

  async commitOffsets(offsets) {
    await this.offsetManager.commitOffsets(offsets)
  }

  uncommittedOffsets() {
    return this.offsetManager.uncommittedOffsets()
  }

  async heartbeat({ interval }) {
    return this[PRIVATE.sharedHeartbeat]({ interval })
  }

  [PRIVATE.emitBrokerFetchStart]({ nodeId }) {
    const emitAndReset = () => {
      this.instrumentationEmitter.emit(FETCH_START, {})
      this.brokerFetchEnds = Object.keys(this.brokerAsyncIterators).reduce(
        (a, brokerId) => ({
          ...a,
          [brokerId]: 0,
        }),
        {}
      )
    }

    if (!this.brokerFetchEnds) {
      // First time through
      emitAndReset()
      this.brokerFetchEnds[nodeId]++
      return
    }

    this.brokerFetchEnds[nodeId]++
    if (Object.values(this.brokerFetchEnds).every(count => count > 0)) {
      emitAndReset()
    }
  }

  [PRIVATE.emitBrokerFetchEnd]({ nodeId }) {
    const reset = () => {
      this.brokerFetchEnds = Object.keys(this.brokerAsyncIterators).reduce(
        (a, brokerId) => ({
          ...a,
          [brokerId]: 0,
        }),
        {}
      )
    }

    if (!this.brokerFetchEnds) {
      // First time through
      reset()
      this.brokerFetchEnds[nodeId]++
      return
    }

    this.brokerFetchEnds[nodeId]++
    if (Object.values(this.brokerFetchEnds).every(count => count > 0)) {
      this.instrumentationEmitter.emit(FETCH, {
        duration: 1,
        numberOfBatches: 0,
      })
      reset()
    }
  }

  [PRIVATE.getBrokerAsyncGenerator](nodeId) {
    if (this.brokerAsyncIterators[nodeId]) return this.brokerAsyncIterators[nodeId]
    this.brokerAsyncIterators[nodeId] = async function* brokerAsyncGenerator() {
      while (this.running) {
        this[PRIVATE.emitBrokerFetchStart]({ nodeId })

        const { requestsForNode, responses } = await this.retrier(
          async (bail, retryCount, retryTime) => {
            const { maxBytesPerPartition, maxWaitTime, minBytes, maxBytes } = this
            /** @type {{topic: string, partitions: { partition: number; fetchOffset: string; maxBytes: number }[]} */
            const requestsForNode = []

            try {
              await this[PRIVATE.sharedRefreshMetadataIfNecessary]()
              this.checkForStaleAssignment()

              if (this.rebalanceRequired) await this[PRIVATE.sharedJoinAndSync]()

              // Seeks for partition on this node
              for (const { topic, partitions } of this.subscriptionState.assigned()) {
                const partitionsForNode =
                  this.findReadReplicaForPartitions(topic, partitions)[nodeId] || []
                for (const partition of partitionsForNode) {
                  const offset = this.seekOffset.get(topic, partition)
                  if (offset != null) {
                    this.logger.debug('Seek offset', {
                      groupId: this.groupId,
                      memberId: this.memberId,
                      seek: { topic, partition, offset },
                    })
                    await this.offsetManager.seek({ topic, partition, offset })
                    this.seekOffset.del(topic, partition)
                  }
                }
              }

              // partition request for this node
              for (const { topic, partitions } of this.subscriptionState.active()) {
                const partitionsForNode =
                  this.findReadReplicaForPartitions(topic, partitions)[nodeId] || []

                const requestPartitions = partitionsForNode.map(partition => ({
                  partition,
                  fetchOffset: this.offsetManager.nextOffset(topic, partition).toString(),
                  maxBytes: maxBytesPerPartition,
                }))

                if (requestPartitions.length)
                  requestsForNode.push({ topic, partitions: requestPartitions })
              }

              if (requestsForNode.length === 0) return { requestsForNode, responses: [] }

              const broker = await this.cluster.findBroker({ nodeId })
              const { responses } = await broker.fetch({
                maxWaitTime,
                minBytes,
                maxBytes,
                isolationLevel: this.isolationLevel,
                topics: requestsForNode,
                rackId: this.rackId,
              })
              return {
                requestsForNode,
                responses,
              }
            } catch (e) {
              return {
                requestsForNode,
                responses: await this.recoverFromFetch(e, bail, retryCount, retryTime),
              }
            }
          }
        )
        this[PRIVATE.emitBrokerFetchEnd]({ nodeId })

        const filteredTopicPartitions = responses.map(({ topicName, partitions }) => ({
          topicName,
          partitions: partitions.filter(
            partitionData =>
              !this.seekOffset.has(topicName, partitionData.partition) &&
              !this.subscriptionState.isPaused(topicName, partitionData.partition)
          ),
        }))

        const batchesPerPartition = filteredTopicPartitions.map(({ topicName, partitions }) => {
          const topicRequestData = requestsForNode.find(({ topic }) => topic === topicName)

          return partitions.map(partitionData => {
            const fetchOffset = topicRequestData.partitions.find(
              ({ partition }) => partition === partitionData.partition
            ).fetchOffset
            return new Batch(topicName, fetchOffset, partitionData)
          })
        })

        for (const batch of flatten(batchesPerPartition)) yield batch

        // Prevent busy-loop if no request actually made
        if (requestsForNode.length === 0) await sleep(this.maxWaitTime)

        // At this point all batches have been processed - safe to move partitions between brokers
        filteredTopicPartitions.forEach(({ topicName, partitions }) => {
          let preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topicName]
          if (!preferredReadReplicas) {
            this.preferredReadReplicasPerTopicPartition[topicName] = preferredReadReplicas = {}
          }

          partitions.forEach(({ partition, preferredReadReplica }) => {
            if (preferredReadReplica != null && preferredReadReplica !== -1) {
              const { nodeId: currentPreferredReadReplica } = preferredReadReplicas[partition] || {}
              if (currentPreferredReadReplica !== preferredReadReplica) {
                this.logger.info(`Preferred read replica is now ${preferredReadReplica}`, {
                  groupId: this.groupId,
                  memberId: this.memberId,
                  topic: topicName,
                  partition,
                })
              }
              preferredReadReplicas[partition] = {
                nodeId: preferredReadReplica,
                expireAt: Date.now() + this.metadataMaxAge,
              }
            }
          })
        })
      }
    }.bind(this)
    return this.brokerAsyncIterators[nodeId]
  }

  async recoverFromFetch(e, bail, retryCount, retryTime) {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting', {
        error: e.message,
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      })
      return []
    }

    if (STALE_METADATA_ERRORS.includes(e.type) || e.name === 'KafkaJSTopicMetadataNotLoaded') {
      this.logger.debug('Stale cluster metadata, refreshing...', {
        groupId: this.groupId,
        memberId: this.memberId,
        error: e.message,
      })

      await this[PRIVATE.sharedRefreshMetadata]
      this.setRebalanceRequired()
      return []
    }

    if (e.name === 'KafkaJSOffsetOutOfRange') {
      await this.recoverFromOffsetOutOfRange(e)
      return []
    }

    if (e.name === 'KafkaJSStaleTopicMetadataAssignment') {
      this.logger.warn(`${e.message}, resync group`, {
        groupId: this.groupId,
        memberId: this.memberId,
        topic: e.topic,
        unknownPartitions: e.unknownPartitions,
      })

      this.setRebalanceRequired()
      return []
    }

    if (e.name === 'KafkaJSConnectionClosedError') {
      this.cluster.removeBroker({ host: e.host, port: e.port })
    }

    if (e.name === 'KafkaJSBrokerNotFound' || e.name === 'KafkaJSConnectionClosedError') {
      this.logger.debug(`${e.message}, refreshing metadata and retrying...`)
      await this[PRIVATE.sharedRefreshMetadata]
    }

    this.logger.debug('Error while fetching data, trying again...', {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
      error: e.message,
      stack: e.stack,
      retryCount,
      retryTime,
    })

    throw e
  }

  async recoverFromOffsetOutOfRange(e) {
    // If we are fetching from a follower try with the leader before resetting offsets
    const preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[e.topic]
    if (preferredReadReplicas && typeof preferredReadReplicas[e.partition] === 'number') {
      this.logger.info('Offset out of range while fetching from follower, retrying with leader', {
        topic: e.topic,
        partition: e.partition,
        groupId: this.groupId,
        memberId: this.memberId,
      })
      delete preferredReadReplicas[e.partition]
    } else {
      this.logger.error('Offset out of range, resetting to default offset', {
        topic: e.topic,
        partition: e.partition,
        groupId: this.groupId,
        memberId: this.memberId,
      })

      await this.offsetManager.setDefaultOffset({
        topic: e.topic,
        partition: e.partition,
      })
    }
  }

  generatePartitionsPerSubscribedTopic() {
    const map = new Map()

    for (const topic of this.topicsSubscribed) {
      const partitions = this.cluster
        .findTopicPartitionMetadata(topic)
        .map(m => m.partitionId)
        .sort()

      map.set(topic, partitions)
    }

    return map
  }

  checkForStaleAssignment() {
    if (!this.partitionsPerSubscribedTopic) {
      return
    }

    const newPartitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()

    for (const [topic, partitions] of newPartitionsPerSubscribedTopic) {
      const diff = arrayDiff(partitions, this.partitionsPerSubscribedTopic.get(topic))

      if (diff.length > 0) {
        throw new KafkaJSStaleTopicMetadataAssignment('Topic has been updated', {
          topic,
          unknownPartitions: diff,
        })
      }
    }
  }

  hasSeekOffset({ topic, partition }) {
    return this.seekOffset.has(topic, partition)
  }

  /**
   * For each of the partitions find the best nodeId to read it from
   *
   * @param {string} topic
   * @param {number[]} partitions
   * @returns {{[nodeId: number]: number[]}} per-node assignment of partitions
   * @see Cluster~findLeaderForPartitions
   */
  // Invariant: The resulting object has each partition referenced exactly once
  findReadReplicaForPartitions(topic, partitions) {
    const partitionMetadata = this.cluster.findTopicPartitionMetadata(topic)
    const preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topic]
    return partitions.reduce((result, id) => {
      const partitionId = parseInt(id, 10)
      const metadata = partitionMetadata.find(p => p.partitionId === partitionId)
      if (!metadata) {
        return result
      }

      if (metadata.leader == null) {
        throw new KafkaJSError('Invalid partition metadata', { topic, partitionId, metadata })
      }

      // Pick the preferred replica if there is one, and it isn't known to be offline, otherwise the leader.
      let nodeId = metadata.leader
      if (preferredReadReplicas) {
        const { nodeId: preferredReadReplica, expireAt } = preferredReadReplicas[partitionId] || {}
        if (Date.now() >= expireAt) {
          this.logger.debug('Preferred read replica information has expired, using leader', {
            topic,
            partitionId,
            groupId: this.groupId,
            memberId: this.memberId,
            preferredReadReplica,
            leader: metadata.leader,
          })
          // Drop the entry
          delete preferredReadReplicas[partitionId]
        } else if (preferredReadReplica != null) {
          // Valid entry, check whether it is not offline
          // Note that we don't delete the preference here, and rather hope that eventually that replica comes online again
          const offlineReplicas = metadata.offlineReplicas
          if (Array.isArray(offlineReplicas) && offlineReplicas.includes(nodeId)) {
            this.logger.debug('Preferred read replica is offline, using leader', {
              topic,
              partitionId,
              groupId: this.groupId,
              memberId: this.memberId,
              preferredReadReplica,
              leader: metadata.leader,
            })
          } else {
            nodeId = preferredReadReplica
          }
        }
      }
      const current = result[nodeId] || []
      return { ...result, [nodeId]: [...current, partitionId] }
    }, {})
  }
}
