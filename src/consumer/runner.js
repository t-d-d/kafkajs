const { EventEmitter } = require('events')
// eslint-disable-next-line node/no-extraneous-require
const { transform, consume } = require('streaming-iterables')

const Long = require('../utils/long')
const createRetry = require('../retry')
const { KafkaJSError } = require('../errors')

const {
  events: { START_BATCH_PROCESS, END_BATCH_PROCESS },
} = require('./instrumentationEvents')

const isRebalancing = e =>
  e.type === 'REBALANCE_IN_PROGRESS' || e.type === 'NOT_COORDINATOR_FOR_GROUP'

const isKafkaJSError = e => e instanceof KafkaJSError

module.exports = class Runner extends EventEmitter {
  /**
   * @param {object} options
   * @param {import("../../types").Logger} options.logger
   * @param {import("./consumerGroup")} options.consumerGroup
   * @param {import("../instrumentation/emitter")} options.instrumentationEmitter
   * @param {boolean} [options.eachBatchAutoResolve=true]
   * @param {number} [options.partitionsConsumedConcurrently]
   * @param {(payload: import("../../types").EachBatchPayload) => Promise<void>} options.eachBatch
   * @param {(payload: import("../../types").EachMessagePayload) => Promise<void>} options.eachMessage
   * @param {number} [options.heartbeatInterval]
   * @param {(reason: Error) => void} options.onCrash
   * @param {import("../../types").RetryOptions} [options.retry]
   * @param {boolean} [options.autoCommit=true]
   */
  constructor({
    logger,
    consumerGroup,
    instrumentationEmitter,
    eachBatchAutoResolve = true,
    partitionsConsumedConcurrently,
    eachBatch,
    eachMessage,
    heartbeatInterval,
    onCrash,
    retry,
    autoCommit = true,
  }) {
    super()
    this.logger = logger.namespace('Runner')
    this.consumerGroup = consumerGroup
    this.instrumentationEmitter = instrumentationEmitter
    this.eachBatchAutoResolve = eachBatchAutoResolve
    this.eachBatch = eachBatch
    this.eachMessage = eachMessage
    this.heartbeatInterval = heartbeatInterval
    this.retrier = createRetry(Object.assign({}, retry))
    this.onCrash = onCrash
    this.autoCommit = autoCommit
    this.partitionsConsumedConcurrently = partitionsConsumedConcurrently
    this.running = false
  }

  async join() {
    await this.consumerGroup.joinAndSync()
  }

  async scheduleJoin() {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      })
      return
    }

    return this.join().catch(this.onCrash)
  }

  async start() {
    if (this.running) return

    this.logger.debug('start consumer group', {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
    })

    this.running = true

    const batchIterator = await this.consumerGroup.connect().catch(this.onCrash)
    this.promiseToEnd = this.runLoop(batchIterator)
  }

  async stop() {
    if (!this.running) return

    this.logger.debug('waiting for consumer to finish...', {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
    })

    this.running = false

    try {
      await this.consumerGroup.disconnect()
      await this.promiseToEnd

      this.logger.debug('stop consumer group', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      })
    } catch (e) {}
  }

  async runLoop(batchIterator) {
    const process = async batch => {
      await this.retrier(async (bail, retryCount, retryTime) => {
        try {
          await this.processBatch(batch)
        } catch (e) {
          if (!this.running) {
            this.logger.debug('consumer not running, exiting', {
              error: e.message,
              groupId: this.consumerGroup.groupId,
              memberId: this.consumerGroup.memberId,
            })
            return
          }

          if (isRebalancing(e)) {
            this.logger.error('The group is rebalancing, re-joining', {
              groupId: this.consumerGroup.groupId,
              memberId: this.consumerGroup.memberId,
              error: e.message,
              retryCount,
              retryTime,
            })

            await this.join()
            return
          }

          if (e.type === 'UNKNOWN_MEMBER_ID') {
            this.logger.error('The coordinator is not aware of this member, re-joining the group', {
              groupId: this.consumerGroup.groupId,
              memberId: this.consumerGroup.memberId,
              error: e.message,
              retryCount,
              retryTime,
            })

            this.consumerGroup.memberId = null
            await this.join()
            return
          }

          if (e.name === 'KafkaJSNotImplemented') {
            return bail(e)
          }

          throw e
        }
      })
    }

    try {
      await consume(transform(this.partitionsConsumedConcurrently, process, batchIterator))
    } catch (e) {
      this.onCrash(e)
    }
  }

  async processBatch(batch) {
    if (!this.running) return

    /**
     * Resolve the offset to skip empty control batches
     *
     * @see https://github.com/apache/kafka/blob/9aa660786e46c1efbf5605a6a69136a1dac6edb9/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java#L1499-L1505
     */
    if (batch.isEmptyControlRecord() || batch.isEmptyDueToLogCompactedMessages()) {
      this.consumerGroup.resolveOffset({
        topic: batch.topic,
        partition: batch.partition,
        offset: batch.lastOffset(),
      })
    }

    if (batch.isEmpty()) {
      await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval })
      await this.autoCommitOffsetsIfNecessary()
      return
    }

    const startBatchProcess = Date.now()
    const payload = {
      topic: batch.topic,
      partition: batch.partition,
      highWatermark: batch.highWatermark,
      offsetLag: batch.offsetLag(),
      /**
       * @since 2019-06-24 (>= 1.8.0)
       *
       * offsetLag returns the lag based on the latest offset in the batch, to
       * keep the event backward compatible we just introduced "offsetLagLow"
       * which calculates the lag based on the first offset in the batch
       */
      offsetLagLow: batch.offsetLagLow(),
      batchSize: batch.messages.length,
      firstOffset: batch.firstOffset(),
      lastOffset: batch.lastOffset(),
    }

    this.instrumentationEmitter.emit(START_BATCH_PROCESS, payload)

    try {
      if (this.eachMessage) {
        await this.processEachMessage(batch)
      } else if (this.eachBatch) {
        await this.processEachBatch(batch)
      }
    } catch (e) {
      if (!isKafkaJSError(e)) {
        this.logger.error(`Error when calling ${this.eachBatch ? 'eachBatch' : 'eachMessage'}`, {
          topic: batch.topic,
          partition: batch.partition,
          offset: batch.firstOffset(),
          stack: e.stack,
          error: e,
        })
      }

      // In case of errors, commit the previously consumed offsets unless autoCommit is disabled
      await this.autoCommitOffsets()
      throw e
    } finally {
      this.instrumentationEmitter.emit(END_BATCH_PROCESS, {
        ...payload,
        duration: Date.now() - startBatchProcess,
      })
    }
  }

  resolveBatchOffset(batch) {
    const { topic, partition } = batch
    const lastFilteredOffset = batch.messages.length
      ? Long.fromValue(batch.messages[batch.messages.length - 1].offset)
      : null

    return function resolveOffsetAndControlRecord(offset) {
      /**
       * The transactional producer generates a control record after committing the transaction.
       * The control record is the last record on the RecordBatch, and it is filtered before it
       * reaches the eachBatch callback. When disabling auto-resolve, the user-land code won't
       * be able to resolve the control record offset, since it never reaches the callback,
       * causing stuck consumers as the consumer will never move the offset marker.
       *
       * When the last offset of the batch is resolved, we should automatically resolve
       * the control record offset as this entry doesn't have any meaning to the user-land code,
       * and won't interfere with the stream processing.
       *
       * @see https://github.com/apache/kafka/blob/9aa660786e46c1efbf5605a6a69136a1dac6edb9/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java#L1499-L1505
       */
      const offsetToResolve =
        lastFilteredOffset && Long.fromValue(offset).equals(lastFilteredOffset)
          ? batch.lastOffset()
          : offset

      this.consumerGroup.resolveOffset({ topic, partition, offset: offsetToResolve })
    }.bind(this)
  }

  async processEachMessage(batch) {
    const { topic, partition } = batch
    const resolveOffset = this.resolveBatchOffset(batch)

    for (const message of batch.messages) {
      if (!this.running || this.consumerGroup.hasSeekOffset({ topic, partition })) {
        break
      }

      await this.eachMessage({ topic, partition, message })

      resolveOffset(message.offset)
      await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval })
      await this.autoCommitOffsetsIfNecessary()
    }
  }

  async processEachBatch(batch) {
    const { topic, partition } = batch
    const resolveOffset = this.resolveBatchOffset(batch)

    await this.eachBatch({
      batch,
      resolveOffset,
      heartbeat: async () => {
        await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval })
      },
      /**
       * Commit offsets if provided. Otherwise commit most recent resolved offsets
       * if the autoCommit conditions are met.
       *
       * @param {OffsetsByTopicPartition} [offsets] Optional.
       */
      commitOffsetsIfNecessary: async offsets => {
        return offsets
          ? this.consumerGroup.commitOffsets(offsets)
          : this.consumerGroup.commitOffsetsIfNecessary()
      },
      uncommittedOffsets: () => this.consumerGroup.uncommittedOffsets(),
      isRunning: () => this.running,
      isStale: () => this.consumerGroup.hasSeekOffset({ topic, partition }),
    })
    await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval })

    // resolveOffset for the last offset can be disabled to allow the users of eachBatch to
    // stop their consumers without resolving unprocessed offsets (issues/18)
    if (this.eachBatchAutoResolve) {
      this.consumerGroup.resolveOffset({ topic, partition, offset: batch.lastOffset() })
    }
    await this.autoCommitOffsetsIfNecessary()
  }

  autoCommitOffsets() {
    if (this.autoCommit) {
      return this.consumerGroup.commitOffsets()
    }
  }

  autoCommitOffsetsIfNecessary() {
    if (this.autoCommit) {
      return this.consumerGroup.commitOffsetsIfNecessary()
    }
  }

  commitOffsets(offsets) {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        offsets,
      })
      return
    }

    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await this.consumerGroup.commitOffsets(offsets)
      } catch (e) {
        if (!this.running) {
          this.logger.debug('consumer not running, exiting', {
            error: e.message,
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            offsets,
          })
          return
        }

        if (isRebalancing(e)) {
          this.logger.error('The group is rebalancing, re-joining', {
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            error: e.message,
            retryCount,
            retryTime,
          })

          setImmediate(() => this.scheduleJoin())

          bail(new KafkaJSError(e))
        }

        if (e.type === 'UNKNOWN_MEMBER_ID') {
          this.logger.error('The coordinator is not aware of this member, re-joining the group', {
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            error: e.message,
            retryCount,
            retryTime,
          })

          this.consumerGroup.memberId = null
          setImmediate(() => this.scheduleJoin())

          bail(new KafkaJSError(e))
        }

        if (e.name === 'KafkaJSNotImplemented') {
          return bail(e)
        }

        this.logger.debug('Error while committing offsets, trying again...', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
          error: e.message,
          stack: e.stack,
          retryCount,
          retryTime,
          offsets,
        })

        throw e
      }
    })
  }
}
