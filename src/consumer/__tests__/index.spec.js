const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForConsumerToJoinGroup,
  generateMessages,
} = require('testHelpers')
const createConsumer = require('../index')
const createProducer = require('../../producer')

describe('Consumer', () => {
  let topicName, groupId, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`
    const cluster = createCluster({ metadataMaxAge: 50 })
    await createTopic({ topic: topicName })
    const producer = createProducer({
      cluster,
      logger: newLogger(),
    })
    await producer.connect()
    await producer.send({
      acks: 1,
      topic: topicName,
      messages: generateMessages(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
  })

  it('when the consumer is already running, ignore subsequent run() calls', async () => {
    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    const eachMessage = jest.fn()

    Promise.all([
      consumer.run({ eachMessage }),
      consumer.run({ eachMessage }),
      consumer.run({ eachMessage }),
    ])

    // Since the consumer gets overridden, it will fail to join the group
    // as three other consumers will also try to join. This case is hard to write a test
    // since we can only assert the symptoms of the problem, but we can't assert that
    // we don't initialize the consumer.
    await waitForConsumerToJoinGroup(consumer)
  })

  it('when the consumer is running, can immediately stop the consumer', async () => {
    const eachMessage = jest.fn()
    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    await consumer.run({ eachMessage })
    await consumer.stop()
    await consumer.disconnect()
    expect(eachMessage).not.toHaveBeenCalled()
  })
})
