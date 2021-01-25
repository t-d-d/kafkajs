module.exports = class SeekOffsets {
  constructor() {
    this.offsets = {}
  }

  set(topic, partition, offset) {
    if (!this.offsets[topic]) this.offsets[topic] = {}
    this.offsets[topic][partition] = offset
  }

  get(topic, partition, offset) {
    return this.has(topic, partition) ? this.offsets[topic][partition] : null
  }

  has(topic, partition) {
    return this.offsets[topic] && this.offsets[topic][partition]
  }

  del(topic, partition) {
    delete this.offsets[topic][partition]
  }
}
