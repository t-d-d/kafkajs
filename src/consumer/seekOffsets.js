module.exports = class SeekOffsets {
  constructor() {
    this.offsets = {}
  }

  set(topic, partition, offset) {
    if (!this.offsets[topic]) this.offsets[topic] = {}
    this.offsets[topic][partition] = offset
  }

  has(topic, partition) {
    return this.offsets[topic] && this.offsets[topic][partition]
  }

  remove(topic, partition) {
    const oldOffset = this.offsets[topic][partition]
    delete this.offsets[topic][partition]
    return oldOffset
  }
}
