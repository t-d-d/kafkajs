module.exports = class SeekOffsets extends Map {
  set(topic, partition, offset) {
    super.set([topic, partition], offset)
  }

  has(topic, partition) {
    return Array.from(this.keys()).some(([t, p]) => t === topic && p === partition)
  }

  remove(topic, partition) {
    const key = [topic, partition]
    const offset = this.get(key)
    this.delete(key)
    return { topic, partition, offset }
  }

  pop() {
    if (this.size === 0) {
      return
    }

    const [key, offset] = this.entries().next().value
    this.delete(key)
    const [topic, partition] = key
    return { topic, partition, offset }
  }
}
