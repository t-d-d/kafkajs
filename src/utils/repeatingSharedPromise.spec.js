const sharedPromiseTo = require('./repeatingSharedPromise')

describe('Utils > repeatingSharedPromise', () => {
  let resolvePromise1, rejectPromise1, sharedPromise1, invocationsPromise1, settleFunction

  beforeEach(() => {
    settleFunction = jest.fn(({ resolve, reject, invocations }) => {
      resolvePromise1 = resolve
      rejectPromise1 = reject
      invocationsPromise1 = invocations
    })
    sharedPromise1 = sharedPromiseTo(settleFunction)
  })

  it('Returns the same pending promise for every invocation', async () => {
    const p1 = sharedPromise1()
    const p2 = sharedPromise1()
    const p3 = sharedPromise1()
    expect(Object.is(p1, p2)).toBe(true)
    expect(Object.is(p2, p3)).toBe(true)
    expect(settleFunction).toHaveBeenCalledTimes(3)
  })

  it('Promise is resolved after calling the resolve function', async () => {
    const message = 'Resolved promise #1'
    const p1 = sharedPromise1()
    resolvePromise1(message)
    await expect(p1).resolves.toMatch(message)
    expect(settleFunction).toHaveBeenCalledTimes(1)
  })

  it('Promise is rejected after calling the reject function', async () => {
    const message = 'Resolved promise #1'
    const p1 = sharedPromise1()
    rejectPromise1(new Error(message))
    await expect(p1).rejects.toThrow(message)
    expect(settleFunction).toHaveBeenCalledTimes(1)
  })

  it('Returns a new promise after first is resolved', async () => {
    const message = 'Resolved promise #1'
    const p1 = sharedPromise1()
    resolvePromise1(message)
    const p2 = sharedPromise1()
    expect(Object.is(p1, p2)).toBe(false)
    expect(settleFunction).toHaveBeenCalledTimes(2)
  })

  it('Returns a new promise after first is rejected', async () => {
    const message = 'Reject promise #1'
    const p1 = sharedPromise1()
    rejectPromise1(new Error(message))
    const p2 = sharedPromise1()
    expect(Object.is(p1, p2)).toBe(false)
    expect(settleFunction).toHaveBeenCalledTimes(2)
  })

  it('Passes through arguments passed at invocation time', async () => {
    const args0 = ['arg0_0', 'arg0_1']
    const args1 = ['arg1_0', 'arg1_1']
    sharedPromise1(...args0)
    sharedPromise1(...args1)
    expect(invocationsPromise1[0].args).toEqual(args0)
    expect(invocationsPromise1[1].args).toEqual(args1)
  })
})
