/**
 * @template T
 * @param { (...args: any) => Promise<T> } [settleFunction]
 * Promise returning function that will only ever be invoked sequentially.
 * @returns { (...args: any) => Promise<T> }
 * Function that may invoke asyncFunction if there is not a currently executing invocation.
 * Returns promise from the currently executing invocation.
 */
module.exports = settleFunction => {
  let promise
  let resolveFunction
  let rejectFunction
  let invocations
  let accumulator

  const resetSharedPromise = () => {
    invocations = []
    accumulator = null
    promise = new Promise((resolve, reject) => {
      resolveFunction = (...args) => {
        resetSharedPromise()
        resolve(...args)
      }
      rejectFunction = reject = (...reason) => {
        resetSharedPromise()
        resolve(...reason)
      }
    })
  }

  resetSharedPromise()

  return (...args) => {
    invocations.push({
      when: Date.now(),
      args,
    })
    const result = promise
    accumulator = settleFunction({
      args,
      accumulator,
      resolve: resolveFunction,
      reject: rejectFunction,
      invocations,
    })
    return result
  }
}
