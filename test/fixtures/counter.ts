import type { TriggerProvider } from '../../src/protocol.js'

const provider: TriggerProvider = {
  version: 1,
  initialize({params, now}) {
    if (typeof params !== 'number') {
      throw new Error('需要数字')
    }
    return {nextAt: new Date(Date.parse(now) + 1000).toISOString(), state: params}
  },
  fire({state, now}) {
    if (typeof state !== 'number') {
      throw new Error('状态无效')
    }
    return {
      nextAt: new Date(Date.parse(now) + 1000).toISOString(),
      state: state + 1,
      payload: {value: state, host: [typeof process, typeof require, typeof fetch, typeof WebSocket]},
    }
  },
}

export default provider
