import { createHash, randomUUID } from 'node:crypto'
import { cronParams, nextCron } from './cron.js'
import type { JsonValue, TriggerEvent, TriggerPort, TriggerRoute } from './protocol.js'
import { ProviderProcesses } from './sandbox.js'
import { TriggerStore } from './store.js'
import type { Subscription } from './store.js'
import { dateAfter, iso, json, maxSourceBytes, providerId, record, text } from './validation.js'

const maxWakeMs = 30000
const deliveryTimeoutMs = 10000
type Sink = (event: TriggerEvent) => Promise<unknown>

function authenticatedRoute(route: TriggerRoute): TriggerRoute {
  return {
    principal: text(route.principal, 2048, '事件所有者'),
    replyTo: text(route.replyTo, 2048, '事件回复目标'),
  }
}

function retryDelay(failures: number): number {
  return Math.min(300000, 1000 * 2 ** Math.min(failures, 9))
}

function waitForDelivery(deliver: Promise<unknown>, signal: AbortSignal): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason)
    if (signal.aborted) { abort() }
    else { signal.addEventListener('abort', abort, {once: true}) }
    void deliver.then(resolve, reject).finally(() => signal.removeEventListener('abort', abort))
  })
}

export class TriggerSubscriptions implements TriggerPort {
  private readonly store: TriggerStore
  private readonly processes = new ProviderProcesses()
  private readonly shutdown = new AbortController()
  private readonly startedAt: number
  private timer?: NodeJS.Timeout
  private work?: Promise<void>
  private closing?: Promise<void>
  private sink?: Sink

  constructor(
    readonly workspace: string,
    storage: string,
    private readonly report: (message: string) => void,
    private readonly now = Date.now,
  ) {
    this.store = new TriggerStore(storage)
    this.startedAt = this.now()
    this.wake()
  }

  setSink(sink: Sink): () => void {
    this.ensureOpen()
    this.sink = sink
    this.wake()
    return () => {
      if (this.sink === sink) { this.sink = undefined }
    }
  }

  async publish(request: {providerId: string; source: string}, authorize?: () => void): Promise<{providerId: string; revision: string}> {
    this.ensureOpen()
    const id = providerId(request.providerId)
    const {source} = request
    if (id === 'cron') { throw new Error('内置 Cron Provider 不接受替换') }
    if (typeof source !== 'string' || !source.trim() || Buffer.byteLength(source) > maxSourceBytes) {
      throw new Error('Provider 源码须为不超过 64 KiB 的 TypeScript')
    }
    await this.processes.run({source, operation: 'inspect'})
    this.ensureOpen()
    const revision = createHash('sha256').update(source).digest('hex')
    authorize?.()
    this.store.publish(id, revision, source)
    return {providerId: id, revision}
  }

  async add(request: {providerId: string; params?: unknown}, target: TriggerRoute, authorize?: () => void): Promise<{
    id: string; providerId: string; nextAt: string | null
  }> {
    this.ensureOpen()
    const provider = providerId(request.providerId)
    const route = authenticatedRoute(target)
    const now = this.now()
    let params: JsonValue
    let state: JsonValue = null
    let revision: string | null = null
    let nextAt: number | null
    if (provider === 'cron') {
      const cron = cronParams(request.params)
      nextAt = nextCron(cron, now)
      if (nextAt === null) { throw new Error('Cron 没有未来的执行时间') }
      params = cron
    } else {
      const definition = this.store.active(provider)
      if (!definition) { throw new Error('Provider 不存在') }
      revision = definition.revision
      params = json(request.params ?? null)
      const plan = record(await this.processes.run({
        source: definition.source,
        operation: 'initialize',
        input: {params, now: iso(now)},
      }))
      nextAt = dateAfter(plan.nextAt, now)
      state = json(plan.state ?? null)
    }
    this.ensureOpen()
    const id = randomUUID()
    authorize?.()
    this.store.add({
      id, provider, revision, principal: route.principal, reply_to: route.replyTo,
      params: JSON.stringify(params), state: JSON.stringify(state), next_at: nextAt,
    })
    this.wake()
    return {id, providerId: provider, nextAt: nextAt === null ? null : iso(nextAt)}
  }

  async remove(id: string, target: TriggerRoute, authorize?: () => void): Promise<boolean> {
    this.ensureOpen()
    const subscriptionId = text(id, 128, '订阅 ID')
    const route = authenticatedRoute(target)
    authorize?.()
    const removed = this.store.remove(subscriptionId, route)
    this.wake()
    return removed
  }

  close(): Promise<void> {
    if (this.closing) { return this.closing }
    this.shutdown.abort(new Error('事件触发器已停止'))
    clearTimeout(this.timer)
    this.sink = undefined
    this.closing = Promise.allSettled([this.processes.close(), this.work]).then(() => this.store.close())
    return this.closing
  }

  private ensureOpen(): void {
    if (this.shutdown.signal.aborted) { throw new Error('事件触发器已停止') }
  }

  private wake(): void {
    clearTimeout(this.timer)
    if (this.work || this.shutdown.signal.aborted) { return }
    this.work = this.tick().catch(() => this.report('事件队列执行失败')).finally(() => {
      this.work = undefined
      if (this.shutdown.signal.aborted) { return }
      let delay = maxWakeMs
      try {
        const next = this.store.nextWake(!!this.sink)
        if (next !== undefined) { delay = Math.max(0, Math.min(maxWakeMs, next - this.now())) }
      } catch {
        this.report('读取下一次事件执行时间失败')
      }
      this.timer = setTimeout(() => this.wake(), delay)
      this.timer.unref()
    })
  }

  private async tick(): Promise<void> {
    if (this.sink) { await this.deliver() }
    for (const schedule of this.store.due(this.now())) {
      if (this.shutdown.signal.aborted) { return }
      await this.advance(schedule)
    }
    if (this.sink) { await this.deliver() }
  }

  private async advance(schedule: Subscription): Promise<void> {
    try {
      const now = this.now()
      const params: unknown = JSON.parse(schedule.params)
      if (schedule.provider === 'cron') {
        const cron = cronParams(params)
        const nextAt = nextCron(cron, now)
        const payload = schedule.next_at < this.startedAt ? undefined : cron.payload
        this.store.advance(schedule, nextAt, null, payload)
        return
      }
      if (schedule.source === null) { throw new Error('找不到订阅引用的 Provider 版本') }
      const state = json(JSON.parse(schedule.state))
      const plan = record(await this.processes.run({
        source: schedule.source,
        operation: 'fire',
        input: {params, state, scheduledAt: iso(schedule.next_at), now: iso(now)},
      }))
      const nextAt = dateAfter(plan.nextAt, now)
      const nextState = Object.hasOwn(plan, 'state') ? json(plan.state) : state
      const payload = Object.hasOwn(plan, 'payload') ? json(plan.payload) : undefined
      this.ensureOpen()
      this.store.advance(schedule, nextAt, nextState, payload)
    } catch {
      if (this.shutdown.signal.aborted) { return }
      this.store.fail(schedule, this.now() + retryDelay(schedule.failures))
      this.report('Provider ' + schedule.provider + ' 执行失败，订阅将重试')
    }
  }

  private async deliver(): Promise<void> {
    for (let count = 0; count < 16 && !this.shutdown.signal.aborted; count++) {
      const sink = this.sink
      if (!sink) { return }
      const claimed = this.store.claim(this.now())
      if (!claimed) { return }
      const {event, attempts, lease} = claimed
      try {
        await waitForDelivery(sink(event), AbortSignal.any([
          this.shutdown.signal, AbortSignal.timeout(deliveryTimeoutMs),
        ]))
        this.store.acknowledge(event.eventId, lease)
      } catch {
        this.store.retry(event.eventId, lease, this.now() + retryDelay(attempts))
        if (!this.shutdown.signal.aborted) { this.report('Provider ' + event.source + ' 事件投递失败，将重试') }
      }
    }
  }
}
