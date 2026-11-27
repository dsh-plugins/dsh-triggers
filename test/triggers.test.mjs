import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { cronParams, nextCron } from '../dist/cron.js'
import { TriggerSubscriptions } from '../dist/triggers.js'
import { json } from '../dist/validation.js'

const route = {principal: 'web:probe', replyTo: 'probe-conversation'}
const counter = await readFile(new URL('./fixtures/counter.ts', import.meta.url), 'utf8')

async function waitFor(check) {
  for (let index = 0; index < 200; index++) {
    if (check()) { return }
    await sleep(25)
  }
  throw new Error('未收到事件')
}

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-triggers-'))
  const filename = join(directory, 'triggers.sqlite')
  let now = Date.parse('2034-08-01T00:00:00.000Z')
  let active
  t.after(async () => {
    await active?.close()
    await rm(directory, {recursive: true, force: true})
  })
  return {
    start() {
      active = new TriggerSubscriptions(directory, filename, () => {}, () => now)
      return active
    },
    advance(ms) { now += ms },
  }
}

test('Cron calculates the same wall time in its announced zone and rejects malformed arguments', () => {
  const start = Date.parse('2034-08-01T00:00:00.000Z')
  const singapore = cronParams({expression: '0 9 * * *', timezone: 'Asia/Singapore'})
  assert.equal(new Date(nextCron(singapore, start)).toISOString(), '2034-08-01T01:00:00.000Z')
  assert.throws(() => cronParams({expression: '0 9 * * *', timezone: '+08:00'}), /IANA/)
  assert.throws(() => cronParams({expression: 'invalid'}), /Cron 表达式/)
  assert.throws(() => nextCron(cronParams({expression: '81 9 * * *'}), start))
  assert.throws(() => json({state: '余'.repeat(6000)}), /16 KiB/)
  const cycle = {}
  cycle.nested = cycle
  assert.throws(() => json(cycle), /复杂/)
})

test('private state advances once and a failed delivery replays the same event after restart', async t => {
  const clock = await fixture(t)
  const first = clock.start()
  const published = await first.publish({providerId: 'counter', source: counter})
  await assert.rejects(first.add({providerId: 'counter', params: {}}, route), /Provider 执行失败/)
  const subscription = await first.add({providerId: 'counter', params: 4}, route)
  const attempts = []
  clock.advance(1000)
  first.setSink(async event => {
    attempts.push(event)
    throw new Error('Fixture sink unavailable')
  })
  await waitFor(() => attempts.length === 1)
  await first.close()
  clock.advance(1000)
  const second = clock.start()
  assert.equal((await second.publish({providerId: 'counter', source: counter})).revision, published.revision)
  const events = []
  second.setSink(async event => { events.push(event) })
  await waitFor(() => events.length === 2)
  assert.deepEqual(events[0], attempts[0])
  assert.equal(events[0].occurredAt, subscription.nextAt)
  assert.deepEqual(events[0].route, route)
  assert.deepEqual(events[0].payload, {value: 4, host: ['undefined', 'undefined', 'undefined', 'undefined']})
  assert.equal(events[1].payload.value, 5)
  assert.notEqual(events[1].eventId, events[0].eventId)
  assert.equal(await second.remove(subscription.id, {...route, replyTo: 'other-conversation'}), false)
  assert.equal(await second.remove(subscription.id, route), true)
})

test('Cron drops unjournaled downtime slots and delivers the next scheduled run', async t => {
  const clock = await fixture(t)
  const first = clock.start()
  await first.add({providerId: 'cron', params: {expression: '* * * * *', payload: {reason: 'fixture'}}}, route)
  await first.close()
  clock.advance(150000)
  const second = clock.start()
  const events = []
  second.setSink(async event => { events.push(event) })
  await sleep(20)
  assert.deepEqual(events, [])
  clock.advance(30000)
  second.setSink(async event => { events.push(event) })
  await waitFor(() => events.length === 1)
  assert.equal(events[0].source, 'cron')
  assert.equal(events[0].occurredAt, '2034-08-01T00:03:00.000Z')
  assert.deepEqual(events[0].payload, {reason: 'fixture'})
})

test('invalid or runaway definitions cannot displace the active Provider revision', async t => {
  const clock = await fixture(t)
  const triggers = clock.start()
  const request = {providerId: 'counter'}
  await triggers.publish({...request, source: counter})
  await assert.rejects(triggers.publish({...request, source: 'export default {}'}), /版本 1/)
  await assert.rejects(triggers.publish({...request, source: 'while(true) {}\n' + counter}), /执行失败或超出资源限制/)
  await assert.rejects(triggers.publish({...request, source: "import 'fixture-only';\n" + counter}), /执行失败或超出资源限制/)
  const result = await triggers.add({...request, params: 2}, route)
  assert.equal(result.providerId, request.providerId)
  assert.ok(result.nextAt)
})

test('revoked authorization prevents committing a validated provider or subscription', async t => {
  const clock = await fixture(t)
  const triggers = clock.start()
  let allowed = true
  const authorize = () => {
    if (!allowed) { throw new Error('授权已撤销') }
  }
  const rejected = triggers.publish({providerId: 'revoked', source: counter}, authorize)
  allowed = false
  await assert.rejects(rejected, /授权已撤销/)
  await assert.rejects(triggers.add({providerId: 'revoked', params: 1}, route), /不存在/)

  allowed = true
  const request = {providerId: 'counter', params: 1}
  await triggers.publish({providerId: 'counter', source: counter}, authorize)
  const active = await triggers.add(request, route, authorize)
  const pending = triggers.add(request, route, authorize)
  allowed = false
  await assert.rejects(pending, /授权已撤销/)
  await assert.rejects(triggers.remove(active.id, route, authorize), /授权已撤销/)
  allowed = true
  assert.equal(await triggers.remove(active.id, route, authorize), true)
})
