import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { MSPNode, SignalType, makeSignal } from '@modelsensingprotocol/msp-ts'
import { advanceMsp, mspConfig, mspParams } from '../dist/msp.js'
import { TriggerStore } from '../dist/store.js'
import { TriggerSubscriptions } from '../dist/triggers.js'

const route = {principal: 'web:probe', replyTo: 'conversation'}
const bridgeId = 'fixture_prism'
const sensorId = 'fixture_sensor'
const otherId = 'fixture_other'

async function waitFor(ready) {
  for (let attempt = 0; attempt < 300; attempt++) {
    if (await ready()) { return }
    await sleep(25)
  }
  throw new Error('等待 MSP 夹具超时')
}

test('MSP 路径通配、冷却和变化检测严格按订阅匹配', () => {
  const event = {nodeId: sensorId, path: 'sensor.temperature', value: {b: 'C', a: 22}}
  const params = mspParams({nodeId: sensorId, path: 'sensor.*', cooldownMs: 1000, changeOnly: true}, [sensorId])
  assert.throws(() => mspParams({nodeId: otherId, path: 'sensor.**'}, [sensorId]), /允许列表/)
  assert.throws(() => mspParams({nodeId: sensorId, path: 'sensor.*.temperature'}, [sensorId]), /点分语法/)
  assert.throws(() => mspParams({nodeId: sensorId, path: '*', cooldownMs: 1.5}, [sensorId]), /整数/)
  assert.throws(() => mspConfig({nodesDir: tmpdir(), allowedNodes: [sensorId], allowedHosts: ['localhost']}, tmpdir()), /IP 地址/)
  assert.equal(advanceMsp(params, null, {...event, path: 'sensor.temperature.room'}, 2000), undefined)
  const state = advanceMsp(params, null, event, 2000)
  assert.ok(state)
  assert.equal(advanceMsp(params, state, {...event, value: {a: 22, b: 'C'}}, 3000), undefined)
  assert.equal(advanceMsp(params, state, {...event, value: {a: 23, b: 'C'}}, 2999), undefined)
  assert.ok(advanceMsp(params, state, {...event, value: {a: 23, b: 'C'}}, 3000))
  assert.ok(advanceMsp(params, state, {...event, path: 'sensor.other'}, 3000))
  assert.ok(advanceMsp({...params, path: 'sensor.**'}, state, {...event, path: 'sensor'}, 3000))
})

test('老版本与 MSP 同名的动态订阅仍沿用原协议', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-msp-legacy-'))
  const store = new TriggerStore(join(directory, 'triggers.sqlite'))
  t.after(async () => {
    store.close()
    await rm(directory, {recursive: true, force: true})
  })
  const now = Date.now()
  store.publish('msp', 'legacy-revision', 'export default {}')
  store.add({id: 'old-msp', provider: 'msp', revision: 'legacy-revision', principal: route.principal, reply_to: route.replyTo,
    state: 'null', params: 'null', next_at: now})
  const payload = {legacy: true, nodeId: sensorId}
  store.advance(store.due(now)[0], null, null, payload)
  const {event} = store.claim(now, [])
  assert.equal(event.kind, undefined)
  assert.deepEqual(event.payload, payload)
})

test('真实 SDK 重连后仍按本地节点与信号边界投递，持久化事件暂停后续投', {timeout: 25_000}, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-msp-test-'))
  const nodesDir = join(directory, 'msp')
  const database = join(directory, 'triggers.sqlite')
  const config = {nodesDir, nodeId: bridgeId, allowedNodes: [sensorId, otherId]}
  let now = 1900000000000
  let triggers
  let sensor
  let denied
  const reports = []
  t.after(async () => {
    denied?.stop()
    sensor?.stop()
    await triggers?.close()
    await rm(directory, {recursive: true, force: true})
  })

  const start = msp => new TriggerSubscriptions(directory, database, message => reports.push(message), () => now, msp)
  const connect = async () => {
    sensor = new MSPNode(sensorId, {nodesDir, connectPolicy: 'lazy'})
    await sensor.start()
    await waitFor(() => sensor.connections().includes(bridgeId))
  }
  triggers = start(config)
  await assert.rejects(triggers.publish({providerId: 'msp', source: ''}), /内置/)
  const subscription = await triggers.add({providerId: 'msp', params: {
    nodeId: sensorId, path: 'sensor.**', cooldownMs: 1000, changeOnly: true,
  }}, route)
  await triggers.add({providerId: 'msp', params: {nodeId: otherId, path: '**'}}, route)
  await connect()
  assert.equal(subscription.nextAt, null)

  const events = []
  let dispose = triggers.setSink(async event => { events.push(event) })
  const declaration = JSON.parse(await readFile(join(nodesDir, `${bridgeId}.json`), 'utf8'))
  denied = new MSPNode('unapproved_sensor', {nodesDir, connectPolicy: 'lazy'})
  await denied.start()
  assert.equal(await denied.protocol.dial(declaration.endpoint.address), null)
  sensor.protocol.send(bridgeId, makeSignal(SignalType.SIGNAL, otherId, {path: 'spoofed', payload: {fake: true}}))
  sensor.publish('other.path', {ignored: 'path'})
  sensor.publish('sensor.large', {ignored: 'x'.repeat(17000)})
  const payload = {value: 22, unit: 'C', role: 'administrator', route: {principal: 'external:untrusted'}}
  sensor.publish('sensor.temperature', payload)
  await waitFor(() => events.length >= 1)
  assert.equal(events.length, 1)
  assert.deepEqual(events[0], {
    eventId: events[0].eventId,
    source: 'msp', kind: 'external.trigger', occurredAt: new Date(now).toISOString(), route,
    payload: {kind: 'msp', trust: 'untrusted', nodeId: sensorId, path: 'sensor.temperature', value: payload},
  })

  sensor.stop()
  now += 1000
  await connect()
  sensor.publish('sensor.temperature', {unit: 'C', value: 22, route: payload.route, role: payload.role})
  sensor.publish('sensor.temperature', {value: 23})
  await waitFor(() => events.length >= 2)
  assert.deepEqual(events[1].payload.value, {value: 23})
  sensor.publish('sensor.temperature', {value: 24})
  await sleep(50)
  assert.equal(events.length, 2)

  dispose()
  now += 1000
  sensor.publish('sensor.temperature', {value: 24})
  const db = new DatabaseSync(database, {readOnly: true})
  let pending
  try {
    await waitFor(() => (pending = db.prepare('SELECT id FROM outbox').get()?.id))
  } finally {
    db.close()
  }
  await triggers.close()
  triggers = start()
  await assert.rejects(triggers.add({providerId: 'msp'}, route), /未启用/)
  triggers.setSink(async event => { events.push(event) })
  await sleep(50)
  assert.equal(events.length, 2)
  await triggers.close()
  triggers = start(config)
  dispose = triggers.setSink(async event => { events.push(event) })
  await waitFor(() => events.length >= 3)
  assert.equal(events[2].eventId, pending)
  assert.deepEqual(events[2].payload.value, {value: 24})
  assert.equal(await triggers.remove(subscription.id, {...route, principal: 'web:other'}), false)
  assert.equal(await triggers.remove(subscription.id, route), true)
  dispose()
  assert.deepEqual(reports, [])
})
