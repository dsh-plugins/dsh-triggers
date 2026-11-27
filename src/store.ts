import { createHash, randomUUID } from 'node:crypto'
import { chmodSync } from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import { advanceMsp } from './msp.js'
import type { MspParams, MspPayload } from './msp.js'
import type { JsonValue, TriggerEvent, TriggerRoute } from './protocol.js'
import { iso } from './validation.js'

export interface Subscription {
  id: string
  provider: string
  revision: string | null
  principal: string
  reply_to: string
  params: string
  state: string
  next_at: number
  failures: number
  source: string | null
}

interface Delivery {
  id: string
  occurred_at: number
  payload: string
  lease: string
  attempts: number
  provider: string
  revision: string | null
  principal: string
  reply_to: string
}

type NewSubscription = Omit<Subscription, 'next_at' | 'failures' | 'source'> & {next_at: number | null}

const leaseMs = 30000
const mspAllowlist = "(s.provider <> 'msp' OR s.revision IS NOT NULL OR json_extract(o.payload, '$.nodeId') IN (SELECT value FROM json_each(?)))"

export class TriggerStore {
  private readonly db: DatabaseSync

  constructor(filename: string) {
    this.db = new DatabaseSync(filename)
    try {
      chmodSync(filename, 0o600)
      this.db.exec('PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON')
      this.db.exec(
        'CREATE TABLE IF NOT EXISTS revisions (' +
        '  provider TEXT NOT NULL, revision TEXT NOT NULL, source TEXT NOT NULL, PRIMARY KEY(provider, revision)' +
        ');' +
        'CREATE TABLE IF NOT EXISTS providers (' +
        '  provider TEXT PRIMARY KEY, revision TEXT NOT NULL,' +
        '  FOREIGN KEY(provider, revision) REFERENCES revisions(provider, revision)' +
        ');' +
        'CREATE TABLE IF NOT EXISTS subscriptions (' +
        '  id TEXT PRIMARY KEY, provider TEXT NOT NULL, revision TEXT, principal TEXT NOT NULL, reply_to TEXT NOT NULL,' +
        '  params TEXT NOT NULL, state TEXT NOT NULL, next_at INTEGER, retry_at INTEGER NOT NULL DEFAULT 0,' +
        '  failures INTEGER NOT NULL DEFAULT 0,' +
        '  FOREIGN KEY(provider, revision) REFERENCES revisions(provider, revision)' +
        ');' +
        'CREATE TABLE IF NOT EXISTS outbox (' +
        '  id TEXT PRIMARY KEY, subscription TEXT NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,' +
        '  occurred_at INTEGER NOT NULL, payload TEXT NOT NULL, next_attempt INTEGER NOT NULL,' +
        '  lease TEXT, lease_until INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0' +
        ');' +
        'CREATE INDEX IF NOT EXISTS outbox_subscription ON outbox(subscription);'
      )
    } catch (error) {
      this.db.close()
      throw error
    }
  }

  publish(provider: string, revision: string, source: string): void {
    this.transaction(() => {
      this.db.prepare('INSERT OR IGNORE INTO revisions VALUES (?, ?, ?)').run(provider, revision, source)
      this.db.prepare(
        'INSERT INTO providers VALUES (?, ?) ON CONFLICT(provider) DO UPDATE SET revision=excluded.revision'
      ).run(provider, revision)
    })
  }

  active(provider: string): {revision: string; source: string} | undefined {
    return this.db.prepare(
      'SELECT p.revision, r.source FROM providers p JOIN revisions r USING(provider, revision) WHERE p.provider=?'
    ).get(provider) as {revision: string; source: string} | undefined
  }

  add(subscription: NewSubscription): void {
    const result = this.db.prepare(
      'INSERT INTO subscriptions(id, provider, revision, principal, reply_to, params, state, next_at)' +
      " SELECT ?, ?, ?, ?, ?, ?, ?, ? WHERE ? <> 'msp' OR (SELECT COUNT(*) FROM subscriptions WHERE provider='msp' AND revision IS NULL) < 128"
    ).run(subscription.id, subscription.provider, subscription.revision, subscription.principal,
      subscription.reply_to, subscription.params, subscription.state, subscription.next_at, subscription.provider)
    if (!result.changes) { throw new Error('当前配置档已达到 128 个 MSP 订阅的上限') }
  }

  remove(id: string, route: TriggerRoute): boolean {
    return this.db.prepare('DELETE FROM subscriptions WHERE id=? AND principal=? AND reply_to=?')
      .run(id, route.principal, route.replyTo).changes === 1
  }

  due(now: number): Subscription[] {
    return this.db.prepare(
      'SELECT s.*, r.source FROM subscriptions s LEFT JOIN revisions r USING(provider, revision)' +
      ' WHERE s.next_at <= ? AND s.retry_at <= ? ORDER BY s.next_at, s.id LIMIT 16'
    ).all(now, now) as unknown as Subscription[]
  }

  /** 推进排期、保存私有状态和记录事件必须同时生效。 */
  advance(subscription: Subscription, nextAt: number | null, state: JsonValue, payload?: JsonValue): void {
    this.transaction(() => {
      const changed = this.db.prepare(
        'UPDATE subscriptions SET next_at=?, state=?, retry_at=0, failures=0 WHERE id=? AND next_at=?'
      ).run(nextAt, JSON.stringify(state), subscription.id, subscription.next_at).changes
      if (changed !== 1 || payload === undefined) { return }
      const id = createHash('sha256').update(JSON.stringify([subscription.id, subscription.next_at])).digest('hex')
      this.db.prepare(
        'INSERT INTO outbox(id, subscription, occurred_at, payload, next_attempt) VALUES (?, ?, ?, ?, 0)'
      ).run(id, subscription.id, subscription.next_at, JSON.stringify(payload))
    })
  }

  fail(subscription: Subscription, nextAttempt: number): void {
    this.db.prepare('UPDATE subscriptions SET retry_at=?, failures=failures+1 WHERE id=? AND next_at=? AND failures=?')
      .run(nextAttempt, subscription.id, subscription.next_at, subscription.failures)
  }

  /** 同一事务记录信号并推进变化检测与冷却状态，重启后沿用事件 ID。 */
  recordMsp(payload: MspPayload, now: number): boolean {
    return this.transaction(() => {
      const rows = this.db.prepare(
        'SELECT s.id, s.params, s.state FROM subscriptions s' +
        " WHERE s.provider='msp' AND s.revision IS NULL AND json_extract(s.params, '$.nodeId')=?" +
        ' AND (SELECT COUNT(*) FROM outbox o WHERE o.subscription=s.id) < 32 ORDER BY s.id'
      ).all(payload.nodeId)
      if (!rows.length) { return false }
      let changed = false
      const update = this.db.prepare('UPDATE subscriptions SET state=? WHERE id=?')
      const enqueue = this.db.prepare('INSERT INTO outbox(id, subscription, occurred_at, payload, next_attempt) VALUES (?, ?, ?, ?, 0)')
      const encoded = JSON.stringify(payload)
      for (const row of rows) {
        const params = JSON.parse(String(row.params)) as MspParams
        const state = advanceMsp(params, JSON.parse(String(row.state)), payload, now)
        if (!state) { continue }
        update.run(JSON.stringify(state), row.id)
        enqueue.run(randomUUID(), row.id, now, encoded)
        changed = true
      }
      return changed
    })
  }

  claim(now: number, allowedNodes: readonly string[]): {event: TriggerEvent; attempts: number; lease: string} | undefined {
    return this.transaction(() => {
      const lease = randomUUID()
      const row = this.db.prepare(
        'UPDATE outbox SET lease=?, lease_until=? WHERE id=(' +
        ' SELECT o.id FROM outbox o JOIN subscriptions s ON o.subscription=s.id WHERE next_attempt<=? AND lease_until<=? AND ' +
        mspAllowlist + ' ORDER BY occurred_at, o.rowid LIMIT 1' +
        ') RETURNING id'
      ).get(lease, now + leaseMs, now, now, JSON.stringify(allowedNodes))
      if (!row) { return }
      const delivery = this.db.prepare(
        'SELECT o.*, s.provider, s.revision, s.principal, s.reply_to FROM outbox o' +
        ' JOIN subscriptions s ON o.subscription=s.id WHERE o.id=?'
      ).get(String(row.id)) as unknown as Delivery
      return {
        lease, attempts: delivery.attempts,
        event: {
          eventId: delivery.id, source: delivery.provider, occurredAt: iso(delivery.occurred_at),
          payload: JSON.parse(delivery.payload) as JsonValue,
          route: {principal: delivery.principal, replyTo: delivery.reply_to},
          ...(delivery.provider === 'msp' && delivery.revision === null ? {kind: 'external.trigger' as const} : {}),
        },
      }
    })
  }

  acknowledge(id: string, lease: string): void {
    this.db.prepare('DELETE FROM outbox WHERE id=? AND lease=?').run(id, lease)
  }

  retry(id: string, lease: string, nextAttempt: number): void {
    this.db.prepare(
      'UPDATE outbox SET next_attempt=?, lease=NULL, lease_until=0, attempts=attempts+1 WHERE id=? AND lease=?'
    ).run(nextAttempt, id, lease)
  }

  nextWake(deliver: boolean, allowedNodes: readonly string[]): number | undefined {
    const schedule = this.db.prepare('SELECT MIN(MAX(next_at, retry_at)) AS at FROM subscriptions').get()?.at
    const outbox = deliver
      ? this.db.prepare(
        'SELECT MIN(MAX(next_attempt, lease_until)) AS at FROM outbox o JOIN subscriptions s ON o.subscription=s.id WHERE ' + mspAllowlist
      ).get(JSON.stringify(allowedNodes))?.at
      : null
    return [schedule, outbox].filter((at): at is number => typeof at === 'number').sort((a, b) => a - b)[0]
  }

  close(): void { this.db.close() }

  private transaction<T>(body: () => T): T {
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const result = body()
      this.db.exec('COMMIT')
      return result
    } catch (error) {
      this.db.exec('ROLLBACK')
      throw error
    }
  }
}
