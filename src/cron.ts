import { Cron } from 'croner'
import { json, record } from './validation.js'
import type { JsonValue } from './protocol.js'

export function cronParams(value: unknown): {expression: string; timezone: string; payload: JsonValue} {
  const params = record(value)
  const expression = typeof params.expression === 'string' ? params.expression.trim() : ''
  const fields = expression.split(/\s+/u)
  if (expression.length > 160 || (fields.length !== 5 && fields.length !== 6)) {
    throw new Error('Cron 表达式必须包含五段或六段')
  }
  if (Object.keys(params).some(key => !['expression', 'timezone', 'payload'].includes(key))) {
    throw new Error('Cron 参数只能包含 expression、timezone 和 payload')
  }
  const timezone = params.timezone ?? 'UTC'
  if (typeof timezone !== 'string' || !timezone || timezone.length > 96 || /^[+-]/u.test(timezone)) {
    throw new Error('Cron 时区必须是有效的 IANA 时区')
  }
  new Intl.DateTimeFormat('en', {timeZone: timezone})
  return {expression, timezone, payload: json(params.payload ?? null)}
}

export function nextCron(params: {expression: string; timezone: string}, after: number): number | null {
  return new Cron(params.expression, {timezone: params.timezone, paused: true}).nextRun(new Date(after))?.getTime() ?? null
}
