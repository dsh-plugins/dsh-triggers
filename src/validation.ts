import type { JsonValue } from './protocol.js'

export const maxSourceBytes = 64 * 1024
export const maxJsonBytes = 16 * 1024

export function providerId(value: unknown): string {
  if (typeof value !== 'string' || !/^[a-z][a-z0-9-]{0,63}$/.test(value)) {
    throw new Error('Provider ID 必须由小写字母开头，后接小写字母、数字或短横线')
  }
  return value
}

export function text(value: unknown, limit: number, label: string): string {
  if (typeof value !== 'string' || !value.trim() || value.length > limit || /[\u0000-\u001f\u007f]/u.test(value)) {
    throw new Error(label + '无效')
  }
  return value
}

export function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value) ||
    ![Object.prototype, null].includes(Object.getPrototypeOf(value))) {
    throw new Error('参数必须是 JSON 对象')
  }
  return value as Record<string, unknown>
}

export function json(value: unknown): JsonValue {
  let objects = 0
  function check(item: unknown, depth: number): void {
    if (++objects > 2048 || depth > 24) { throw new Error('JSON 参数或状态过于复杂') }
    if (item === null || typeof item === 'string' || typeof item === 'boolean' ||
      (typeof item === 'number' && Number.isFinite(item))) { return }
    const children = Array.isArray(item) ? item : Object.values(record(item))
    for (const child of children) { check(child, depth + 1) }
  }
  check(value, 0)
  const encoded = JSON.stringify(value)
  if (Buffer.byteLength(encoded) > maxJsonBytes) { throw new Error('JSON 参数、状态或事件载荷超过 16 KiB') }
  return JSON.parse(encoded) as JsonValue
}

export function dateAfter(result: unknown, after: number): number | null {
  if (result === null) { return null }
  const epoch = typeof result === 'string' ? Date.parse(result) : NaN
  if (!Number.isFinite(epoch) || new Date(epoch).toISOString() !== result || epoch < after + 1000) {
    throw new Error('Provider 的下一次执行时间必须为至少一秒之后的 UTC ISO 时间')
  }
  return epoch
}

export function iso(epoch: number): string {
  return new Date(epoch).toISOString()
}
