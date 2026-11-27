import {
  MSPNode, SignalType, decodeSignal, isValidNodeId, isValidSignalPath, patternMatches,
} from '@modelsensingprotocol/msp-ts'
import type { Endpoint, Signal } from '@modelsensingprotocol/msp-ts'
import { createHash } from 'node:crypto'
import { lstat, open } from 'node:fs/promises'
import { isIP } from 'node:net'
import { isAbsolute, join, normalize } from 'node:path'
import type { JsonValue } from './protocol.js'
import { json, record, text } from './validation.js'

/** 由配置档启用的 MSP 发现桥。节点 ID 是过滤条件，载荷不受信任。 */
export interface MspConfig {
  nodesDir: string
  nodeId?: string
  allowedNodes: readonly string[]
  allowedHosts?: readonly string[]
}

export interface MspParams {
  nodeId: string
  path: string
  cooldownMs: number
  changeOnly: boolean
}

export type MspPayload = {
  kind: 'msp'
  trust: 'untrusted'
  nodeId: string
  path: string
  value: JsonValue
}

interface MspState {
  emittedAt: number
  hashes: [string, string][]
}

const maxPathLength = 256
const maxFrameBytes = 64 * 1024
const reconnectMs = 2000
const loopback = ['127.0.0.1', '::1']

function nodeId(value: unknown): string {
  const id = text(value, 64, 'MSP 节点 ID')
  if (!isValidNodeId(id)) { throw new Error('MSP 节点 ID 无效') }
  return id
}

export function mspConfig(input: MspConfig, workspace: string): Required<MspConfig> {
  const config = record(input)
  const nodesDir = text(config.nodesDir, 4096, 'MSP 发现目录')
  if (!isAbsolute(nodesDir)) { throw new Error('MSP 发现目录必须使用绝对路径') }
  if (!Array.isArray(config.allowedNodes) || !config.allowedNodes.length || config.allowedNodes.length > 64) {
    throw new Error('MSP 配置须填写 1 至 64 个允许连接的节点 ID')
  }
  const id = config.nodeId === undefined
    ? 'prism_' + createHash('sha256').update(workspace).digest('hex').slice(0, 12)
    : nodeId(config.nodeId)
  const allowedNodes = [...new Set(config.allowedNodes.map(nodeId))]
  if (allowedNodes.includes(id)) { throw new Error('MSP 允许列表不能包含棱镜桥自身') }
  const hosts = config.allowedHosts ?? []
  if (!Array.isArray(hosts) || hosts.length > 64 || hosts.some(host => typeof host !== 'string' || !isIP(host))) {
    throw new Error('MSP 允许连接的主机必须为 IP 地址')
  }
  return {nodesDir: normalize(nodesDir), nodeId: id, allowedNodes, allowedHosts: [...new Set([...loopback, ...hosts])]}
}

export function mspParams(input: unknown, allowedNodes: readonly string[]): MspParams {
  const params = record(input)
  if (Object.keys(params).some(key => !['nodeId', 'path', 'cooldownMs', 'changeOnly'].includes(key))) {
    throw new Error('MSP 订阅参数仅支持 nodeId、path、cooldownMs 和 changeOnly')
  }
  const id = nodeId(params.nodeId)
  if (!allowedNodes.includes(id)) { throw new Error('MSP 节点不在配置档的允许列表中') }
  const path = text(params.path, maxPathLength, 'MSP 信号路径')
  if (path !== '*' && path !== '**' && !isValidSignalPath(path.replace(/\.\*\*?$/, ''))) {
    throw new Error('MSP 信号路径须使用点分语法，可在末尾添加 * 或 **')
  }
  const {cooldownMs = 30000, changeOnly = false} = params
  if (typeof cooldownMs !== 'number' || !Number.isInteger(cooldownMs) || cooldownMs < 0 || cooldownMs > 86400000) {
    throw new Error('MSP 冷却时间须为 0 至 86400000 的整数毫秒数')
  }
  if (typeof changeOnly !== 'boolean') { throw new Error('MSP changeOnly 须为布尔值') }
  return {nodeId: id, path, cooldownMs, changeOnly}
}

function sortedJson(value: JsonValue): JsonValue {
  if (Array.isArray(value)) { return value.map(sortedJson) }
  if (value === null || typeof value !== 'object') { return value }
  return Object.fromEntries(Object.keys(value).sort().map(key => [key, sortedJson(value[key])]))
}

export function advanceMsp(params: MspParams, state: MspState | null, event: MspPayload, now: number): MspState | undefined {
  const matches = params.path === '**' || (params.path === '*' ? !event.path.includes('.') : patternMatches(params.path, event.path))
  if (!matches) { return }
  if (state && now - state.emittedAt < params.cooldownMs) { return }
  if (!params.changeOnly) { return {emittedAt: now, hashes: []} }

  const path = createHash('sha256').update(event.path).digest('base64url')
  const value = createHash('sha256').update(JSON.stringify(sortedJson(event.value))).digest('base64url')
  const recent = new Map(state?.hashes)
  if (recent.get(path) === value) { return }
  recent.delete(path)
  recent.set(path, value)
  return {emittedAt: now, hashes: [...recent].slice(-64)}
}

async function discoverEndpoint(path: string, id: string, allowedHosts: readonly string[]): Promise<Endpoint | undefined> {
  try {
    // 只读取允许节点的定长声明，避免解析共享目录中无关的文件。
    if (!(await lstat(path)).isFile()) { return }
    const file = await open(path, 'r')
    try {
      const buffer = Buffer.alloc(4097)
      const {bytesRead} = await file.read(buffer)
      if (bytesRead === buffer.length) { return }
      const declaration = record(JSON.parse(buffer.toString('utf8', 0, bytesRead)))
      if (declaration.node_id !== id) { return }
      const timestamp = declaration.timestamp
      if (timestamp !== undefined && (typeof timestamp !== 'number' || !Number.isFinite(timestamp) ||
          Math.abs(Date.now() - timestamp * 1000) > 30000)) { return }
      const endpoint = record(declaration.endpoint)
      if (endpoint.transport_type !== 'tcp') { return }
      const {host, port} = record(endpoint.address)
      if (typeof host !== 'string' || !allowedHosts.includes(host) || typeof port !== 'number' ||
          !Number.isInteger(port) || port < 1 || port > 65535) { return }
      return {host, port}
    } finally {
      await file.close()
    }
  } catch {
    // 声明可能过期、暂不可读或正在被其他节点替换，下一轮重新读取。
    return
  }
}

export class MspBridge {
  private readonly allowed: Set<string>
  private node?: MSPNode
  private timer?: NodeJS.Timeout
  private startTask?: Promise<void>
  private closed = false

  constructor(
    private readonly config: Required<MspConfig>,
    private readonly accept: (payload: MspPayload) => void,
    private readonly report: (message: string) => void,
  ) {
    this.allowed = new Set(config.allowedNodes)
    this.start()
  }

  private start(): void {
    const {nodesDir, nodeId: id} = this.config
    const node = new MSPNode(id, {nodesDir, host: '127.0.0.1', connectPolicy: 'lazy'})
    this.node = node
    node.onConnected(peerId => {
      const connection = node.protocol.connections.get(peerId)
      if (!connection) { return }
      if (this.closed || this.node !== node || !this.allowed.has(peerId)) {
        connection.close()
        return
      }
      // SDK 公开订阅回调只传信封。使用连接登记的节点复核信封中的来源。
      const next = connection.onData
      connection.onData = frame => {
        if (connection.isClosed) { return }
        if (frame.length > maxFrameBytes) {
          connection.close()
          return
        }
        let signal: Signal
        try {
          signal = decodeSignal(frame)
        } catch {
          connection.close()
          return
        }
        if (signal.source !== peerId) { return }
        if (signal.type === SignalType.SIGNAL) { this.receive(peerId, signal) }
        next(frame)
      }
    })
    this.startTask = node.start().then(() => {
      if (this.closed) { return }
      const dialling = new Set<string>()
      const dial = async (peerId: string) => {
        if (this.closed || dialling.has(peerId) || node.protocol.connections.has(peerId)) { return }
        dialling.add(peerId)
        try {
          const endpoint = await discoverEndpoint(join(nodesDir, `${peerId}.json`), peerId, this.config.allowedHosts)
          if (endpoint && !this.closed && this.node === node && !node.protocol.connections.has(peerId)) {
            await node.protocol.dial(endpoint)
          }
        } finally {
          dialling.delete(peerId)
        }
      }
      const poll = () => {
        for (const peerId of this.allowed) { void dial(peerId).catch(() => this.report('MSP 节点连接失败，将重试')) }
      }
      poll()
      this.timer = setInterval(poll, reconnectMs)
      this.timer.unref()
    }).catch(() => {
      this.stop()
      if (this.closed) { return }
      this.report('MSP 发现桥启动失败，将重试')
      this.timer = setTimeout(() => this.start(), reconnectMs)
      this.timer.unref()
    })
  }

  private receive(peerId: string, signal: Signal): void {
    if (this.closed || typeof signal.path !== 'string' || signal.path.length > maxPathLength || !isValidSignalPath(signal.path) ||
        (signal.target !== null && signal.target !== this.config.nodeId)) { return }
    let payload: MspPayload
    try {
      payload = {
        kind: 'msp', trust: 'untrusted', nodeId: peerId, path: signal.path, value: json(record(signal.payload)),
      }
      json(payload)
    } catch {
      return
    }
    try {
      this.accept(payload)
    } catch {
      this.report('无法将 MSP 信号写入事件队列')
    }
  }

  private stop(): void {
    clearTimeout(this.timer)
    this.node?.stop()
    this.node = undefined
  }

  async close(): Promise<void> {
    this.closed = true
    clearTimeout(this.timer)
    await this.startTask
    this.stop()
  }
}
