import { spawn } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { record } from './validation.js'

export interface ProcessorRequest {
  source: string
  operation: 'inspect' | 'initialize' | 'fire'
  input?: unknown
}

const workerFile = fileURLToPath(new URL('./worker.js', import.meta.url))
const wallTimeMs = 10000
const maxProcesses = 4

/** 子进程接收源码和 JSON，宿主不会把 Provider 转成可直接执行的 Node 模块。 */
export class ProviderProcesses {
  private readonly tasks = new Map<ChildProcess, Promise<unknown>>()
  private stopped = false

  async run(request: ProcessorRequest): Promise<unknown> {
    if (this.stopped || this.tasks.size >= maxProcesses) {
      throw new Error('Provider 执行器当前不可用')
    }
    const worker = spawn(process.execPath, ['--max-old-space-size=96', workerFile], {
      env: {},
      stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
      windowsHide: true,
    })
    const task = new Promise<unknown>((resolve, reject) => {
      let response: unknown
      let failure: Error | undefined
      const timeout = setTimeout(() => {
        failure = new Error('Provider 执行超时')
        worker.kill()
      }, wallTimeMs)
      worker.once('message', frame => { response = frame })
      worker.once('error', () => {
        failure = new Error('无法启动 Provider 子进程')
      })
      worker.once('close', code => {
        clearTimeout(timeout)
        if (failure || code !== 0) {
          reject(failure ?? new Error('Provider 子进程异常结束'))
          return
        }
        try {
          const result = record(response)
          if (result.ok !== true) {
            throw new Error(typeof result.error === 'string' ? result.error : 'Provider 执行失败')
          }
          resolve(result.value)
        } catch (error) {
          reject(error)
        }
      })
      worker.send!(request, error => {
        if (error) {
          failure = new Error('无法向 Provider 子进程发送请求')
          worker.kill()
        }
      })
    })
    this.tasks.set(worker, task)
    try {
      return await task
    } finally {
      this.tasks.delete(worker)
    }
  }

  async close(): Promise<void> {
    this.stopped = true
    for (const worker of this.tasks.keys()) { worker.kill() }
    await Promise.allSettled(this.tasks.values())
  }
}
