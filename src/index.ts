import type { Context } from '@deepseek-ai/cordis'
import { mkdir, realpath } from 'node:fs/promises'
import { join, relative } from 'node:path'
import { TriggerSubscriptions } from './triggers.js'
import type { MspConfig } from './msp.js'
import type { TriggerPort } from './protocol.js'

export type { JsonValue, TriggerEvent, TriggerPort, TriggerProvider, TriggerRoute, TriggerSchedule } from './protocol.js'
export type { MspConfig } from './msp.js'

/** 可选的 MSP 连接配置。 */
export interface Config {
  msp?: MspConfig
}

export const name = '@dsh-prism/dsh-triggers'
export const inject = ['profileContext']

declare module '@deepseek-ai/cordis' {
  interface Context {
    prismTriggers: TriggerPort
  }
}

export async function apply(ctx: Context, config: Config = {}): Promise<() => Promise<void>> {
  const profile = ctx.get('profileContext') as {dir: string}
  const parent = await realpath(profile.dir)
  let directory = parent
  for (const segment of ['data', 'dsh-prism', 'workspace', 'events']) {
    const target = join(directory, segment)
    await mkdir(target, {recursive: true, mode: 0o700})
    directory = await realpath(target)
    if (relative(target, directory)) { throw new Error('事件工作区路径包含重定向目录') }
  }
  const triggers = new TriggerSubscriptions(
    directory, join(parent, 'data', 'dsh-prism', 'triggers.sqlite'),
    message => ctx.logger.warn(message),
    Date.now, config.msp,
  )
  try {
    ctx.provide('prismTriggers', triggers)
    return () => triggers.close()
  } catch (error) {
    await triggers.close()
    throw error
  }
}
