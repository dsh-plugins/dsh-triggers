/** 事件协议只跨越 JSON 边界，避免把宿主对象或句柄交给 Provider。 */
export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue }

/** 编排器签发并保存的路由。Provider 无权读取或改变路由。 */
export interface TriggerRoute {
  principal: string
  replyTo: string
}

/** 调度器向编排器交付的稳定事件信封。 */
export interface TriggerEvent {
  eventId: string
  source: string
  occurredAt: string
  payload: JsonValue
  route: TriggerRoute
}

/** 下次执行时间为 UTC ISO 字符串。传 null 时此订阅停止调度。 */
export interface TriggerSchedule {
  nextAt: string | null
  state?: JsonValue
}

/** 定义一份可动态挂载的 TypeScript Provider。初始化负责业务参数校验。 */
export interface TriggerProvider {
  readonly version: 1
  initialize(input: {params: JsonValue; now: string}): TriggerSchedule
  fire(input: {params: JsonValue; state: JsonValue; scheduledAt: string; now: string}): TriggerSchedule & {
    payload?: JsonValue
  }
}

/** 编排器持有此接口。动态 Provider 只接触 TriggerProvider 协议。 */
export interface TriggerPort {
  readonly workspace: string
  setSink(sink: (event: TriggerEvent) => Promise<unknown>): () => void
  add(request: {providerId: string; params?: unknown}, route: TriggerRoute, authorize?: () => void): Promise<{
    id: string; providerId: string; nextAt: string | null
  }>
  remove(id: string, route: TriggerRoute, authorize?: () => void): Promise<boolean>
  publish(request: {providerId: string; source: string}, authorize?: () => void): Promise<{providerId: string; revision: string}>
}
