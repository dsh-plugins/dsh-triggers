# dsh-triggers

棱镜事件触发插件将 Cron 和动态 TypeScript Provider 的信号投递给创建订阅的 L1。编排器仍负责校验投递目标是否有效。

## 安装

构建后可用本地包验证。先把支持触发器协议的编排器装入同一个 DSH Web 配置档。

```powershell
npm ci
npm run build
npm test
npm pack
dsh plugin --profile web add ./dsh-prism-dsh-triggers-0.1.0.tgz
```

插件把事件编写工作区固定在当前配置档的 `data/dsh-prism/workspace/events/`，将订阅、Provider 修订、实例状态和待投递事件保存在相邻的 `data/dsh-prism/triggers.sqlite`。

## 内置 Cron

订阅 Cron 时传入 `expression`。它支持五段或六段表达式，六段的第一项为秒。可选的 `timezone` 使用 IANA 时区，默认为 UTC。可选的 `payload` 会随事件传给 L1。

```json
{
  "providerId": "cron",
  "params": {
    "expression": "0 9 * * *",
    "timezone": "Asia/Singapore",
    "payload": { "topic": "整理今日项目进度" }
  }
}
```

重启时跳过停机期间还未记录的 Cron 时间点。已经记录的事件会沿用原来的事件 ID 补投。删除订阅会取消未来排期和尚未提交给编排器的事件。

## TypeScript Provider

Provider 默认导出版本 1 对象。初始化校验参数并返回首个时间点。执行回调可以返回事件载荷、下一次时间和要为当前订阅保存的私有状态。时间使用 UTC ISO 字符串，下一次时间至少在调用时间后一秒。返回 `nextAt: null` 结束排期。省略执行结果的 `payload` 只推进排期。

```typescript
import type { TriggerProvider } from '@dsh-prism/dsh-triggers/protocol'

const provider: TriggerProvider = {
  version: 1,
  initialize({params, now}) {
    if (typeof params !== 'string' || !params.trim()) {
      throw new Error('请填写提醒内容')
    }
    return {nextAt: new Date(Date.parse(now) + 60000).toISOString()}
  },
  fire({params}) {
    return {nextAt: null, payload: {reminder: params}}
  },
}

export default provider
```

Provider 在独立进程中的 QuickJS 环境运行。它接收 JSON 参数、时间和实例状态，可以返回 JSON 排期与事件。运行环境没有 Node.js、文件或网络接口。TypeScript 类型导入在编译时移除。同一 Provider 更新后，新订阅使用新修订，原有订阅继续使用创建时的修订。
