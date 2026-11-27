# dsh-triggers

棱镜事件触发插件将 Cron、MSP 和动态 TypeScript Provider 的信号投递给创建订阅的 L1。编排器负责校验投递目标是否有效。

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

## 内置 MSP

在当前配置档的 `cordis.patch.yml` 中为 `dsh-triggers` 填写 `msp`，即可启用单个 MSP 桥。`nodesDir` 是和 MSP 节点共用的绝对路径。`allowedNodes` 填写允许订阅的节点 ID，可通过 `nodeId` 为棱镜桥指定独立 ID。

```yaml
- id: dsh-triggers
  config:
    msp:
      nodesDir: C:/msp/nodes
      allowedNodes: [room_sensor]
```

桥的侦听地址是 `127.0.0.1`。出站时按允许节点的发现声明重试连接，默认接受 `127.0.0.1` 和 `::1`。使用局域网节点时，在 `allowedHosts` 追加声明中的具体 IP。MSP 节点名用于过滤，部署时应为共享目录设置合适的访问权限。信号以不受信任的 `external.trigger` 数据投递。

```json
{
  "providerId": "msp",
  "params": {
    "nodeId": "room_sensor",
    "path": "sensor.temperature.**",
    "cooldownMs": 30000,
    "changeOnly": true
  }
}
```

`path` 支持精确路径、末尾 `.*` 匹配一层和末尾 `.**` 匹配零层或多层。单独 `*` 匹配一个路径段，`**` 匹配任意有效路径。冷却时间按订阅计算，默认 30 秒，设置 `0` 关闭。`changeOnly` 默认关闭。开启后按实际信号路径比较上次入队值，忽略 JSON 对象键的顺序，每个订阅最多记住最近入队的 64 条路径。每个订阅最多保留 32 条待投递信号，队列达到上限时丢弃新信号。

断线时自动重连。MSP 停止期间未接收的信号不会补发，已入队事件会在重启后用原 ID 继续投递。关闭 MSP 桥或移除节点允许项会暂停相应事件的投递。删除订阅会清理其待投递事件。

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
