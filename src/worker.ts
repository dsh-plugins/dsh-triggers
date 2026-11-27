import { performance } from 'node:perf_hooks'
import { getQuickJS } from 'quickjs-emscripten'
import ts from 'typescript'
import type { ProcessorRequest } from './sandbox.js'
import { maxJsonBytes, maxSourceBytes } from './validation.js'

async function evaluate(request: ProcessorRequest): Promise<unknown> {
  const {source, operation, input} = request
  if (typeof source !== 'string' || Buffer.byteLength(source) > maxSourceBytes ||
    !['inspect', 'initialize', 'fire'].includes(operation)) {
    throw new Error('Provider 请求无效')
  }
  const compiled = ts.transpileModule(source, {
    compilerOptions: {module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022},
    fileName: 'provider.ts',
    reportDiagnostics: true,
  })
  if (compiled.diagnostics?.some(item => item.category === ts.DiagnosticCategory.Error)) {
    throw new Error('Provider TypeScript 语法无效')
  }
  const module = await getQuickJS()
  using runtime = module.newRuntime()
  runtime.setMemoryLimit(8 * 1024 * 1024)
  runtime.setMaxStackSize(512 * 1024)
  const deadline = performance.now() + 250
  runtime.setInterruptHandler(() => performance.now() >= deadline)
  using vm = runtime.newContext()
  const program = [
    '(() => {',
    'const stringify = JSON.stringify.bind(JSON);',
    'const parse = JSON.parse.bind(JSON);',
    'const provider = (() => {',
    'const exports = Object.create(null);',
    compiled.outputText,
    'return exports.default;',
    '})();',
    "if (!provider || provider.version !== 1 || typeof provider.initialize !== 'function' || typeof provider.fire !== 'function') {",
    "return '__invalid_protocol__';",
    '}',
    ...(operation === 'inspect' ? ['return "null";'] : [
      'const result = provider[' + JSON.stringify(operation) + '](parse(' + JSON.stringify(JSON.stringify(input)) + '));',
      'const encoded = stringify(result, (_key, value) => {',
      "if (value === undefined || typeof value === 'function' || typeof value === 'symbol' || typeof value === 'bigint' ||",
      "  (typeof value === 'number' && !Number.isFinite(value))) { throw 'not JSON'; }",
      'return value;',
      '});',
      "if (typeof encoded !== 'string' || encoded.length > " + (maxJsonBytes * 3) + ") { throw 'invalid output'; }",
      'return encoded;',
    ]),
    '})()',
  ].join('\n')
  const evaluated = vm.evalCode(program, 'provider.js')
  if (evaluated.error) {
    evaluated.error.dispose()
    throw new Error('Provider 执行失败或超出资源限制')
  }
  using output = evaluated.value
  const value = vm.getString(output)
  if (value === '__invalid_protocol__') {
    throw new Error('Provider 必须导出版本 1 的 initialize 和 fire 方法')
  }
  if (Buffer.byteLength(value) > maxJsonBytes * 3) {
    throw new Error('Provider 输出过大')
  }
  return JSON.parse(value)
}

process.once('disconnect', () => process.exit())
process.once('message', async (raw: unknown) => {
  let result: Record<string, unknown>
  try {
    result = {ok: true, value: await evaluate(raw as ProcessorRequest)}
  } catch (error) {
    result = {ok: false, error: error instanceof Error ? error.message.slice(0, 120) : 'Provider 执行失败'}
  }
  process.send?.(result, undefined, undefined, error => process.exit(error ? 1 : 0))
})
