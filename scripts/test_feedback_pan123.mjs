import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import test from "node:test";

const root = process.env.NIYIEN_FEEDBACK_TEST_ROOT || resolve(dirname(fileURLToPath(import.meta.url)), "..");
const dailyDir = "20261028";
const ok = (data, code = 0, status = 200) => ({
  ok: status >= 200 && status < 300,
  status,
  async text() { return JSON.stringify({ code, data }); },
});

async function harness(scenario, env = {}) {
  const state = { now: Date.UTC(2026, 9, 28, 4), calls: [], logs: [] };
  const budgets = new WeakMap();
  class ClockDate extends Date {
    constructor(...args) { super(...(args.length ? args : [state.now])); }
    static now() { return state.now; }
  }
  state.delay = (ms, signal) => {
    const budget = budgets.get(signal);
    assert.ok(budget, "the complete upstream response needs an abort signal");
    const remaining = budget.ms - (state.now - budget.start);
    state.now += Math.min(ms, remaining);
    if (ms > remaining) throw new DOMException("upstream stalled", "TimeoutError");
  };
  const context = vm.createContext({
    Date: ClockDate,
    URL,
    crypto: { randomUUID },
    console: Object.fromEntries(["log", "warn", "error"].map(level => [level, (...args) => state.logs.push([level, ...args])])),
    process: { env: {
      PAN123_CLIENT_ID: "test-client-id",
      PAN123_CLIENT_SECRET: "test-client-secret",
      PAN123_FEEDBACK_ROOT_ID: "100",
      PAN123_RELEASES_ROOT_ID: "200",
      KV_REST_API_URL: "https://kv.invalid",
      KV_REST_API_TOKEN: "test-kv-token",
      ...env,
    } },
    AbortSignal: { timeout(ms) {
      const signal = new AbortController().signal;
      budgets.set(signal, { ms, start: state.now });
      return signal;
    } },
    async fetch(url, options) {
      const parsed = new URL(url);
      const call = { path: parsed.pathname, query: parsed.searchParams, options, timeoutMs: budgets.get(options.signal)?.ms };
      state.calls.push(call);
      const response = await scenario?.(call, state);
      if (response) return response;
      if (call.path === "/api/v1/access_token") {
        return ok({ accessToken: "test-access-token", expiredAt: new Date(state.now + 3_600_000).toISOString() });
      }
      if (call.path === "/api/v2/file/list") {
        return ok({ fileList: [{ fileId: 101, filename: dailyDir, type: 1, trashed: 0 }], lastFileId: -1 });
      }
      if (call.path === "/upload/v1/file/mkdir") return ok({ dirID: 101 });
      if (call.path === "/pipeline") {
        const commands = JSON.parse(options.body);
        return { ok: true, async json() { return commands.map(() => ({ result: "1" })); } };
      }
      throw new Error(`Unexpected request: ${call.path}`);
    },
  });
  const definitions = {
    "_control-plane": {
      CURRENT_SOURCE_APP_ID: "test", DEFAULT_PRODUCT_ID: "test", LEGACY_SOURCE_APP_ID: "old",
      safeJsonParse(value, fallback = null) { try { return JSON.parse(value); } catch { return fallback; } },
    },
    "_r2": { getR2BucketName: () => "test", signR2PutUrl: async () => ({ url: "https://r2.invalid/test", headers: {} }) },
  };
  const modules = new Map();
  async function load(name) {
    if (modules.has(name)) return modules.get(name);
    const values = definitions[name];
    const module = values
      ? new vm.SyntheticModule(Object.keys(values), function() {
        for (const [key, value] of Object.entries(values)) this.setExport(key, value);
      }, { context, identifier: name })
      : new vm.SourceTextModule(readFileSync(resolve(root, "api", `${name}.js`), "utf8"), { context, identifier: name });
    modules.set(name, module);
    await module.link(specifier => load(specifier.replace(/^\.\//, "")));
    await module.evaluate();
    return module;
  }
  async function submit(country = "CN") {
    const res = { code: 200, body: null, setHeader() {}, status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } };
    const module = await load("feedback");
    await module.namespace.default({
      method: "POST", headers: { "x-forwarded-for": "192.0.2.10" }, query: { country },
      body: { size: 3, sha256: "a".repeat(64), summary: "[TEST] synthetic feedback" },
    }, res);
    return res;
  }
  return { state, load, submit };
}

test("a 12-second token response completes instead of failing at eight seconds", async () => {
  const h = await harness((call, state) => {
    if (call.path === "/api/v1/access_token") state.delay(12_000, call.options.signal);
  });
  const res = await h.submit();
  assert.equal(res.code, 200, JSON.stringify(res.body));
  assert.equal(res.body.upload.kind, "pan123_multipart");
  assert.equal(h.state.calls.find(call => call.path === "/api/v2/file/list").options.headers.Authorization, "Bearer test-access-token");
});

test("one stalled token attempt retries and keeps the same credentials", async () => {
  let tokens = 0;
  const h = await harness((call, state) => {
    if (call.path === "/api/v1/access_token" && ++tokens === 1) state.delay(20_000, call.options.signal);
  });
  assert.equal((await h.submit()).code, 200);
  const calls = h.state.calls.filter(call => call.path === "/api/v1/access_token");
  assert.equal(calls.length, 2);
  assert.equal(calls[0].timeoutMs, 15_000);
  assert.equal(calls[0].options.body, calls[1].options.body);
  assert.equal(h.state.logs.filter(entry => entry[1] === "[feedback] 123 API retry").length, 1);
});

test("token and directory reads retry transient HTTP errors", async () => {
  for (const path of ["/api/v1/access_token", "/api/v2/file/list"]) {
    let calls = 0;
    const h = await harness(call => call.path === path && ++calls === 1 ? ok({}, 0, 503) : undefined);
    assert.equal((await h.submit()).code, 200);
    assert.equal(calls, 2);
  }
});

test("token transport failures retry without retrying permission or API errors", async () => {
  for (const mode of ["network", "permission", "api"]) {
    let calls = 0;
    const h = await harness(call => {
      if (call.path !== "/api/v1/access_token") return;
      calls++;
      if (mode === "network" && calls === 1) throw new TypeError("fetch failed", { cause: { code: "UND_ERR_CONNECT_TIMEOUT" } });
      if (mode === "permission") return ok({}, 0, 403);
      if (mode === "api") return ok({}, 401);
    });
    assert.equal((await h.submit()).code, mode === "network" ? 200 : 500);
    assert.equal(calls, mode === "network" ? 2 : 1);
  }
});

test("the deadline covers token response bodies and all directory pages", async () => {
  let tokenBodies = 0;
  const h = await harness((call, state) => {
    if (call.path === "/api/v1/access_token" && tokenBodies++ === 0) {
      return { ok: true, status: 200, async text() { state.delay(20_000, call.options.signal); } };
    }
  });
  assert.equal((await h.submit()).code, 200);
  assert.equal(tokenBodies, 2);

  const paged = await harness((call, state) => {
    if (call.path === "/api/v2/file/list") {
      state.delay(11_000, call.options.signal);
      return ok({ fileList: [], lastFileId: state.calls.length });
    }
  });
  const start = paged.state.now;
  const res = await paged.submit();
  assert.equal(res.code, 500);
  assert.equal(paged.state.now - start, 40_000);
  assert.equal(paged.state.calls.filter(call => call.path === "/api/v2/file/list").length, 4);
});

test("directory creation is not repeated after an ambiguous timeout", async () => {
  let creates = 0;
  const h = await harness((call, state) => {
    if (call.path === "/api/v2/file/list") return ok({ fileList: [], lastFileId: -1 });
    if (call.path === "/upload/v1/file/mkdir") {
      creates++;
      state.delay(20_000, call.options.signal);
    }
  });
  assert.equal((await h.submit()).code, 500);
  assert.equal(creates, 1);
});

test("failed token attempts release the shared promise for the next submission", async () => {
  let fail = true;
  let tokens = 0;
  const h = await harness((call, state) => {
    if (call.path !== "/api/v1/access_token") return;
    tokens++;
    if (fail) state.delay(20_000, call.options.signal);
  });
  const failed = await h.submit();
  assert.equal(failed.code, 500);
  assert.match(failed.body.detail, /123 API timeout after 15000ms: \/api\/v1\/access_token/);
  assert.ok(h.state.logs.some(entry => entry[1] === "[feedback] upload signing failed"));
  assert.doesNotMatch(JSON.stringify(h.state.logs), /test-client-secret|test-access-token|test-kv-token/);
  fail = false;
  assert.equal((await h.submit()).code, 200);
  assert.equal(tokens, 3);
});

test("ordinary downloads retain their eight-second request limit and single attempt", async () => {
  let tokens = 0;
  const h = await harness((call, state) => {
    if (call.path === "/api/v1/access_token") {
      tokens++;
      state.delay(12_000, call.options.signal);
    }
  });
  const module = await h.load("_pan123");
  await assert.rejects(module.namespace.resolvePan123ReleaseDownloadUrl("run-test", "test.zip"), /timeout after 8000ms/);
  assert.equal(tokens, 1);
});

test("global feedback still uses R2 without calling 123", async () => {
  const h = await harness();
  const res = await h.submit("US");
  assert.equal(res.code, 200);
  assert.equal(res.body.upload.kind, "r2_presigned_put");
  assert.ok(h.state.calls.every(call => call.path === "/pipeline"));
});

test("pending-record write errors remain visible after successful signing", async () => {
  const h = await harness(call => {
    if (call.path === "/pipeline" && JSON.parse(call.options.body)[0][0] === "SET") {
      return { ok: false, status: 503 };
    }
  });
  const res = await h.submit();
  assert.equal(res.code, 500);
  assert.equal(res.body.error, "KV write failed");
  assert.ok(h.state.logs.some(entry => entry[1] === "[feedback] pending record write failed"));
});
