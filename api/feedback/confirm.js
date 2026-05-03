// POST /api/feedback/confirm — finalize a previously-signed feedback upload.
//
// The client calls this after the upload PUT (R2) or 123 multipart sequence
// reports success. We validate that:
//   - `fb:pending:<id>` still exists (it auto-expires at TTL=300s).
//   - The submitted `size` and `sha256` match the pending record (anti-tamper).
//
// On success we copy the record to `fb:<id>` (no TTL), LPUSH the id onto the
// daily index `fb:index:<yyyymmdd>`, and DEL the pending key.

import { safeJsonParse, upstashCommand, upstashPipeline } from "../_telemetry-shared";

const SHA256_HEX_RE = /^[a-f0-9]{64}$/i;

export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store");

  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const body = typeof req.body === "string" ? safeJsonParse(req.body) : req.body;
  if (!body || typeof body !== "object") {
    return res.status(400).json({ error: "Invalid JSON body" });
  }

  const id = String(body.id || "").trim();
  const size = Number(body.size);
  const sha256 = String(body.sha256 || "").trim().toLowerCase();
  if (!isValidId(id)) {
    return res.status(400).json({ error: "Invalid id" });
  }
  if (!Number.isFinite(size) || size <= 0) {
    return res.status(400).json({ error: "Invalid size" });
  }
  if (!SHA256_HEX_RE.test(sha256)) {
    return res.status(400).json({ error: "Invalid sha256" });
  }

  let pendingRaw;
  try {
    const response = await upstashCommand(["GET", `fb:pending:${id}`]);
    pendingRaw = response?.result || null;
  } catch (error) {
    return res.status(500).json({
      error: "KV read failed",
      detail: error.message || String(error),
    });
  }

  if (!pendingRaw) {
    return res.status(404).json({ error: "Pending record not found or expired" });
  }

  const pending = safeJsonParse(pendingRaw, null);
  if (!pending || typeof pending !== "object") {
    return res.status(500).json({ error: "Pending record corrupted" });
  }

  if (Number(pending.size) !== size || String(pending.sha256).toLowerCase() !== sha256) {
    return res.status(400).json({ error: "size or sha256 mismatch" });
  }

  const yyyymmdd = id.split("-")[0];
  if (!/^\d{8}$/.test(yyyymmdd)) {
    return res.status(400).json({ error: "Malformed id prefix" });
  }

  const confirmedRecord = {
    ...pending,
    confirmed_ts: new Date().toISOString(),
  };

  try {
    // SET full record (no TTL), LPUSH the id onto the daily index, DEL the
    // pending key. Wrapped in a single pipeline for atomicity-ish (Upstash
    // REST pipelines are sent in one HTTP call but not transactional; the
    // SET happens before LPUSH which happens before DEL — order matters so
    // that if LPUSH fails the SET is still durable).
    await upstashPipeline([
      ["SET", `fb:${id}`, JSON.stringify(confirmedRecord)],
      ["LPUSH", `fb:index:${yyyymmdd}`, id],
      ["DEL", `fb:pending:${id}`],
    ]);
  } catch (error) {
    return res.status(500).json({
      error: "KV write failed",
      detail: error.message || String(error),
    });
  }

  return res.status(200).json({ id, status: "confirmed" });
}

function isValidId(id) {
  // <yyyymmdd>-<8 hex chars>
  return /^\d{8}-[a-f0-9]{8}$/.test(String(id || ""));
}
