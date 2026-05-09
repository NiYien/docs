const DEFAULT_SDK_PREFIX = "sdk";

export default async function handler(req, res) {
  const relativePath = normalizePath(req.query.path);
  if (!relativePath) {
    return res.status(400).json({ error: "Missing SDK path" });
  }

  const origin = getSdkOrigin();
  if (!origin) {
    return res.status(503).json({ error: "Missing NIYIEN_SDK_R2_PUBLIC_BASE" });
  }

  const target = buildTargetUrl(origin, relativePath);
  res.setHeader("Cache-Control", "public, max-age=3600, s-maxage=86400");
  res.setHeader("Location", target);
  return res.status(302).end();
}

function getSdkOrigin() {
  return String(process.env.NIYIEN_SDK_R2_PUBLIC_BASE || "").trim().replace(/\/+$/, "");
}

function buildTargetUrl(origin, relativePath) {
  const prefix = String(process.env.NIYIEN_SDK_R2_PREFIX || DEFAULT_SDK_PREFIX)
    .trim()
    .replace(/^\/+|\/+$/g, "");
  const encodedPath = relativePath
    .split("/")
    .map((part) => encodeURIComponent(part))
    .join("/");
  return prefix ? `${origin}/${prefix}/${encodedPath}` : `${origin}/${encodedPath}`;
}

function normalizePath(value) {
  const raw = Array.isArray(value) ? value.join("/") : String(value || "");
  const parts = [];
  for (const part of raw.replace(/\\/g, "/").split("/")) {
    const decoded = decodeURIComponent(part || "").trim();
    if (!decoded || decoded === ".") {
      continue;
    }
    if (decoded === "..") {
      return "";
    }
    parts.push(decoded);
  }
  return parts.join("/");
}
