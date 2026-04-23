import { Pan123NotFoundError, resolvePan123ReleaseDownloadUrl } from "./_pan123";

export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store, max-age=0");
  res.setHeader("X-Robots-Tag", "noindex");

  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const scope = String(req.query.scope || "").trim().toLowerCase();
  const tag = normalizeTag(req.query.tag);
  const relativePath = normalizeRelativePath(req.query.path);

  if (!scope || (scope !== "app" && scope !== "content")) {
    return res.status(400).json({ error: "Invalid scope" });
  }
  if (!tag || !relativePath) {
    return res.status(400).json({ error: "Missing tag or path" });
  }

  try {
    const downloadUrl = await resolvePan123ReleaseDownloadUrl(tag, relativePath);
    res.statusCode = 302;
    res.setHeader("Location", downloadUrl);
    return res.end();
  } catch (error) {
    if (error instanceof Pan123NotFoundError) {
      return res.status(404).json({ error: "File not found" });
    }

    return res.status(502).json({
      error: "Download resolution failed",
      detail: error.message || String(error),
    });
  }
}

function normalizeTag(value) {
  const tag = String(value || "").trim();
  if (!tag || tag.includes("/") || tag === "." || tag === "..") {
    return "";
  }
  return tag;
}

function normalizeRelativePath(value) {
  const raw = Array.isArray(value) ? value.join("/") : String(value || "");
  const parts = raw
    .split("/")
    .map((item) => decodePart(item))
    .map((item) => item.trim())
    .filter(Boolean);

  if (!parts.length || parts.some((item) => item === "." || item === "..")) {
    return "";
  }

  return parts.join("/");
}

function decodePart(value) {
  try {
    return decodeURIComponent(String(value || ""));
  } catch (error) {
    return String(value || "");
  }
}
