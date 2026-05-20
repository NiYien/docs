import { buildManifestPayload } from "./_control-plane";

export default async function handler(req, res) {
  // Browser caches 5min (same user reuses response); shared caches (Vercel
  // CDN / generic CDN) must not store, otherwise geo-specific responses leak
  // across users on the same edge node.
  res.setHeader("Cache-Control", "private, max-age=300");
  res.setHeader("Vercel-CDN-Cache-Control", "no-store");
  res.setHeader("CDN-Cache-Control", "no-store");

  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  return res.status(200).json(await buildManifestPayload(req));
}
