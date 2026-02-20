const https = require('https');

export default async function handler(req, res) {
  // Security check: Only allow if a special secret token is provided in query
  // e.g. /api/migrate?secret=my-migration-secret
  const secret = process.env.MIGRATION_SECRET || "temp-migration-key";
  if (req.query.secret !== secret) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  
  // Increase timeout limit if possible, but Vercel limits stick (10s on hobby, 60s on pro)
  // So we will process a limited batch each time if needed, or hope it finishes fast.
  // For safety, we'll try to do as much as possible.

  const URL = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
  const TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;

  if (!URL || !TOKEN) {
    return res.status(500).json({ error: "Missing Upstash Env Vars" });
  }

  const counters = {
    city: {},
    brand: {},
    model: {},
    lang: {},
    country: {},
    city_brand: {},
    city_model: {} // New: Try to aggregate city-model if possible (though old keys didn't strictly link them perfectly, we can try best effort if keys exist)
  };
  
  const logs = [];
  const log = (msg) => logs.push(msg);

  try {
    log("Starting aggregation...");

    // Helper functions inside handler
    const upstashCmd = async (command) => {
        const fetchRes = await fetch(`${URL}/pipeline`, {
            method: 'POST',
            headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
            body: JSON.stringify([command]),
        });
        const json = await fetchRes.json();
        return json[0]?.result;
    };

    const upstashPipeline = async (commands) => {
        const fetchRes = await fetch(`${URL}/pipeline`, {
            method: 'POST',
            headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
            body: JSON.stringify(commands),
        });
        return await fetchRes.json();
    };

    const fetchValues = async (keys) => {
      const values = [];
      for (let i = 0; i < keys.length; i += 500) {
        const chunk = keys.slice(i, i + 500);
        const res = await upstashCmd(['MGET', ...chunk]);
        if (res) values.push(...res);
      }
      return values;
    };

    const decode = (s) => { try { return decodeURIComponent(s); } catch (e) { return s; } };
    const encode = (s) => encodeURIComponent(s);
    const increment = (store, key, amount) => { store[key] = (store[key] || 0) + amount; };

    // --- scanning logic ---
    // We limit scanning to avoid timeout. If you have HUGE data, this might need multiple runs with cursor.
    // For now, let's assume it fits in one go or we just do partial.
    
    // Patterns to scan
    const patterns = [
        { pat: 'telemetry:day:*:city:*:brand:*:event:open', type: 'city_brand' },
        { pat: 'telemetry:day:*:model:*:event:open', type: 'model' },
        { pat: 'telemetry:day:*:lang:*:event:open', type: 'lang' },
        { pat: 'telemetry:day:*:country:*:event:open', type: 'country' }
    ];

    for (const p of patterns) {
        let cursor = '0';
        let count = 0;
        // Limit loop to prevent total timeout
        for(let loop=0; loop<50; loop++){ 
            const res = await upstashCmd(['SCAN', cursor, 'MATCH', p.pat, 'COUNT', 1000]);
            if (!res) break;
            cursor = res[0];
            const keys = res[1];
            
            if (keys && keys.length > 0) {
                const values = await fetchValues(keys);
                for (let i = 0; i < keys.length; i++) {
                    const key = keys[i];
                    const val = parseInt(values[i] || '0', 10);
                    if (!val) continue;

                    const parts = key.split(':');
                    
                    if (p.type === 'city_brand') {
                        // key: telemetry:day:DATE:city:CITY:brand:BRAND:event:open
                        const cityIdx = parts.indexOf('city');
                        const brandIdx = parts.indexOf('brand');
                        if (cityIdx > -1 && brandIdx > -1) {
                            const city = decode(parts[cityIdx + 1]);
                            const brand = decode(parts[brandIdx + 1]);
                            
                            increment(counters.city_brand, `telemetry:total:city:${encode(city)}:brand:${encode(brand)}:event:open`, val);
                            increment(counters.city, city, val); // Aggregate city total too
                            increment(counters.brand, brand, val); // Aggregate brand total too
                        }
                    } else if (p.type === 'model') {
                        const idx = parts.indexOf('model');
                        if (idx > -1) increment(counters.model, decode(parts[idx+1]), val);
                    } else if (p.type === 'lang') {
                        const idx = parts.indexOf('lang');
                        if (idx > -1) increment(counters.lang, decode(parts[idx+1]), val);
                    } else if (p.type === 'country') {
                        const idx = parts.indexOf('country');
                        if (idx > -1) increment(counters.country, decode(parts[idx+1]), val);
                    }
                }
                count += keys.length;
            }
            if (cursor === '0') break;
        }
        log(`Scanned ${count} keys for ${p.type}`);
    }

    // --- Writing Back ---
    const pipeline = [];
    const pushSet = (key, val) => pipeline.push(['SET', key, val]);
    const pushIncr = (store, prefix) => {
        Object.entries(store).forEach(([k, v]) => {
            // For simple counters (city, brand...), we construct the key.
            // For composite (city_brand), k is ALREADY the full key.
            if (k.startsWith('telemetry:')) {
                pushSet(k, v);
            } else {
                pushSet(`telemetry:total:${prefix}:${encode(k)}:event:open`, v);
            }
        });
    };

    pushIncr(counters.city, 'city');
    pushIncr(counters.brand, 'brand');
    pushIncr(counters.model, 'model');
    pushIncr(counters.lang, 'lang');
    pushIncr(counters.country, 'country');
    // For city_brand, keys are fully formed in loop above
    Object.entries(counters.city_brand).forEach(([k, v]) => pushSet(k, v));

    if (pipeline.length > 0) {
        // Write in batches
        const BATCH = 500;
        for (let i = 0; i < pipeline.length; i += BATCH) {
             await upstashPipeline(pipeline.slice(i, i + BATCH));
        }
        log(`Successfully wrote ${pipeline.length} total keys.`);
    } else {
        log("No data found to migrate.");
    }

    return res.status(200).json({ ok: true, logs });

  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: error.message, logs });
  }
}
