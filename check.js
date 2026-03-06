// check.js — VK Ads Guard v2.4 (GitHub Actions)
// Читает настройки из Gist, проверяет баннеры VK Ads, пишет лог обратно в Gist.

// fetch встроен в Node.js 18+

const VK_CLIENT_ID     = process.env.VK_CLIENT_ID;
const VK_CLIENT_SECRET = process.env.VK_CLIENT_SECRET;
const GIST_TOKEN       = process.env.GIST_TOKEN;
const GIST_ID          = process.env.GIST_ID;

const API_V2 = "https://ads.vk.com/api/v2/";
const API_V3 = "https://ads.vk.com/proxy/mt/v3/";
const TOKEN_URL = "https://ads.vk.com/api/v2/oauth2/token.json";
const LOG_MAX_ENTRIES = 100;

// ─── Gist helpers ─────────────────────────────────────────────────────────────

async function readGist() {
  const r = await fetch(`https://api.github.com/gists/${GIST_ID}`, {
    headers: {
      "Authorization": `Bearer ${GIST_TOKEN}`,
      "Accept": "application/vnd.github+json",
    },
  });
  if (!r.ok) throw new Error(`Gist read error: ${r.status} ${await r.text()}`);
  const data = await r.json();
  const content = data.files["vk-ads-guard-config.json"]?.content || "{}";
  try { return JSON.parse(content); } catch { return {}; }
}

async function writeGist(config) {
  const r = await fetch(`https://api.github.com/gists/${GIST_ID}`, {
    method: "PATCH",
    headers: {
      "Authorization": `Bearer ${GIST_TOKEN}`,
      "Accept": "application/vnd.github+json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      files: {
        "vk-ads-guard-config.json": {
          content: JSON.stringify(config, null, 2),
        },
      },
    }),
  });
  if (!r.ok) throw new Error(`Gist write error: ${r.status} ${await r.text()}`);
}

// ─── VK OAuth2 ────────────────────────────────────────────────────────────────

let cachedToken = null;

async function getAccessToken(config) {
  // Используем токен из Gist если свежий (< 23 часов)
  if (config.accessToken && config.tokenUpdatedAt) {
    if (Date.now() - config.tokenUpdatedAt < 23 * 60 * 60 * 1000) {
      cachedToken = config.accessToken;
      return config.accessToken;
    }
  }

  const res = await fetch(TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type:    "client_credentials",
      client_id:     VK_CLIENT_ID,
      client_secret: VK_CLIENT_SECRET,
      permanent:     "true",
    }).toString(),
  });

  if (!res.ok) throw new Error(`Auth error: ${res.status} ${await res.text()}`);
  const data = await res.json();
  if (!data.access_token) throw new Error(`No token: ${JSON.stringify(data)}`);

  cachedToken = data.access_token;
  config.accessToken      = data.access_token;
  config.tokenUpdatedAt   = Date.now();
  return data.access_token;
}

async function getSudo(config) {
  if (config.sudo) return config.sudo;
  const data = await vkApiV2("GET", "user.json", {}, null, config);
  if (!data.username) throw new Error("Не удалось получить username");
  config.sudo = data.username;
  return data.username;
}

// ─── HTTP helpers ─────────────────────────────────────────────────────────────

async function vkApiV2(method, endpoint, params = {}, body = null, config, isRetry = false) {
  const token = cachedToken || await getAccessToken(config);
  let url = API_V2 + endpoint;
  if (method === "GET" && Object.keys(params).length)
    url += "?" + new URLSearchParams(params).toString();

  const opts = {
    method,
    headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
  };
  if (body) opts.body = JSON.stringify(body);

  const res = await fetch(url, opts);

  if (res.status === 401 && !isRetry) {
    cachedToken = null;
    await getAccessToken(config);
    return vkApiV2(method, endpoint, params, body, config, true);
  }
  if (res.status === 429 && !isRetry) {
    await sleep(5000);
    return vkApiV2(method, endpoint, params, body, config, true);
  }
  if (!res.ok) throw new Error(
    `HTTP ${res.status} (${endpoint}): ${JSON.stringify(await res.json().catch(() => {}))}`
  );
  if (res.status === 204) return {};
  return res.json();
}

async function vkApiV3(endpoint, queryParams = {}, bodyParams = {}, config, isRetry = false) {
  const token = cachedToken || await getAccessToken(config);
  const url = API_V3 + endpoint + "?" + new URLSearchParams(queryParams).toString();

  const res = await fetch(url, {
    method: "POST",
    headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify(bodyParams),
  });

  if (res.status === 401 && !isRetry) {
    cachedToken = null;
    await getAccessToken(config);
    return vkApiV3(endpoint, queryParams, bodyParams, config, true);
  }
  if (res.status === 429 && !isRetry) {
    await sleep(5000);
    return vkApiV3(endpoint, queryParams, bodyParams, config, true);
  }
  if (!res.ok) throw new Error(
    `HTTP v3 ${res.status} (${endpoint}): ${JSON.stringify(await res.json().catch(() => {}))}`
  );
  return res.json();
}

// ─── Метрики (дублируем логику из shared.js) ──────────────────────────────────

function computeMetric(metric, s) {
  if (!s) return null;
  const spent       = parseFloat(s.spent)        || 0;
  const impressions = parseInt(s.impressions, 10) || 0;
  const clicks      = parseInt(s.clicks, 10)      || 0;
  const cpuCpa      = parseFloat(s.cpu_cpa)       || 0;
  const audioAdds   = parseInt(s.audio_adds, 10)  || 0;

  switch (metric) {
    case "spent": return spent       > 0 ? spent                        : null;
    case "cpm":   return impressions > 0 ? (spent / impressions) * 1000 : null;
    case "cpc":   return clicks      > 0 ? spent / clicks              : null;
    case "cpu":
      if (spent === 0) return null;
      if (audioAdds === 0) return spent;
      return cpuCpa;
    default: return null;
  }
}

function shouldResume(metric, s, threshold) {
  if (!s) return false;
  const spent     = parseFloat(s.spent)        || 0;
  const audioAdds = parseInt(s.audio_adds, 10)  || 0;
  const cpuCpa    = parseFloat(s.cpu_cpa)       || 0;

  switch (metric) {
    case "cpu":
      if (spent === 0) return true;
      if (audioAdds === 0) return spent < threshold;
      return cpuCpa <= threshold;
    default: {
      const value = computeMetric(metric, s);
      return value !== null && value <= threshold;
    }
  }
}

function metricLabel(metric) {
  const labels = { spent: "Расход ₽", cpm: "CPM", cpu: "Цена за добавление аудио" };
  return labels[metric] || metric;
}

function todayDate() { return new Date().toISOString().slice(0, 10); }

function todayDateDMY() {
  const d = new Date();
  return String(d.getDate()).padStart(2,"0") + "." +
         String(d.getMonth()+1).padStart(2,"0") + "." + d.getFullYear();
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── Основная проверка ────────────────────────────────────────────────────────

async function runCheck(config) {
  const logs = [];
  const addLog = (level, message) =>
    logs.push({ level, message, time: new Date().toISOString() });

  const rules        = config.rules || {};
  const guardMode    = config.guardMode || "stop_only";
  const isResumeMode = guardMode === "stop_resume";
  const accountId    = config.accountId;
  const pixelId      = config.pixelId;

  const byGuard = new Set(config.stoppedByGuard || []);
  const byUser  = new Set(config.stoppedByUser  || []);

  const activeRuleIds = Object.keys(rules)
    .map(Number)
    .filter(id => id && rules[String(id)].enabled !== false);

  if (!activeRuleIds.length) {
    addLog("ok", "Нет активных правил.");
    return logs;
  }

  // 1. Загружаем кампании
  const plansResp = await vkApiV2("GET", "ad_plans.json", {
    "_id__in": activeRuleIds.join(","),
    fields: "id,name,status,delivery",
  }, null, config);

  const allPlans = plansResp.items || [];

  const activePlans = allPlans.filter(
    p => p.delivery === "delivering" || p.delivery === "started"
  );
  const resumeCandidatePlans = isResumeMode
    ? allPlans.filter(p => p.delivery !== "delivering" && p.delivery !== "started")
    : [];

  const plansToProcess = [...activePlans, ...resumeCandidatePlans];
  if (!plansToProcess.length) {
    addLog("ok", "Активных кампаний нет.");
    return logs;
  }

  const sudo     = (accountId && pixelId) ? await getSudo(config) : null;
  const today    = todayDate();
  const todayDMY = todayDateDMY();

  let totalStopped = 0;
  let totalResumed = 0;

  for (const plan of plansToProcess) {
    const rule = rules[String(plan.id)];
    if (!rule || rule.enabled === false) continue;

    const isPlanActive = plan.delivery === "delivering" || plan.delivery === "started";

    // 2. Получаем баннеры
    let banners = [];
    try {
      const groupsResp = await vkApiV2("GET", "ad_groups.json", {
        "_ad_plan_id": plan.id,
        limit: 200,
      }, null, config);

      const groups = groupsResp.items || [];
      if (!groups.length) continue;
      const groupIds = groups.map(g => g.id).join(",");

      if (isPlanActive) {
        const resp = await vkApiV2("GET", "banners.json", {
          "_ad_group_id__in": groupIds,
          "_status": "active",
          limit: 200,
        }, null, config);
        banners = resp.items || [];
      } else {
        const resp = await vkApiV2("GET", "banners.json", {
          "_ad_group_id__in": groupIds,
          "_status": "blocked",
          limit: 200,
        }, null, config);
        banners = (resp.items || []).filter(
          b => byGuard.has(String(b.id)) && !byUser.has(String(b.id))
        );
      }
    } catch (e) {
      addLog("warn", `Кампания #${plan.id}: ошибка получения баннеров — ${e.message}`);
      continue;
    }

    if (!banners.length) continue;

    const bannerIds = banners.map(b => b.id);

    // 3. Базовая статистика v2
    let baseStatsMap = {};
    try {
      const statsV2 = await vkApiV2("GET", "statistics/banners/day.json", {
        date_from: today,
        date_to:   today,
        id_list:   bannerIds.join(","),
      }, null, config);

      for (const item of (statsV2.items || [])) {
        const row  = item.rows && item.rows[0];
        const base = row ? ((row.total && row.total.base) || row.base || {}) : {};
        baseStatsMap[String(item.id)] = {
          spent:       parseFloat(base.spent || base.sum || 0),
          impressions: parseInt(base.shows || base.impressions || 0, 10),
          clicks:      parseInt(base.clicks || 0, 10),
        };
      }
    } catch (e) {
      addLog("warn", `Кампания #${plan.id}: ошибка статистики — ${e.message}`);
      continue;
    }

    // 4. Аудио-статистика v3
    let audioMap = {};
    if (accountId && pixelId && rule.metric === "cpu") {
      try {
        const audioField = `base,custom_event.custom_event_${pixelId}_music_track_to_profile`;
        const statsV3 = await vkApiV3(
          "statistics/banners/day.json",
          { account: accountId, sudo },
          {
            date_from:   todayDMY,
            date_to:     todayDMY,
            attribution: "conversion",
            fields:      audioField,
            id:          bannerIds,
          },
          config
        );

        for (const item of (statsV3.items || [])) {
          const baseV3 = item.total?.base;
          if (baseV3 && parseFloat(baseV3.spent) > 0) {
            const sid = String(item.id);
            if (!baseStatsMap[sid]) baseStatsMap[sid] = { spent: 0, impressions: 0, clicks: 0 };
            baseStatsMap[sid].spent = parseFloat(baseV3.spent);
          }
          const ev = item.total?.custom_event?.[`custom_event_${pixelId}_music_track_to_profile`];
          if (ev) {
            audioMap[String(item.id)] = {
              count: parseInt(ev.count, 10) || 0,
              cpa:   parseFloat(ev.cpa)     || 0,
            };
          }
        }
      } catch (e) {
        addLog("warn", `Кампания #${plan.id}: ошибка аудио-статистики — ${e.message}`);
      }
    }

    // 5. Финальный statsMap
    const statsMap = {};
    for (const bid of bannerIds) {
      const sid   = String(bid);
      const base  = baseStatsMap[sid] || { spent: 0, impressions: 0, clicks: 0 };
      const audio = audioMap[sid]     || { count: 0, cpa: 0 };
      statsMap[sid] = {
        spent:       base.spent,
        impressions: base.impressions,
        clicks:      base.clicks,
        audio_adds:  audio.count,
        cpu_cpa:     audio.cpa,
      };
    }

    // 6. Остановка
    if (isPlanActive) {
      for (const banner of banners) {
        const sid = String(banner.id);
        if (byUser.has(sid)) continue;

        const s = statsMap[sid];
        if (!s) continue;
        const value = computeMetric(rule.metric, s);
        if (value === null) continue;

        if (value > rule.threshold) {
          try {
            await vkApiV2("POST", "banners/mass_action.json", {}, [
              { id: banner.id, status: "blocked" }
            ], config);
            byGuard.add(sid);

            const name   = banner.name || `#${banner.id}`;
            const reason = s.audio_adds === 0 && rule.metric === "cpu"
              ? `расход ${value.toFixed(2)} ₽ без добавлений (холодный старт)`
              : `${metricLabel(rule.metric)} = ${value.toFixed(2)} > ${rule.threshold}`;

            addLog("stopped", `⛔ Баннер «${name}» (#${banner.id}) кампании #${plan.id} остановлен. ${reason}`);
            totalStopped++;
          } catch (e) {
            addLog("warn", `Не удалось остановить баннер #${banner.id}: ${e.message}`);
          }
        }
      }
    }

    // 7. Возобновление
    if (isResumeMode && !isPlanActive) {
      for (const banner of banners) {
        const sid = String(banner.id);
        const s = statsMap[sid];
        if (!s) continue;

        if (shouldResume(rule.metric, s, rule.threshold)) {
          try {
            await vkApiV2("POST", "banners/mass_action.json", {}, [
              { id: banner.id, status: "active" }
            ], config);
            byGuard.delete(sid);

            const name       = banner.name || `#${banner.id}`;
            const cpaCurrent = s.audio_adds > 0 ? `${s.cpu_cpa.toFixed(2)} ₽` : "нет добавлений";
            addLog("ok", `✅ Баннер «${name}» (#${banner.id}) кампании #${plan.id} возобновлён. ${metricLabel(rule.metric)}: ${cpaCurrent} ≤ ${rule.threshold} ₽`);
            totalResumed++;
          } catch (e) {
            addLog("warn", `Не удалось возобновить баннер #${banner.id}: ${e.message}`);
          }
        }
      }
    }

    // 8. Синхронизируем byUser для активных планов
    if (isPlanActive) {
      try {
        const groupsResp = await vkApiV2("GET", "ad_groups.json", {
          "_ad_plan_id": plan.id, limit: 200,
        }, null, config);
        const groups = groupsResp.items || [];
        if (groups.length) {
          const groupIds = groups.map(g => g.id).join(",");

          const blockedResp = await vkApiV2("GET", "banners.json", {
            "_ad_group_id__in": groupIds, "_status": "blocked", limit: 200,
          }, null, config);
          for (const b of (blockedResp.items || [])) {
            const sid = String(b.id);
            if (!byGuard.has(sid)) byUser.add(sid);
          }

          const activeResp = await vkApiV2("GET", "banners.json", {
            "_ad_group_id__in": groupIds, "_status": "active", limit: 200,
          }, null, config);
          for (const b of (activeResp.items || [])) {
            byUser.delete(String(b.id));
          }
        }
      } catch (_) {}
    }
  }

  // Сохраняем обновлённые sets в config
  config.stoppedByGuard = [...byGuard];
  config.stoppedByUser  = [...byUser];

  if (totalStopped === 0 && totalResumed === 0)
    addLog("ok", `Нарушений нет. Кампаний с правилами: ${activeRuleIds.length}.`);

  return logs;
}

// ─── Entry point ──────────────────────────────────────────────────────────────

async function main() {
  if (!VK_CLIENT_ID || !VK_CLIENT_SECRET || !GIST_TOKEN || !GIST_ID) {
    console.error("❌ Не заданы переменные окружения: VK_CLIENT_ID, VK_CLIENT_SECRET, GIST_TOKEN, GIST_ID");
    process.exit(1);
  }

  console.log("🔍 VK Ads Guard v2.4 — старт проверки...");

  // Читаем конфиг из Gist
  const config = await readGist();

  // Если гвард отключён — выходим
  if (config.enabled === false) {
    console.log("⏸ Гвард отключён (enabled: false).");
    process.exit(0);
  }

  // Получаем токен
  await getAccessToken(config);

  // Запускаем проверку
  let newLogs = [];
  try {
    newLogs = await runCheck(config);
  } catch (e) {
    newLogs = [{ level: "error", message: `Ошибка: ${e.message}`, time: new Date().toISOString() }];
    console.error("❌", e.message);
  }

  // Обновляем лог в конфиге
  const existingLog = config.log || [];
  config.log = [...newLogs, ...existingLog].slice(0, LOG_MAX_ENTRIES);
  config.lastCheckAt = Date.now();

  // Пишем обратно в Gist
  await writeGist(config);

  // Выводим результат в консоль Actions
  for (const entry of newLogs) {
    console.log(`[${entry.level.toUpperCase()}] ${entry.message}`);
  }

  console.log("✅ Проверка завершена.");
}

main().catch(e => {
  console.error("Fatal:", e);
  process.exit(1);
});
