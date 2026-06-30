require("dotenv").config();
const admin = require("firebase-admin");
const { initializeApp: initClientApp } = require("firebase/app");
const {
  getFirestore: getClientFirestore,
  collection: fsCollection,
  getDocs,
  Timestamp: ClientTimestamp,
  GeoPoint: ClientGeoPoint,
} = require("firebase/firestore");
const cron = require("node-cron");
const http = require("http");

// ═════════════════════════════════════════════════════════════════════════════
// 1. FIREBASE INITIALIZATION
// ═════════════════════════════════════════════════════════════════════════════

// PRIMARY — client SDK (web config). Reads only, governed by your public
// Firestore security rules. Natural data-loss guard: if credentials are ever
// accidentally swapped, the backup DB's rules block reads before any wipe.
const primaryClientApp = initClientApp(
  {
    apiKey:            process.env.PRIMARY_API_KEY,
    authDomain:        process.env.PRIMARY_AUTH_DOMAIN,
    projectId:         process.env.PRIMARY_PROJECT_ID,
    storageBucket:     process.env.PRIMARY_STORAGE_BUCKET,
    messagingSenderId: process.env.PRIMARY_MESSAGING_SENDER_ID,
    appId:             process.env.PRIMARY_APP_ID,
  },
  "primary-client"
);
const primaryDB = getClientFirestore(primaryClientApp);

// BACKUP — Admin SDK (service account). Writes only. One key, one side.
function parsePrivateKey(raw) { return raw.replace(/\\n/g, "\n"); }

const backupApp = admin.initializeApp(
  {
    credential: admin.credential.cert({
      projectId:   process.env.BACKUP_PROJECT_ID,
      clientEmail: process.env.BACKUP_CLIENT_EMAIL,
      privateKey:  parsePrivateKey(process.env.BACKUP_PRIVATE_KEY),
    }),
  },
  "backup"
);
const backupDB = admin.firestore(backupApp);

// ═════════════════════════════════════════════════════════════════════════════
// 2. CONFIGURATION
// ═════════════════════════════════════════════════════════════════════════════

const COLLECTIONS = (process.env.COLLECTIONS || "customers,purchases,products")
  .split(",").map((c) => c.trim()).filter(Boolean);

// Metadata collection in the BACKUP Firestore database.
// Layout:
//   _backup_metadata/
//     {collectionName}          — per-collection checksum + sync stats
//     slot_{slotISO}            — per-slot retry state + full attempt log
const METADATA_COLLECTION = "_backup_metadata";
const PORT = process.env.PORT || 3000;

// Schedule: two slots per day in PHT.
const BACKUP_HOUR_1 = parseInt(process.env.BACKUP_HOUR_1 ?? "5",  10); //  5 AM PHT
const BACKUP_HOUR_2 = parseInt(process.env.BACKUP_HOUR_2 ?? "17", 10); // 5 PM PHT

// Retry policy:
//   • Within the first hour: up to MAX_CONSECUTIVE_FAILS fast attempts
//     (CONSECUTIVE_RETRY_DELAY_MS apart). Fast retries handle transient errors.
//   • After the first hour is exhausted: retry once per hour (anchored to the
//     first attempt time, e.g. 5:12 → 6:12 → 7:12 …).
//   • Stop when the slot age exceeds GRACE_HOURS (5 h). For a 5 PM slot that
//     means the last allowed hourly window starts before 10 PM.
const MAX_CONSECUTIVE_FAILS    = parseInt(process.env.MAX_CONSECUTIVE_FAILS    ?? "5",      10);
const CONSECUTIVE_RETRY_DELAY  = parseInt(process.env.CONSECUTIVE_RETRY_DELAY  ?? "30000",  10); // ms between fast retries
const GRACE_HOURS              = parseInt(process.env.GRACE_HOURS              ?? "5",      10); // max hours after slot

// ═════════════════════════════════════════════════════════════════════════════
// 3. LOGGER
// ═════════════════════════════════════════════════════════════════════════════

function phNow() {
  return new Date().toLocaleString("en-PH", { timeZone: "Asia/Manila" });
}

function log(level, message, data = null) {
  const prefix = `[${phNow()}] [${level.toUpperCase()}]`;
  if (data) console.log(`${prefix} ${message}`, JSON.stringify(data, null, 2));
  else      console.log(`${prefix} ${message}`);
}

// ═════════════════════════════════════════════════════════════════════════════
// 4. IN-MEMORY STATE
// ═════════════════════════════════════════════════════════════════════════════

const backupState = {
  isRunning:   false,
  lastRun:     null,
  lastResults: null,
};

// ═════════════════════════════════════════════════════════════════════════════
// 5. TIMEZONE HELPERS
// ═════════════════════════════════════════════════════════════════════════════

const PH_OFFSET_MINUTES = 8 * 60;

function getPhWallClockParts(date) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Manila",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  }).formatToParts(date);
  const m = {};
  for (const p of parts) m[p.type] = p.value;
  return {
    year:   parseInt(m.year,   10),
    month:  parseInt(m.month,  10),
    day:    parseInt(m.day,    10),
    hour:   parseInt(m.hour,   10) % 24,
    minute: parseInt(m.minute, 10),
  };
}

function phWallClockToUTC(year, month, day, hour, minute) {
  return new Date(
    Date.UTC(year, month - 1, day, hour, minute, 0) - PH_OFFSET_MINUTES * 60000
  );
}

// Returns the most recent scheduled slot (as a true UTC Date) that is <= now.
function getMostRecentSlot() {
  const now = new Date();
  const { year, month, day } = getPhWallClockParts(now);
  const candidates = [];
  for (const dayOffset of [0, -1]) {
    const base = new Date(Date.UTC(year, month - 1, day + dayOffset));
    for (const h of [BACKUP_HOUR_1, BACKUP_HOUR_2]) {
      candidates.push(phWallClockToUTC(
        base.getUTCFullYear(), base.getUTCMonth() + 1, base.getUTCDate(), h, 0
      ));
    }
  }
  const past = candidates.filter((d) => d <= now);
  past.sort((a, b) => b - a);
  return past[0];
}

// Slot deadline = slot time + GRACE_HOURS. No retries after this.
function getSlotDeadline(slotDate) {
  return new Date(slotDate.getTime() + GRACE_HOURS * 60 * 60 * 1000);
}

function getNextScheduledTime() {
  const now  = new Date();
  const phNowDate = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Manila" }));
  const h    = phNowDate.getHours();
  const m    = phNowDate.getMinutes();
  let nextH;
  if      (h < BACKUP_HOUR_1 || (h === BACKUP_HOUR_1 && m === 0)) nextH = BACKUP_HOUR_1;
  else if (h < BACKUP_HOUR_2 || (h === BACKUP_HOUR_2 && m === 0)) nextH = BACKUP_HOUR_2;
  else nextH = BACKUP_HOUR_1 + 24;
  const next = new Date(phNowDate);
  next.setHours(nextH, 0, 0, 0);
  return next.toLocaleString("en-PH", { timeZone: "Asia/Manila" });
}

// Stable doc ID for the slot's metadata (safe for Firestore doc IDs).
function slotDocId(slotDate) {
  return "slot_" + slotDate.toISOString().replace(/[:.]/g, "-");
}

// ═════════════════════════════════════════════════════════════════════════════
// 6. SLOT METADATA (persisted in backup Firestore)
// ═════════════════════════════════════════════════════════════════════════════
//
// Document: _backup_metadata / slot_{slotISO}
// {
//   slotTime:              ISO string  — the scheduled slot this belongs to
//   slotLabel:             "5:00 AM PHT 2026-06-30"  — human-readable
//   status:                "pending" | "in_progress" | "success" | "abandoned"
//   firstAttemptAt:        ISO string  — anchors the hourly retry schedule
//   nextRetryAt:           ISO string  — when next hourly window opens (null if none)
//   hourlyWindowIndex:     number      — 0 = first hour, 1 = second hour, …
//   consecutiveFailsInWindow: number   — resets on each hourly window
//   totalAttempts:         number
//   lastAttemptAt:         ISO string
//   lastAttemptStatus:     "success" | "all_collections_failed" | "partial"
//   deadline:              ISO string  — slot + GRACE_HOURS, no retries after
//   attempts: [            — append-only log of every attempt
//     {
//       attemptNumber:     number
//       hourlyWindow:      number      — which hourly window (0-based)
//       windowAttempt:     number      — attempt within that window (1-based)
//       startedAt:         ISO string
//       finishedAt:        ISO string
//       elapsedSeconds:    string
//       triggeredBy:       string
//       status:            "success" | "all_failed" | "partial"
//       totalReads:        number
//       totalWrites:       number
//       totalDeletes:      number
//       collections: {
//         [name]: { status, reads, writes, deletes, error? }
//       }
//     }
//   ]
// }

async function readSlotMeta(slotDate) {
  try {
    const doc = await backupDB
      .collection(METADATA_COLLECTION)
      .doc(slotDocId(slotDate))
      .get();
    if (!doc.exists) return null;
    return doc.data();
  } catch (err) {
    log("warn", `Could not read slot metadata: ${err.message}`);
    return null;
  }
}

async function writeSlotMeta(slotDate, data) {
  try {
    await backupDB
      .collection(METADATA_COLLECTION)
      .doc(slotDocId(slotDate))
      .set(data, { merge: true });
  } catch (err) {
    log("warn", `Could not write slot metadata: ${err.message}`);
  }
}

async function appendAttemptLog(slotDate, slotMeta, attemptRecord) {
  // Firestore arrays can't be appended atomically with set+merge when using
  // raw arrays, so we read the existing attempts array, push, then write back.
  const existing = slotMeta?.attempts || [];
  existing.push(attemptRecord);
  await writeSlotMeta(slotDate, { attempts: existing });
}

// ═════════════════════════════════════════════════════════════════════════════
// 7. PER-COLLECTION METADATA (checksum diffing)
// ═════════════════════════════════════════════════════════════════════════════

async function readCollectionMeta(collectionName) {
  try {
    const doc = await backupDB
      .collection(METADATA_COLLECTION)
      .doc(collectionName)
      .get();
    if (!doc.exists) return null;
    return doc.data();
  } catch (err) {
    log("warn", `Could not read collection metadata [${collectionName}]: ${err.message}`);
    return null;
  }
}

async function writeCollectionMeta(collectionName, metadata) {
  try {
    await backupDB
      .collection(METADATA_COLLECTION)
      .doc(collectionName)
      .set(metadata, { merge: true });
  } catch (err) {
    log("warn", `Could not write collection metadata [${collectionName}]: ${err.message}`);
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// 8. CORE SYNC LOGIC
// ═════════════════════════════════════════════════════════════════════════════

// ─────────────────────────────────────────────────────────────────────────
// Cross-SDK data sanitizer
// ─────────────────────────────────────────────────────────────────────────
// The primary side reads documents using the CLIENT SDK (firebase/firestore),
// while the backup side writes using the ADMIN SDK (firebase-admin). Both
// SDKs have their own separate `Timestamp` and `GeoPoint` classes — even
// though they represent the same data, they are NOT the same class, so the
// Admin SDK's `.set()` rejects them with:
//   "Detected an object of type 'Timestamp' that doesn't match the
//    expected instance ... Firestore types ... from the same NPM package."
//
// Fix: recursively walk the document and convert any client-SDK Timestamp
// into a plain JS Date (Admin SDK auto-converts Date -> its own Timestamp
// on write — this is the one type both SDKs agree on), and any client-SDK
// GeoPoint into a plain {latitude, longitude} object reconstructed with the
// Admin SDK's own GeoPoint class.
function sanitizeForAdminSDK(value) {
  if (value === null || value === undefined) return value;

  // Client SDK Timestamp -> plain JS Date
  if (value instanceof ClientTimestamp) {
    return value.toDate();
  }

  // Client SDK GeoPoint -> Admin SDK GeoPoint
  if (value instanceof ClientGeoPoint) {
    return new admin.firestore.GeoPoint(value.latitude, value.longitude);
  }

  // Already a plain Date (shouldn't normally appear from client SDK reads,
  // but harmless to pass through untouched).
  if (value instanceof Date) return value;

  // Arrays — sanitize each element.
  if (Array.isArray(value)) {
    return value.map(sanitizeForAdminSDK);
  }

  // Plain objects — sanitize each field recursively.
  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = sanitizeForAdminSDK(v);
    }
    return out;
  }

  // Primitives (string, number, boolean) pass through unchanged.
  return value;
}

function buildChecksumMap(docs) {
  const map = {};
  for (const doc of docs) {
    const data = doc.data();
    const ts =
      data?.updatedAt?.toDate?.()?.toISOString() ||
      data?.updated_at?.toDate?.()?.toISOString() ||
      data?.createdAt?.toDate?.()?.toISOString() ||
      data?.created_at?.toDate?.()?.toISOString() ||
      String(JSON.stringify(data).length);
    map[doc.id] = ts;
  }
  return map;
}

async function syncCollection(collectionName) {
  const stats = { reads: 0, writes: 0, deletes: 0, skipped: 0, status: "ok" };

  log("info", `━━━ Syncing collection: [${collectionName}] ━━━`);

  const meta = await readCollectionMeta(collectionName);
  const previousChecksums = meta?.docChecksums || {};
  log("info", `  Metadata loaded. Previously tracked ${Object.keys(previousChecksums).length} docs.`);

  let primarySnapshot;
  try {
    primarySnapshot = await getDocs(fsCollection(primaryDB, collectionName));
    stats.reads += primarySnapshot.size;
  } catch (err) {
    log("error", `  Failed to read primary [${collectionName}]: ${err.message}`);
    stats.status = "error";
    stats.error = err.message;
    return stats;
  }

  if (primarySnapshot.empty) {
    log("warn", `  [${collectionName}] is EMPTY in primary — skipping to avoid data loss.`);
    stats.status = "skipped_empty";
    return stats;
  }

  const primaryDocs    = primarySnapshot.docs;
  const currentChecksums = buildChecksumMap(primaryDocs);
  const toUpsert = [];
  const toDelete = [];

  for (const doc of primaryDocs) {
    if (previousChecksums[doc.id] !== currentChecksums[doc.id]) toUpsert.push(doc);
    else stats.skipped++;
  }
  const currentIds = new Set(primaryDocs.map((d) => d.id));
  for (const oldId of Object.keys(previousChecksums)) {
    if (!currentIds.has(oldId)) toDelete.push(oldId);
  }

  log("info", `  Diff → upsert: ${toUpsert.length}, delete: ${toDelete.length}, unchanged: ${stats.skipped}`);

  if (toUpsert.length === 0 && toDelete.length === 0) {
    log("info", `  No changes detected for [${collectionName}].`);
    stats.status = "no_changes";
    return stats;
  }

  const BATCH_LIMIT = 400;

  for (let i = 0; i < toUpsert.length; i += BATCH_LIMIT) {
    const chunk = toUpsert.slice(i, i + BATCH_LIMIT);
    const batch = backupDB.batch();
    for (const doc of chunk) {
      const sanitizedData = sanitizeForAdminSDK(doc.data());
      batch.set(backupDB.collection(collectionName).doc(doc.id), sanitizedData, { merge: true });
    }
    await batch.commit();
    stats.writes += chunk.length;
    log("info", `  ✓ Upserted batch of ${chunk.length} docs.`);
  }

  for (let i = 0; i < toDelete.length; i += BATCH_LIMIT) {
    const chunk = toDelete.slice(i, i + BATCH_LIMIT);
    const batch = backupDB.batch();
    for (const docId of chunk) {
      batch.delete(backupDB.collection(collectionName).doc(docId));
    }
    await batch.commit();
    stats.deletes += chunk.length;
    log("info", `  ✓ Deleted batch of ${chunk.length} stale docs.`);
  }

  await writeCollectionMeta(collectionName, {
    docChecksums:  currentChecksums,
    lastDocCount:  primaryDocs.length,
    lastSyncedAt:  new Date().toISOString(),
    lastSyncStats: stats,
  });

  log("info", `  ✓ Collection metadata updated for [${collectionName}].`);
  return stats;
}

// ═════════════════════════════════════════════════════════════════════════════
// 9. MASTER BACKUP RUNNER
// ═════════════════════════════════════════════════════════════════════════════

async function runBackup(triggeredBy, slotDate, slotMeta, attemptNumber, hourlyWindow, windowAttempt) {
  if (backupState.isRunning) {
    log("warn", "Backup already in progress — skipping.");
    return null;
  }

  backupState.isRunning = true;
  const startedAt  = new Date();
  const startMs    = startedAt.getTime();
  const startLabel = startedAt.toLocaleString("en-PH", { timeZone: "Asia/Manila" });

  log("info", "╔══════════════════════════════════════════════╗");
  log("info", "║         FIREBASE POS BACKUP STARTED          ║");
  log("info", `║  ${startLabel.padEnd(44)}║`);
  log("info", `║  Triggered by : ${triggeredBy.padEnd(29)}║`);
  log("info", `║  Attempt      : #${String(attemptNumber).padEnd(28)}║`);
  log("info", `║  Hourly window: ${String(hourlyWindow).padEnd(29)}║`);
  log("info", `║  Window try   : ${String(windowAttempt).padEnd(29)}║`);
  log("info", "╚══════════════════════════════════════════════╝");

  // Write "in_progress" to Firestore immediately so we have a record even
  // if the process crashes mid-backup.
  await writeSlotMeta(slotDate, {
    status:          "in_progress",
    lastAttemptAt:   startedAt.toISOString(),
    totalAttempts:   attemptNumber,
  });

  const results = {};
  for (const col of COLLECTIONS) {
    try {
      results[col] = await syncCollection(col);
    } catch (err) {
      log("error", `Unexpected error syncing [${col}]: ${err.message}`);
      results[col] = { status: "fatal_error", error: err.message };
    }
  }

  const finishedAt     = new Date();
  const elapsedSeconds = ((finishedAt - startMs) / 1000).toFixed(2);

  let totalReads = 0, totalWrites = 0, totalDeletes = 0;
  for (const s of Object.values(results)) {
    totalReads   += s.reads   || 0;
    totalWrites  += s.writes  || 0;
    totalDeletes += s.deletes || 0;
  }

  const allFailed  = Object.values(results).every((r) => r.status === "error" || r.status === "fatal_error");
  const anyFailed  = Object.values(results).some((r)  => r.status === "error" || r.status === "fatal_error");
  const runStatus  = allFailed ? "all_failed" : anyFailed ? "partial" : "success";

  log("info", `Backup ${runStatus} — reads: ${totalReads}, writes: ${totalWrites}, deletes: ${totalDeletes}, elapsed: ${elapsedSeconds}s`);

  // Build the attempt log entry.
  const attemptRecord = {
    attemptNumber,
    hourlyWindow,
    windowAttempt,
    triggeredBy,
    startedAt:      startedAt.toISOString(),
    startedAtPHT:   startLabel,
    finishedAt:     finishedAt.toISOString(),
    finishedAtPHT:  finishedAt.toLocaleString("en-PH", { timeZone: "Asia/Manila" }),
    elapsedSeconds,
    status:         runStatus,
    totalReads,
    totalWrites,
    totalDeletes,
    collections:    Object.fromEntries(
      Object.entries(results).map(([k, v]) => [k, {
        status:  v.status,
        reads:   v.reads   || 0,
        writes:  v.writes  || 0,
        deletes: v.deletes || 0,
        ...(v.error ? { error: v.error } : {}),
      }])
    ),
  };

  // Persist the attempt log + updated slot state.
  await appendAttemptLog(slotDate, slotMeta, attemptRecord);

  backupState.isRunning  = false;
  backupState.lastRun    = finishedAt.toISOString();
  backupState.lastResults = { attemptRecord, triggeredBy };

  return attemptRecord;
}

// ═════════════════════════════════════════════════════════════════════════════
// 10. RETRY ORCHESTRATOR
// ═════════════════════════════════════════════════════════════════════════════
//
// State machine persisted in Firestore (_backup_metadata/slot_{id}):
//
//   [slot fires or server wakes within grace window]
//        │
//        ▼
//   Read slot metadata from Firestore
//        │
//        ├── status = "success"  ──────────────────────────► Done for this slot
//        │
//        ├── status = "abandoned" ─────────────────────────► Done for this slot
//        │
//        └── status = "pending" | "in_progress" | null
//                 │
//                 ▼
//        Are we past the slot deadline (slot + 5 h)?
//                 │
//                 ├── Yes ──► Mark abandoned, stop
//                 │
//                 └── No
//                          │
//                          ▼
//                 Do we have a firstAttemptAt?
//                          │
//                          ├── No  ──► Run now (window 0, attempt 1)
//                          │
//                          └── Yes
//                                   │
//                                   ▼
//                          Are we in a new hourly window?
//                          (now >= firstAttemptAt + windowIndex*1h)
//                                   │
//                                   ├── No  ──► Have we hit MAX_CONSECUTIVE_FAILS?
//                                   │           ├── No  ──► Run now (same window)
//                                   │           └── Yes ──► Wait for next window
//                                   │
//                                   └── Yes ──► Open new window, run now

let orchestratorInFlight = false;

async function runSlotOrchestrator(triggeredBy) {
  if (backupState.isRunning || orchestratorInFlight) return;
  orchestratorInFlight = true;

  try {
    const now  = new Date();
    const slot = getMostRecentSlot();
    if (!slot) return;

    const deadline         = getSlotDeadline(slot);
    const slotLabelPHT     = slot.toLocaleString("en-PH", { timeZone: "Asia/Manila" });
    const deadlineLabelPHT = deadline.toLocaleString("en-PH", { timeZone: "Asia/Manila" });

    // ── Past the grace window entirely? ────────────────────────────────────
    if (now >= deadline) return; // Silent — we just wait for the next slot.

    // ── Read current slot state from Firestore ──────────────────────────────
    let meta = await readSlotMeta(slot);

    // ── Already succeeded or abandoned? ────────────────────────────────────
    if (meta?.status === "success" || meta?.status === "abandoned") return;

    // ── First ever attempt for this slot? ──────────────────────────────────
    if (!meta || !meta.firstAttemptAt) {
      log("info", `Slot ${slotLabelPHT}: first attempt. Deadline: ${deadlineLabelPHT}.`);

      const initMeta = {
        slotTime:                 slot.toISOString(),
        slotTimePHT:              slotLabelPHT,
        deadline:                 deadline.toISOString(),
        deadlinePHT:              deadlineLabelPHT,
        status:                   "pending",
        firstAttemptAt:           now.toISOString(),
        nextRetryAt:              null,
        hourlyWindowIndex:        0,
        consecutiveFailsInWindow: 0,
        totalAttempts:            0,
        lastAttemptAt:            null,
        lastAttemptStatus:        null,
        attempts:                 [],
      };
      await writeSlotMeta(slot, initMeta);
      meta = initMeta;
    }

    // ── Determine current hourly window ────────────────────────────────────
    const firstAttemptAt  = new Date(meta.firstAttemptAt);
    const msSinceFirst    = now - firstAttemptAt;
    const currentWindow   = Math.floor(msSinceFirst / (60 * 60 * 1000)); // 0, 1, 2 …
    const lastWindow      = meta.hourlyWindowIndex ?? 0;
    const failsInWindow   = meta.consecutiveFailsInWindow ?? 0;
    const totalAttempts   = meta.totalAttempts ?? 0;

    // Opening a new hourly window?
    const newWindowOpen = currentWindow > lastWindow;

    if (!newWindowOpen && failsInWindow >= MAX_CONSECUTIVE_FAILS) {
      // Still in the same window and already hit the consecutive fail limit.
      // nextRetryAt tells us when the next window opens.
      const nextRetryAt = meta.nextRetryAt ? new Date(meta.nextRetryAt) : null;
      const waitMin = nextRetryAt ? Math.ceil((nextRetryAt - now) / 60000) : "?";
      log("info", `Slot ${slotLabelPHT}: window ${lastWindow} exhausted (${failsInWindow}/${MAX_CONSECUTIVE_FAILS} fails). Next retry in ~${waitMin} min.`);
      return;
    }

    // ── Decide window index and window-attempt counter ──────────────────────
    let windowIndex, windowAttempt;
    if (newWindowOpen) {
      windowIndex   = currentWindow;
      windowAttempt = 1;
      log("info", `Slot ${slotLabelPHT}: opening hourly window ${windowIndex} (retry hour ${windowIndex}).`);
      // Reset consecutive fail counter for the new window in Firestore.
      await writeSlotMeta(slot, {
        hourlyWindowIndex:        windowIndex,
        consecutiveFailsInWindow: 0,
      });
      // Re-read so the attempt log append later has fresh data.
      meta = await readSlotMeta(slot) || meta;
      meta.consecutiveFailsInWindow = 0;
      meta.hourlyWindowIndex        = windowIndex;
    } else {
      windowIndex   = lastWindow;
      windowAttempt = failsInWindow + 1;
    }

    const attemptNumber = totalAttempts + 1;

    // ── Run the actual backup ───────────────────────────────────────────────
    const record = await runBackup(
      triggeredBy, slot, meta,
      attemptNumber, windowIndex, windowAttempt
    );
    if (!record) return; // was already running

    const succeeded = record.status === "success";
    const newFails  = succeeded ? 0 : (meta.consecutiveFailsInWindow ?? 0) + 1;

    // ── Compute next retry window time ─────────────────────────────────────
    // Anchored to firstAttemptAt so retries are exactly 1 h apart regardless
    // of how long the backup itself takes.
    const nextWindowStart = new Date(firstAttemptAt.getTime() + (windowIndex + 1) * 60 * 60 * 1000);
    const nextRetryAt     = nextWindowStart < deadline ? nextWindowStart.toISOString() : null;

    // ── Persist updated slot state ──────────────────────────────────────────
    const updatedSlotMeta = {
      status:                   succeeded ? "success" : (nextRetryAt ? "pending" : "abandoned"),
      lastAttemptAt:            record.finishedAt,
      lastAttemptAtPHT:         record.finishedAtPHT,
      lastAttemptStatus:        record.status,
      totalAttempts:            attemptNumber,
      consecutiveFailsInWindow: newFails,
      nextRetryAt:              succeeded ? null : nextRetryAt,
      nextRetryAtPHT:           (succeeded || !nextRetryAt) ? null
        : new Date(nextRetryAt).toLocaleString("en-PH", { timeZone: "Asia/Manila" }),
    };
    await writeSlotMeta(slot, updatedSlotMeta);

    if (succeeded) {
      log("info", `✅ Slot ${slotLabelPHT}: backup succeeded on attempt #${attemptNumber}.`);
    } else if (!nextRetryAt) {
      log("warn", `⛔ Slot ${slotLabelPHT}: all retry windows exhausted (deadline ${deadlineLabelPHT}). Marking abandoned.`);
    } else if (newFails >= MAX_CONSECUTIVE_FAILS) {
      log("warn", `⚠️  Slot ${slotLabelPHT}: window ${windowIndex} exhausted (${newFails}/${MAX_CONSECUTIVE_FAILS} fails). Next hourly retry at ${new Date(nextRetryAt).toLocaleString("en-PH", { timeZone: "Asia/Manila" })}.`);
    } else {
      log("warn", `↩️  Slot ${slotLabelPHT}: attempt #${attemptNumber} failed (${newFails}/${MAX_CONSECUTIVE_FAILS}). Will retry in window ${windowIndex} again.`);
    }

  } catch (err) {
    log("error", `Orchestrator error: ${err.message}`);
  } finally {
    orchestratorInFlight = false;
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// 11. HTTP SERVER
// ═════════════════════════════════════════════════════════════════════════════

const server = http.createServer((req, res) => {
  const url = req.url;

  if (url === "/" || url === "/health") {
    runSlotOrchestrator("http_health_check").catch((err) =>
      log("error", `Orchestrator trigger error: ${err.message}`)
    );
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      status:         "ok",
      service:        "Firebase POS Backup",
      currentTimePHT: phNow(),
      nextBackup:     getNextScheduledTime(),
      isRunning:      backupState.isRunning,
      lastRun:        backupState.lastRun,
    }));
    return;
  }

  if (url === "/status") {
    runSlotOrchestrator("http_status_check").catch((err) =>
      log("error", `Orchestrator trigger error: ${err.message}`)
    );
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      service:        "Firebase POS Backup",
      collections:    COLLECTIONS,
      currentTimePHT: phNow(),
      nextBackup:     getNextScheduledTime(),
      isRunning:      backupState.isRunning,
      lastRun:        backupState.lastRun,
      lastResults:    backupState.lastResults,
    }, null, 2));
    return;
  }

  if (url === "/trigger" && req.method === "POST") {
    if (backupState.isRunning) {
      res.writeHead(409, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Backup already in progress" }));
      return;
    }
    res.writeHead(202, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ message: "Backup triggered manually" }));
    runSlotOrchestrator("manual_trigger").catch((err) =>
      log("error", `Manual trigger error: ${err.message}`)
    );
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

server.listen(PORT, () => {
  log("info", `HTTP server listening on port ${PORT}`);
  log("info", `  GET  /health  — health check + catch-up trigger`);
  log("info", `  GET  /status  — last backup results`);
  log("info", `  POST /trigger — run backup now (for testing)`);
});

// ═════════════════════════════════════════════════════════════════════════════
// 12. CRON — fires every minute, orchestrator decides what to do
// ═════════════════════════════════════════════════════════════════════════════

log("info", "Firebase POS Backup Service starting...");
log("info", `Scheduled slots : ${BACKUP_HOUR_1}:00 PHT and ${BACKUP_HOUR_2}:00 PHT daily`);
log("info", `Grace window    : ${GRACE_HOURS} hours per slot`);
log("info", `Fast retry      : up to ${MAX_CONSECUTIVE_FAILS} consecutive attempts per hourly window`);
log("info", `Collections     : ${COLLECTIONS.join(", ")}`);

cron.schedule(
  "* * * * *",
  () => {
    runSlotOrchestrator("cron_tick").catch((err) =>
      log("error", `Cron orchestrator error: ${err.message}`)
    );
  },
  { timezone: "Asia/Manila" }
);
