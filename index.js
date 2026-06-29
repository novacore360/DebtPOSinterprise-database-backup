require("dotenv").config();
const admin = require("firebase-admin");
const cron = require("node-cron");

// ─────────────────────────────────────────────
// 1. Firebase App Initialization
// ─────────────────────────────────────────────

/**
 * Parse private key from env (handles escaped newlines from Render env vars)
 */
function parsePrivateKey(raw) {
  return raw.replace(/\\n/g, "\n");
}

// Primary Firebase App
const primaryApp = admin.initializeApp(
  {
    credential: admin.credential.cert({
      projectId: process.env.PRIMARY_PROJECT_ID,
      clientEmail: process.env.PRIMARY_CLIENT_EMAIL,
      privateKey: parsePrivateKey(process.env.PRIMARY_PRIVATE_KEY),
    }),
  },
  "primary"
);

// Backup Firebase App
const backupApp = admin.initializeApp(
  {
    credential: admin.credential.cert({
      projectId: process.env.BACKUP_PROJECT_ID,
      clientEmail: process.env.BACKUP_CLIENT_EMAIL,
      privateKey: parsePrivateKey(process.env.BACKUP_PRIVATE_KEY),
    }),
  },
  "backup"
);

const primaryDB = admin.firestore(primaryApp);
const backupDB = admin.firestore(backupApp);

// ─────────────────────────────────────────────
// 2. Configuration
// ─────────────────────────────────────────────

const COLLECTIONS = (process.env.COLLECTIONS || "customers,purchases,products")
  .split(",")
  .map((c) => c.trim())
  .filter(Boolean);

const METADATA_COLLECTION = "__backup_metadata__";

// ─────────────────────────────────────────────
// 3. Logger
// ─────────────────────────────────────────────

function log(level, message, data = null) {
  const ts = new Date().toLocaleString("en-PH", { timeZone: "Asia/Manila" });
  const prefix = `[${ts}] [${level.toUpperCase()}]`;
  if (data) {
    console.log(`${prefix} ${message}`, JSON.stringify(data, null, 2));
  } else {
    console.log(`${prefix} ${message}`);
  }
}

// ─────────────────────────────────────────────
// 4. Metadata Helpers
// ─────────────────────────────────────────────

/**
 * Read the last-synced metadata from the BACKUP database.
 * Metadata stores: { [collection]: { lastDocCount, lastSyncedAt, docChecksums: { [docId]: updatedAt } } }
 */
async function readBackupMetadata(collectionName) {
  try {
    const doc = await backupDB
      .collection(METADATA_COLLECTION)
      .doc(collectionName)
      .get();
    if (!doc.exists) return null;
    return doc.data();
  } catch (err) {
    log("warn", `Could not read metadata for [${collectionName}]: ${err.message}`);
    return null;
  }
}

/**
 * Write updated metadata to backup DB after a successful sync.
 */
async function writeBackupMetadata(collectionName, metadata) {
  await backupDB
    .collection(METADATA_COLLECTION)
    .doc(collectionName)
    .set(metadata, { merge: true });
}

// ─────────────────────────────────────────────
// 5. Core Sync Logic (per collection)
// ─────────────────────────────────────────────

/**
 * Build a simple checksum map: { docId -> updatedAt ISO string (or createdAt fallback) }
 * Used to detect which documents actually changed since last backup.
 */
function buildChecksumMap(docs) {
  const map = {};
  for (const doc of docs) {
    const data = doc.data();
    // Use Firestore server timestamps if available; fallback to a hash of stringified data
    const ts =
      data?.updatedAt?.toDate?.()?.toISOString() ||
      data?.updated_at?.toDate?.()?.toISOString() ||
      data?.createdAt?.toDate?.()?.toISOString() ||
      data?.created_at?.toDate?.()?.toISOString() ||
      JSON.stringify(data).length.toString(); // last-resort: data size as dirty flag
    map[doc.id] = ts;
  }
  return map;
}

/**
 * Sync a single collection from primary → backup.
 * Returns stats object: { reads, writes, deletes, skipped, status }
 */
async function syncCollection(collectionName) {
  const stats = { reads: 0, writes: 0, deletes: 0, skipped: 0, status: "ok" };

  log("info", `━━━ Syncing collection: [${collectionName}] ━━━`);

  // ── Step 1: Read metadata from BACKUP (1 read, cheap) ──
  const meta = await readBackupMetadata(collectionName);
  const previousChecksums = meta?.docChecksums || {};
  log("info", `  Metadata loaded. Previously tracked ${Object.keys(previousChecksums).length} docs.`);

  // ── Step 2: Read ALL docs from PRIMARY ──
  let primarySnapshot;
  try {
    primarySnapshot = await primaryDB.collection(collectionName).get();
    stats.reads += primarySnapshot.size;
  } catch (err) {
    log("error", `  Failed to read primary [${collectionName}]: ${err.message}`);
    stats.status = "error";
    return stats;
  }

  // ── Step 3: Guard – skip if primary collection is empty ──
  if (primarySnapshot.empty) {
    log("warn", `  [${collectionName}] is EMPTY in primary. Skipping to avoid data loss.`);
    stats.status = "skipped_empty";
    return stats;
  }

  const primaryDocs = primarySnapshot.docs;
  const currentChecksums = buildChecksumMap(primaryDocs);

  // ── Step 4: Diff – find what changed ──
  const toUpsert = [];
  const toDelete = [];

  // Docs to add or update
  for (const doc of primaryDocs) {
    const prev = previousChecksums[doc.id];
    const curr = currentChecksums[doc.id];
    if (prev !== curr) {
      toUpsert.push(doc);
    } else {
      stats.skipped++;
    }
  }

  // Docs deleted from primary (exist in old meta but not in current primary)
  const currentIds = new Set(primaryDocs.map((d) => d.id));
  for (const oldId of Object.keys(previousChecksums)) {
    if (!currentIds.has(oldId)) {
      toDelete.push(oldId);
    }
  }

  log("info", `  Diff result → upsert: ${toUpsert.length}, delete: ${toDelete.length}, unchanged: ${stats.skipped}`);

  if (toUpsert.length === 0 && toDelete.length === 0) {
    log("info", `  No changes detected for [${collectionName}]. Nothing to write.`);
    stats.status = "no_changes";
    return stats;
  }

  // ── Step 5: Write changes to BACKUP in batches (max 500 ops/batch) ──
  const BATCH_LIMIT = 400; // safe margin under Firestore's 500 limit

  // Upserts
  for (let i = 0; i < toUpsert.length; i += BATCH_LIMIT) {
    const chunk = toUpsert.slice(i, i + BATCH_LIMIT);
    const batch = backupDB.batch();
    for (const doc of chunk) {
      const ref = backupDB.collection(collectionName).doc(doc.id);
      batch.set(ref, doc.data(), { merge: true });
    }
    await batch.commit();
    stats.writes += chunk.length;
    log("info", `  ✓ Upserted batch of ${chunk.length} docs.`);
  }

  // Deletes (only when primary is NOT empty — guarded above)
  for (let i = 0; i < toDelete.length; i += BATCH_LIMIT) {
    const chunk = toDelete.slice(i, i + BATCH_LIMIT);
    const batch = backupDB.batch();
    for (const docId of chunk) {
      const ref = backupDB.collection(collectionName).doc(docId);
      batch.delete(ref);
    }
    await batch.commit();
    stats.deletes += chunk.length;
    log("info", `  ✓ Deleted batch of ${chunk.length} stale docs.`);
  }

  // ── Step 6: Update metadata in backup ──
  await writeBackupMetadata(collectionName, {
    docChecksums: currentChecksums,
    lastDocCount: primaryDocs.length,
    lastSyncedAt: new Date().toISOString(),
    lastSyncStats: stats,
  });

  log("info", `  ✓ Metadata updated for [${collectionName}].`);
  return stats;
}

// ─────────────────────────────────────────────
// 6. Master Backup Runner
// ─────────────────────────────────────────────

async function runBackup() {
  const startTime = Date.now();
  const phTime = new Date().toLocaleString("en-PH", { timeZone: "Asia/Manila" });

  log("info", "╔══════════════════════════════════════════╗");
  log("info", "║       FIREBASE POS BACKUP STARTED        ║");
  log("info", `║  ${phTime.padEnd(40)}║`);
  log("info", "╚══════════════════════════════════════════╝");
  log("info", `Collections to sync: ${COLLECTIONS.join(", ")}`);

  const results = {};

  for (const col of COLLECTIONS) {
    try {
      results[col] = await syncCollection(col);
    } catch (err) {
      log("error", `Unexpected error syncing [${col}]: ${err.message}`);
      results[col] = { status: "fatal_error", error: err.message };
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);

  log("info", "╔══════════════════════════════════════════╗");
  log("info", "║          BACKUP SUMMARY                  ║");
  log("info", "╚══════════════════════════════════════════╝");

  let totalReads = 0, totalWrites = 0, totalDeletes = 0;
  for (const [col, stat] of Object.entries(results)) {
    log("info", `  [${col}]`, stat);
    totalReads += stat.reads || 0;
    totalWrites += stat.writes || 0;
    totalDeletes += stat.deletes || 0;
  }

  log("info", `  ─── Totals ───`);
  log("info", `  Reads:   ${totalReads}`);
  log("info", `  Writes:  ${totalWrites}`);
  log("info", `  Deletes: ${totalDeletes}`);
  log("info", `  Elapsed: ${elapsed}s`);
  log("info", "══════════════════════════════════════════");
}

// ─────────────────────────────────────────────
// 7. Time-gate: Only run during backup window
// ─────────────────────────────────────────────

/**
 * Cron fires every minute, but we only proceed if we're within
 * the first minute of the scheduled hour (PHT).
 */
function isBackupTime() {
  const now = new Date();
  const phHour = parseInt(
    now.toLocaleString("en-PH", { timeZone: "Asia/Manila", hour: "numeric", hour12: false }),
    10
  );
  const phMinute = parseInt(
    now.toLocaleString("en-PH", { timeZone: "Asia/Manila", minute: "numeric" }),
    10
  );

  const hour1 = parseInt(process.env.BACKUP_HOUR_1 ?? "8", 10);
  const hour2 = parseInt(process.env.BACKUP_HOUR_2 ?? "20", 10);

  return phMinute === 0 && (phHour === hour1 || phHour === hour2);
}

// ─────────────────────────────────────────────
// 8. Cron Schedule
// ─────────────────────────────────────────────

log("info", "Firebase POS Backup Service starting...");
log("info", `Scheduled backups: 08:00 PHT and 20:00 PHT`);
log("info", `Collections: ${COLLECTIONS.join(", ")}`);
log("info", "Waiting for next scheduled window...\n");

// Run every minute; the time-gate inside decides whether to proceed.
// This avoids relying on server clock timezone alignment.
cron.schedule(
  "* * * * *",
  async () => {
    if (!isBackupTime()) return; // silent skip — not backup time yet
    try {
      await runBackup();
    } catch (err) {
      log("error", `Fatal backup error: ${err.message}`);
    }
  },
  {
    timezone: "Asia/Manila",
  }
);

// Keep process alive on Render's free tier (optional heartbeat log)
setInterval(() => {
  const phTime = new Date().toLocaleString("en-PH", {
    timeZone: "Asia/Manila",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  });
  process.stdout.write(`\r[Heartbeat] PHT: ${phTime} — waiting for next backup window...`);
}, 60_000);
