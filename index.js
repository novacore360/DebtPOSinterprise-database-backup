require("dotenv").config();
const admin = require("firebase-admin");
const cron = require("node-cron");
const http = require("http");

// ─────────────────────────────────────────────
// 1. Firebase App Initialization
// ─────────────────────────────────────────────

function parsePrivateKey(raw) {
  return raw.replace(/\\n/g, "\n");
}

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
const PORT = process.env.PORT || 3000;

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
// 4. Backup state (for status endpoint)
// ─────────────────────────────────────────────

const backupState = {
  lastRun: null,
  lastResults: null,
  isRunning: false,
  nextRun: null,
};

// ─────────────────────────────────────────────
// 5. Metadata Helpers
// ─────────────────────────────────────────────

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

async function writeBackupMetadata(collectionName, metadata) {
  await backupDB
    .collection(METADATA_COLLECTION)
    .doc(collectionName)
    .set(metadata, { merge: true });
}

// ─────────────────────────────────────────────
// 6. Core Sync Logic
// ─────────────────────────────────────────────

function buildChecksumMap(docs) {
  const map = {};
  for (const doc of docs) {
    const data = doc.data();
    const ts =
      data?.updatedAt?.toDate?.()?.toISOString() ||
      data?.updated_at?.toDate?.()?.toISOString() ||
      data?.createdAt?.toDate?.()?.toISOString() ||
      data?.created_at?.toDate?.()?.toISOString() ||
      JSON.stringify(data).length.toString();
    map[doc.id] = ts;
  }
  return map;
}

async function syncCollection(collectionName) {
  const stats = { reads: 0, writes: 0, deletes: 0, skipped: 0, status: "ok" };

  log("info", `━━━ Syncing collection: [${collectionName}] ━━━`);

  const meta = await readBackupMetadata(collectionName);
  const previousChecksums = meta?.docChecksums || {};
  log("info", `  Metadata loaded. Previously tracked ${Object.keys(previousChecksums).length} docs.`);

  let primarySnapshot;
  try {
    primarySnapshot = await primaryDB.collection(collectionName).get();
    stats.reads += primarySnapshot.size;
  } catch (err) {
    log("error", `  Failed to read primary [${collectionName}]: ${err.message}`);
    stats.status = "error";
    return stats;
  }

  if (primarySnapshot.empty) {
    log("warn", `  [${collectionName}] is EMPTY in primary. Skipping to avoid data loss.`);
    stats.status = "skipped_empty";
    return stats;
  }

  const primaryDocs = primarySnapshot.docs;
  const currentChecksums = buildChecksumMap(primaryDocs);

  const toUpsert = [];
  const toDelete = [];

  for (const doc of primaryDocs) {
    const prev = previousChecksums[doc.id];
    const curr = currentChecksums[doc.id];
    if (prev !== curr) {
      toUpsert.push(doc);
    } else {
      stats.skipped++;
    }
  }

  const currentIds = new Set(primaryDocs.map((d) => d.id));
  for (const oldId of Object.keys(previousChecksums)) {
    if (!currentIds.has(oldId)) {
      toDelete.push(oldId);
    }
  }

  log("info", `  Diff → upsert: ${toUpsert.length}, delete: ${toDelete.length}, unchanged: ${stats.skipped}`);

  if (toUpsert.length === 0 && toDelete.length === 0) {
    log("info", `  No changes detected for [${collectionName}]. Nothing to write.`);
    stats.status = "no_changes";
    return stats;
  }

  const BATCH_LIMIT = 400;

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
// 7. Master Backup Runner
// ─────────────────────────────────────────────

async function runBackup() {
  if (backupState.isRunning) {
    log("warn", "Backup already in progress, skipping.");
    return;
  }

  backupState.isRunning = true;
  const startTime = Date.now();
  const phTime = new Date().toLocaleString("en-PH", { timeZone: "Asia/Manila" });

  log("info", "╔══════════════════════════════════════════╗");
  log("info", "║       FIREBASE POS BACKUP STARTED        ║");
  log("info", `║  ${phTime.padEnd(40)}║`);
  log("info", "╚══════════════════════════════════════════╝");

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

  let totalReads = 0, totalWrites = 0, totalDeletes = 0;
  for (const stat of Object.values(results)) {
    totalReads += stat.reads || 0;
    totalWrites += stat.writes || 0;
    totalDeletes += stat.deletes || 0;
  }

  log("info", `Backup complete — reads: ${totalReads}, writes: ${totalWrites}, deletes: ${totalDeletes}, elapsed: ${elapsed}s`);

  backupState.lastRun = new Date().toISOString();
  backupState.lastResults = { results, totalReads, totalWrites, totalDeletes, elapsedSeconds: elapsed };
  backupState.isRunning = false;
}

// ─────────────────────────────────────────────
// 8. Time-gate
// ─────────────────────────────────────────────

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

function getNextBackupTime() {
  const now = new Date();
  const hour1 = parseInt(process.env.BACKUP_HOUR_1 ?? "8", 10);
  const hour2 = parseInt(process.env.BACKUP_HOUR_2 ?? "20", 10);

  const phNow = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Manila" }));
  const phHour = phNow.getHours();
  const phMin = phNow.getMinutes();

  let nextHour;
  if (phHour < hour1 || (phHour === hour1 && phMin === 0)) nextHour = hour1;
  else if (phHour < hour2 || (phHour === hour2 && phMin === 0)) nextHour = hour2;
  else nextHour = hour1 + 24;

  const next = new Date(phNow);
  next.setHours(nextHour, 0, 0, 0);
  return next.toLocaleString("en-PH", { timeZone: "Asia/Manila" });
}

// ─────────────────────────────────────────────
// 9. HTTP Server (required for Render Web Service)
// ─────────────────────────────────────────────

const server = http.createServer((req, res) => {
  const url = req.url;

  // Health check — Render pings this to keep the service alive
  if (url === "/" || url === "/health") {
    const phTime = new Date().toLocaleString("en-PH", { timeZone: "Asia/Manila" });
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      status: "ok",
      service: "Firebase POS Backup",
      currentTimePHT: phTime,
      nextBackup: getNextBackupTime(),
      isRunning: backupState.isRunning,
      lastRun: backupState.lastRun,
    }));
    return;
  }

  // Status page — shows last backup results
  if (url === "/status") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      service: "Firebase POS Backup",
      collections: COLLECTIONS,
      currentTimePHT: new Date().toLocaleString("en-PH", { timeZone: "Asia/Manila" }),
      nextBackup: getNextBackupTime(),
      isRunning: backupState.isRunning,
      lastRun: backupState.lastRun,
      lastResults: backupState.lastResults,
    }, null, 2));
    return;
  }

  // Manual trigger — useful for testing
  if (url === "/trigger" && req.method === "POST") {
    if (backupState.isRunning) {
      res.writeHead(409, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Backup already in progress" }));
      return;
    }
    res.writeHead(202, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ message: "Backup triggered manually" }));
    runBackup().catch((err) => log("error", `Manual trigger error: ${err.message}`));
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

server.listen(PORT, () => {
  log("info", `HTTP server listening on port ${PORT}`);
  log("info", `  GET  /health  — health check`);
  log("info", `  GET  /status  — last backup results`);
  log("info", `  POST /trigger — run backup now (for testing)`);
});

// ─────────────────────────────────────────────
// 10. Cron Schedule
// ─────────────────────────────────────────────

log("info", "Firebase POS Backup Service starting...");
log("info", `Scheduled: 08:00 PHT and 20:00 PHT daily`);
log("info", `Collections: ${COLLECTIONS.join(", ")}`);

cron.schedule(
  "* * * * *",
  async () => {
    if (!isBackupTime()) return;
    try {
      await runBackup();
    } catch (err) {
      log("error", `Fatal backup error: ${err.message}`);
    }
  },
  { timezone: "Asia/Manila" }
);
