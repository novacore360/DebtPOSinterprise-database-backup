# Firebase POS Backup Service

Automated Firestore-to-Firestore backup for your POS system. Runs at **8:00 AM** and **8:00 PM Philippine Time** daily, with smart diffing to minimize Firestore reads.

---

## How It Works

```
Every minute (cron)
    │
    ▼
Is it 8:00 AM or 8:00 PM PHT?
    │
    ├── No  → Sleep (no reads at all)
    │
    └── Yes
           │
           ▼
    Read backup metadata (1 read per collection — cheap)
           │
           ▼
    Read ALL primary documents
           │
           ▼
    Primary collection empty?
           │
           ├── Yes → SKIP (data-loss guard)
           │
           └── No
                  │
                  ▼
           Diff checksums vs. metadata
                  │
                  ├── No changes → Exit (0 backup writes)
                  │
                  └── Changes found
                         │
                         ├── Upsert changed/new docs (batched)
                         ├── Delete stale docs (batched)
                         └── Update metadata
```

---

## Catch-Up on Missed Backups (Free-Tier Sleep Protection)

If this service runs on a platform that sleeps when idle (e.g. Render free tier), the cron scheduler is also asleep — so if nobody hits the URL right at 8:00 or 20:00, that backup is silently skipped, **and it won't run itself when the server later wakes up**.

To fix this, every request to `/`, `/health`, or `/status` (plus every cron tick) triggers a **catch-up check**:

```
Request comes in (e.g. you open the URL at 8:10)
    │
    ▼
What's the most recent scheduled slot? (today 8 AM/8 PM, or yesterday's)
    │
    ▼
Is "now" within the grace window of that slot? (default 180 min)
    │
    ├── No  → Too late, do nothing — wait for next real slot
    │
    └── Yes
           │
           ▼
    Has a backup already run for that slot? (checks __global__ marker doc)
           │
           ├── Yes → Already covered, do nothing
           │
           └── No  → Run the backup now, labeled "catchup:<source>"
```

This relies on a small marker document, `__backup_metadata__/__global__`, written every time `runBackup()` finishes — separate from the per-collection metadata, since that only updates when there are actual data changes to write.

**Config:**
```env
CATCHUP_GRACE_MINUTES=180   # how late a catch-up is still allowed to run (default 3 hours)
```

> Note: this is a safety net, not a fix for the underlying problem. If your service sleeps and nobody visits it within the grace window, that backup is still missed — the real fix is deploying as a Background Worker (see below) or pinging it with an external uptime monitor (e.g. UptimeRobot, cron-job.org) around 8:00/20:00 PHT.



### 1. Get Service Account Keys

For **both** Firebase projects (primary & backup):

1. Go to [Firebase Console](https://console.firebase.google.com)
2. Project Settings → Service Accounts
3. Click **Generate new private key** → download JSON
4. Copy `client_email` and `private_key` from each JSON file

### 2. Create `.env` file

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

```env
# Primary (hallow-2c7e6)
PRIMARY_PROJECT_ID=hallow-2c7e6
PRIMARY_CLIENT_EMAIL=firebase-adminsdk-xxxx@hallow-2c7e6.iam.gserviceaccount.com
PRIMARY_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----\n"

# Backup (nova-e6655)
BACKUP_PROJECT_ID=nova-e6655
BACKUP_CLIENT_EMAIL=firebase-adminsdk-xxxx@nova-e6655.iam.gserviceaccount.com
BACKUP_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----\n"

# Collections
COLLECTIONS=customers,purchases,products
TZ=Asia/Manila
BACKUP_HOUR_1=8
BACKUP_HOUR_2=20

# Catch-up grace window (minutes) — see "Catch-Up on Missed Backups" above
CATCHUP_GRACE_MINUTES=180
```

> ⚠️ **Important:** When pasting `private_key` in `.env`, keep it as one line with literal `\n` (not real newlines). The code handles the conversion.

### 3. Install & Run

```bash
npm install
npm start
```

---

## Deploying to Render

1. Push this folder to a GitHub repo
2. On [Render](https://render.com), create a new **Background Worker**
3. Set:
   - **Build Command:** `npm install`
   - **Start Command:** `npm start`
   - **Environment:** Node
4. Add all `.env` variables under **Environment → Environment Variables**
   - Paste `PRIMARY_PRIVATE_KEY` and `BACKUP_PRIVATE_KEY` exactly as-is (with `\n`)
5. Deploy ✅

---

## Read Cost Optimization

| Strategy | Savings |
|---|---|
| Time-gate (cron checks time first) | Zero reads outside backup window |
| Metadata checksum diffing | Only writes changed documents |
| Skip empty collection | Prevents accidental wipes |
| Batched writes (400/batch) | Efficient Firestore usage |

A typical backup with no changes = **3 reads** (1 metadata read per collection).

---

## Backup Metadata

The backup database stores a hidden collection `__backup_metadata__` with one document per collection. It tracks:

- `docChecksums` — map of `{ docId: lastSeenTimestamp }` for diffing
- `lastDocCount` — number of docs at last sync
- `lastSyncedAt` — ISO timestamp of last successful sync
- `lastSyncStats` — reads/writes/deletes from last run

---

## Collections Backed Up

- `customers`
- `purchases`  
- `products`

To add more, update `COLLECTIONS` in your `.env`.
