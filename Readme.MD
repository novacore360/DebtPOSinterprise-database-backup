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

## Setup

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
