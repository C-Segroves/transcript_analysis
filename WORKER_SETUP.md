# Worker setup — adding a machine to the scoring / island fleet

How to bring a new worker online (a Proxmox LXC container, a VM, or a
Windows/macOS box). A worker runs **scoring clients** (transcripts → model
scores) and **island finders** (scores → islands), all pointed at the task
server. The server, database, and dashboard run elsewhere.

- **Task server / dashboard:** `<SERVER_IP>` (port `5000` task socket, `8080` dashboard)
- **Database:** `<SERVER_IP>:<DB_PORT>`
- **Repo:** https://github.com/C-Segroves/transcript_analysis

Replace `<SERVER_IP>` / `<DB_PORT>` with your own values (they live in
`config/db_config.json`, which is gitignored). The launcher reads the task-server
host from that file automatically, so you don't hardcode it anywhere.

---

## Quick version

```bash
# (on the Proxmox host, once) enable Docker support in the CT, then reboot it
pct set <CTID> --features nesting=1,keyctl=1
pct reboot <CTID>

# (inside the CT)
apt update && apt -y upgrade
apt -y install git curl ca-certificates
curl -fsSL https://get.docker.com | sh
systemctl enable --now docker

cd /opt
git clone https://github.com/C-Segroves/transcript_analysis.git
cd transcript_analysis

mkdir -p config
nano config/db_config.json          # paste the DB config (see below)

chmod +x launch_fleet.sh
sed -i 's/\r$//' launch_fleet.sh     # strip CRLF in case it was committed from Windows
./launch_fleet.sh                    # 1 scoring client per CPU + 1 island worker


docker ps
docker logs -f $(hostname)-client-1
```

That's it. The machine starts contributing within a minute (it may sit in
`pause` until the server finishes its nightly maintenance, then starts working).

---

## Step by step (Proxmox Ubuntu/Debian CT)

### 1. Enable Docker in the container (Proxmox host)
Docker won't run in an unprivileged LXC without nesting. On the **Proxmox host**:

```bash
pct set <CTID> --features nesting=1,keyctl=1
pct reboot <CTID>
```

(GUI equivalent: select the CT → **Options → Features** → tick **nesting** and
**keyctl**, then reboot.)

### 2. Install tools + Docker (inside the CT, as root)
```bash
apt update && apt -y upgrade
apt -y install git curl ca-certificates
curl -fsSL https://get.docker.com | sh
systemctl enable --now docker
docker run --rm hello-world          # should print "Hello from Docker!"
```
If `hello-world` fails with a storage-driver error, re-check step 1 (nesting) —
that's almost always the cause.

### 3. Clone the repo
```bash
cd /opt
git clone https://github.com/C-Segroves/transcript_analysis.git
cd transcript_analysis
```

### 4. Create the DB config
`config/` is gitignored, so it isn't in the clone — create it:

```bash
mkdir -p config
nano config/db_config.json
```

```json
{
  "dbname": "postgres",
  "user": "postgres",
  "password": "YOUR_DB_PASSWORD",
  "host": "<SERVER_IP>",
  "port": "<DB_PORT>"
}
```

Workers only need `db_config.json`. `config/YouTube.txt` is **only** for the
server's maintenance job, not for workers.

### 5. Launch
```bash
chmod +x launch_fleet.sh
sed -i 's/\r$//' launch_fleet.sh
./launch_fleet.sh
```

`launch_fleet.sh [num_clients] [num_island_workers]`:
- defaults to **one scoring client per CPU** + **one island worker**
- `./launch_fleet.sh 4 1` → 4 clients, 1 island worker
- `SERVER_HOST=<server-ip> ./launch_fleet.sh` → point at a task server other than the DB host

The script builds both Docker images and starts the containers with
`--restart unless-stopped` (so they survive reboots) and unique names
(`<host>-client-N`, `<host>-island-N`).

### 6. Verify
```bash
docker ps
docker logs -f $(hostname)-client-1
```
A healthy client logs: connect → `play` → `Assigned model …` → `Model …: N pending videos`.
The island worker logs `Model … islands stored` once the island queue is seeded
(see below). It's normal to briefly see `pause` (server maintenance) or
`No pending tasks` (island queue not seeded yet).

---

## One-time: seed the island queue

Island finders only process tasks in `island_task_table`. That table must be
seeded once from the existing scores — until then island workers sit idle
("No pending tasks"). Run this **once, on a single machine** (any worker is fine):

```bash
SEED=1 ./launch_fleet.sh
```

It enqueues one task per real (non-empty) scored `(vid_id, model_id)` pair, in
committed batches with progress logging — safe to interrupt and re-run. After
more scoring happens, re-run `SEED=1 ./launch_fleet.sh` (or
`python setup_island_tables.py --refresh`) to top up new pairs. Every *other*
machine just runs the plain `./launch_fleet.sh`.

---

## Updating a worker after code changes

A Docker image is a frozen snapshot of the code at build time, so after pulling
new code you must rebuild. `launch_fleet.sh` does this automatically:

```bash
git pull
./launch_fleet.sh
```

This rebuilds both images and recreates the containers with the new code.

---

## Windows / macOS box

Same idea with `launch_fleet.bat` and Docker Desktop:

```bat
git clone https://github.com/C-Segroves/transcript_analysis.git
cd transcript_analysis
mkdir config & notepad config\db_config.json
launch_fleet.bat                  REM or: launch_fleet.bat 6 1
```
(Intel Macs run the linux/amd64 images natively — no emulation.)

---

## Tuning & tips

- **Cores:** each scoring client uses one core. Run one client per core you want
  to use (the default). To use a 16-core box, that's ~16 clients.
- **`MODEL_CACHE_SIZE`:** the launcher sets this to `1` (model-major scoring
  never reuses a model, so a bigger cache only wastes RAM). Leave it.
- **Island workers:** islands are light; 1–2 per machine is plenty. They're
  DB-coordinated and never talk to the task server, so they keep running even
  during the server's maintenance window.
- **No double work:** the server assigns each model to one client at a time, and
  island workers claim tasks atomically — so you can run as many machines as you
  like without overlap.
- **Watch progress:** the dashboard at http://<SERVER_IP>:8080 shows the
  Workers panel (who's on what) and overall scoring / island progress.

### Stop the workers on a box
```bash
docker rm -f $(docker ps -q --filter name=$(hostname)-)
```
Windows:
```bat
for /f %n in ('docker ps -q --filter name^=%COMPUTERNAME%-') do docker rm -f %n
```

---

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `docker: command not found` | Docker install didn't finish — re-run step 2. |
| `hello-world` storage-driver error | LXC nesting not enabled — step 1, then reboot the CT. |
| `./launch_fleet.sh: bad interpreter` | CRLF line endings — `sed -i 's/\r$//' launch_fleet.sh`. |
| `ERROR: config/db_config.json not found` | Create it (step 4). |
| Client stuck on `pause` | Server is mid-maintenance; it resumes automatically. |
| Island worker: `No pending tasks` forever | Island queue not seeded — run `SEED=1 ./launch_fleet.sh` once. |
| `column ... does not exist` | Worker image is stale — `git pull` then `./launch_fleet.sh` to rebuild. |
