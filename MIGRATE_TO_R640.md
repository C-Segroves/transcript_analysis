# Migrating Postgres + task server to the R640

Moves the database and the task server off the i3 desktop (192.168.1.204) onto a
new CT on the R640, over the LAN. The desktop becomes a normal worker (or frees
up entirely).

**Why both move together:** the launchers derive the task‑server host *from* the
DB host in `config/db_config.json`. If Postgres and the server share one IP, every
worker just needs that one IP and the auto‑detect keeps working. So we run both in
**one CT** on the R640 (Postgres container + server container, one IP).

**Safety note:** each score row is a complete, committed result, so stopping a
client mid‑model never corrupts data — it just pauses a model that resumes later.
We stop all writers *before* the snapshot only so nothing is written to the old DB
after the copy.

---

## Step 0 — Check the DB size first (decides the method)

On the desktop:
```
docker exec -it segroves_postgres psql -U postgres -c "SELECT pg_size_pretty(pg_database_size('postgres'));"
docker exec -it segroves_postgres postgres --version
```
- Note the **size** and the **Postgres major version** (the target must match it).
- If it's up to a few hundred GB → use **pg_dump/restore** (below, portable, simple).
- If it's very large (TB‑scale) → consider the **physical volume copy** in the
  Appendix instead (faster, but requires the exact same Postgres version).

Make sure the desktop has free disk for the dump file, and the R640 has room for
the dump **and** the restored DB.

---

## Step 1 — Prepare the target CT on the R640

Create a CT (Docker‑capable: nesting on) sized for the DB + server:
- **~8–10 vCPU, 24–32 GB RAM**, plenty of disk.
- On the Proxmox host: `pct set <CTID> --features nesting=1,keyctl=1` then reboot.
- Inside it: install Docker (`curl -fsSL https://get.docker.com | sh`).

Run a Postgres container **of the same major version** you noted in Step 0, e.g.:
```
docker run -d --name pg-main -p 5433:5432 \
  -e POSTGRES_PASSWORD=<your-db-password> \
  -v pgdata:/var/lib/postgresql/data \
  postgres:<MAJOR>
```
(Use port 5433 to match your current `db_config.json`, or pick one and update the
configs accordingly.) Don't start the task server yet — DB first.

Note this CT's **LAN IP** — call it `<R640_IP>`. That becomes the new DB *and*
server host.

---

## Step 2 — Stop all writers (graceful pause)

Nothing should be writing to the old DB during the copy.

**On every worker machine** (Linux CTs):
```
docker rm -f $(docker ps -q --filter name=$(hostname)-)
```
Windows worker boxes:
```
for /f %n in ('docker ps -q --filter name^=%COMPUTERNAME%-') do docker rm -f %n
```

**On the desktop (192.168.1.204):** stop the local client and the task server, but
**leave Postgres (`segroves_postgres`) running** — we need it to dump:
```
docker rm -f client-container server-container
docker ps   (confirm only segroves_postgres + gitea remain)
```

Now the database is quiet.

---

## Step 3 — Dump the database (desktop)

```
docker exec -t segroves_postgres pg_dump -U postgres -Fc -d postgres -f /tmp/transcript.dump
docker cp segroves_postgres:/tmp/transcript.dump C:\transcript.dump
```
`-Fc` is the compressed custom format (needed for parallel restore).

---

## Step 4 — Transfer the dump to the R640

Any LAN method works. From the desktop, e.g. SCP to the R640 CT:
```
scp C:\transcript.dump root@<R640_IP>:/root/transcript.dump
```
(or copy via an SMB share / USB if SCP isn't set up). Large dumps take a while.

---

## Step 5 — Restore into the new Postgres (R640 CT)

```
docker cp /root/transcript.dump pg-main:/tmp/transcript.dump
docker exec -it pg-main pg_restore -U postgres -d postgres -j 4 --no-owner /tmp/transcript.dump
```
`-j 4` restores in parallel (raise it if the CT has more cores). Indexes rebuild
during restore, so this is the slow part for big tables — let it finish.

---

## Step 6 — Verify the copy

Compare a few counts between old and new (small tables are exact; big ones use the
estimate). On both DBs:
```
SELECT (SELECT count(*) FROM channel_table) AS channels,
       (SELECT count(*) FROM vid_table)     AS vids,
       (SELECT reltuples::bigint FROM pg_class WHERE relname='vid_score_table') AS scores_est;
```
`channels` and `vids` should match exactly; `scores_est` should be in the same
ballpark (run `ANALYZE vid_score_table;` on the new DB first to refresh it).

---

## Step 7 — Start the task server on the R640 CT

Get the repo + config onto the CT (clone, then create `config/db_config.json` and
`config/YouTube.txt`). **Point `db_config.json` at the local Postgres:**
```json
{ "dbname": "postgres", "user": "postgres", "password": "<pw>", "host": "<R640_IP>", "port": "5433" }
```
Build and run the server (it also serves the dashboard on 8080):
```
docker rm -f server-container 2>/dev/null
docker build -t my-server-image -f server/Dockerfile .
docker run -d --name server-container -p 5000:5000 -p 8080:8080 my-server-image
```
Open `http://<R640_IP>:8080` — the dashboard should load against the migrated DB.

---

## Step 8 — Re-point the fleet

On **each worker machine**, edit `config/db_config.json` so `host` is `<R640_IP>`,
then relaunch (rebuilds the image with the new config; `SERVER_HOST` is auto‑read
from that host, so it now points at the R640 server too):
```
# Linux CT
cd /opt/transcript_analysis
nano config/db_config.json        # set "host": "<R640_IP>"
./launch_fleet.sh <vCPUs> 1
```
```
REM Windows
notepad config\db_config.json     REM set "host": "<R640_IP>"
launch_fleet.bat <cores> 1
```
Workers reconnect to the new server, get assignments, and resume scoring — no data
lost, because pending work is "videos missing a real score," derived fresh from the
migrated DB.

---

## Step 9 — Decommission the old DB (after you're confident)

Once everything's been running against the R640 for a while and the dashboard looks
right, retire the desktop's old stack — but **keep the dump file as a backup** for a
few days before deleting anything:
```
docker rm -f segroves_postgres      # on the desktop, when ready
```
The desktop is now free; if you want it scoring, set its `config/db_config.json`
host to `<R640_IP>` and run a client there too.

---

## Appendix A — Reduce RAM on the worker CTs

Scoring is CPU‑bound; each client uses well under 1 GB, so 14 GB CTs are wasted RAM.
On the **Proxmox host**, for each worker CT (LXC memory is adjustable live):
```
pct set <CTID> --memory 4096 --swap 1024
```
(4 GB RAM, 1 GB swap — generous for 4 clients + an island worker + Ubuntu.) Or via
GUI: CT → Resources → Memory. Do this while the CT is using less than the new limit
(it is). Don't add RAM to speed scoring — it won't; the lever is vCPUs: set the CT's
core count to what you want it to use and run that many clients (`./launch_fleet.sh
<vCPUs> 1`).

---

## Appendix B — Physical volume copy (only for very large DBs)

Faster than dump/restore for TB‑scale DBs, but the **Postgres major version must be
identical** on both ends.

On the desktop (after Step 2, with all writers stopped):
```
docker stop segroves_postgres
docker run --rm -v segroves_postgres_data:/data -v C:\:/backup alpine \
  tar czf /backup/pgdata.tgz -C /data .
```
(Replace `segroves_postgres_data` with the actual volume from
`docker inspect segroves_postgres` → Mounts.) Transfer `pgdata.tgz` to the R640,
then on a **stopped** same‑version Postgres container with an empty volume, untar it
into that volume and start it. Verify as in Step 6.

---

## Rollback

If anything looks wrong, you haven't lost the original: the desktop's
`segroves_postgres` still has all the data (you only stopped writers, you didn't
delete it). Point `config/db_config.json` back to `192.168.1.204`, restart the old
`server-container`, relaunch the fleet, and you're back where you started.

---

## Appendix C — Size and tune the Postgres CT

If you created the Postgres CT with the default **4 vCPU / 8 GB**, bump it up — it's
the shared bottleneck for the whole fleet, so it should be the best‑resourced CT.

**Recommended on the R640** (16c/32t, 72 GB): **8–12 vCPU and 32 GB RAM**, which
still leaves room for the server CT, island workers, your other Postgres CT, and a
future Ollama/T4 VM.

On the **Proxmox host** — LXC cores and memory adjust live, no reboot needed:
```
pct set <CTID> --cores 8 --memory 32768 --swap 4096
```
(`--cores 8`, 32 GB RAM, 4 GB swap; use 10–12 cores if you can spare them. GUI
equivalent: CT → Resources → edit Cores and Memory.)

**Raising the CT's RAM does nothing until Postgres is told to use it** — the default
container `shared_buffers` is tiny (128 MB). Tune it (these persist in the data dir):
```
docker exec -it pg-main psql -U postgres -c "ALTER SYSTEM SET shared_buffers='8GB';"
docker exec -it pg-main psql -U postgres -c "ALTER SYSTEM SET effective_cache_size='24GB';"
docker exec -it pg-main psql -U postgres -c "ALTER SYSTEM SET work_mem='64MB';"
docker exec -it pg-main psql -U postgres -c "ALTER SYSTEM SET maintenance_work_mem='1GB';"
docker exec -it pg-main psql -U postgres -c "ALTER SYSTEM SET max_connections='200';"
docker restart pg-main
```
- `shared_buffers` ≈ 25% of the CT's RAM; `effective_cache_size` ≈ 75% (it's a hint,
  not an allocation). With 32 GB, 8 GB / 24 GB is a good split.
- `max_connections = 200`: each client process keeps a small pool open, and you have
  ~30+ of them plus island workers, the server, and the dashboard — the default 100
  can run out. If you see `FATAL: remaining connection slots are reserved` / `too many
  clients already`, raise it further (and give shared_buffers a bit more headroom).
- `shared_buffers` and `max_connections` need the `docker restart`; the rest reload live.

Confirm it took:
```
docker exec -it pg-main psql -U postgres -c "SHOW shared_buffers; SHOW max_connections;"
```

---

## Appendix D — Storage sizing for the Postgres CT

Check the real numbers before trusting the CT's disk. On the source DB:
```
docker exec -it segroves_postgres psql -U postgres -c "SELECT pg_size_pretty(pg_database_size('postgres'));"
docker exec -it segroves_postgres psql -U postgres -c \
  "SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS total
   FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 10;"
```

Budget the CT's disk for **all** of these at once:
- the **current DB size**, plus
- the **dump file**, which lives on the CT next to the restored DB during the
  restore (deleted afterward), plus
- **growth**: `vid_score_table` stores a per‑word score array per (video, model);
  re‑scoring the empty rows into real arrays makes them much larger, and you're only
  ~7% through real scoring — so the DB will grow a lot. The transcript‑chunk
  embeddings and 8values data add more on top, plus
- **~20% free headroom** (never run a DB volume near full).

**Measured:** the DB is **~1.03 TB** today, on a CT allocated 1.92 TiB — already ~half
full. It will grow severalfold as the empty rows get re‑scored into full per‑word
arrays (only ~7% through real scoring) plus the embeddings and 8values data. So 1.92
TiB is **not enough**.

With ~8.4 TB total on the R640, storage isn't the constraint. Provision the new
Postgres CT at **~4 TB** (≈4× the current DB, room to grow), leaving the rest for the
other Postgres CT, backups, the migration dump, and headroom. Two checks: that the
CT's disk sits on the pool that has the free space, and that the Postgres data lands
on the **fastest tier** (SSD/NVMe) — a 1 TB+ DB does constant random I/O from the
fleet, so bulk HDD space is plentiful but slow.

**At ~1 TB, prefer the physical volume copy (Appendix B) over pg_dump/restore:** a
logical restore rebuilds every index from scratch (many hours at this size), while the
physical copy moves the indexes as‑is. Match the Postgres major version on both ends.

**Grow an LXC CT's disk** (Proxmox host), if the storage pool has free space:
```
pct resize <CTID> rootfs +500G          # grows the volume + filesystem, live
```
Caveat: this only works if the underlying Proxmox **storage pool** has the room.
1.92 TiB may be the whole pool on that box — in which case "more" means adding
drives, or putting the Postgres data on a larger/faster storage via a dedicated
mount point, e.g.:
```
pct set <CTID> --mp0 <big-storage>:600,mp=/var/lib/postgresql      # 600 GB data mount
```
A DB this size is happiest on its own large, fast volume (NVMe/SSD‑backed) rather
than the CT root disk.

**Strategic note:** the per‑word score arrays are the dominant storage cost. The
embeddings approach (task #18) is far more compact, and you likely don't need to
keep the raw per‑word arrays forever once islands are derived — options if disk
gets tight.

