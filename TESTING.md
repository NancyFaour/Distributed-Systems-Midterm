# Screenshot and test guide

Run commands in the PyCharm PowerShell terminal at the project root. Activate `.venv` first if necessary. Take screenshots only after observing the stated result. A smoke test is not the full workload evaluation. Do not include `.env` or API keys in screenshots.

## 0. Start tomorrow

If the full benchmark is still running, let it finish before starting screenshot tests. The startup script detects a running Windows benchmark and defers container changes. Check `Get-Content test-results/benchmark-full.log -Tail 25`; completion requires four verified comparison rows and the final PASS.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/start-lab.ps1
.\.venv\Scripts\Activate.ps1
```

Wait for healthy services. Open http://localhost:8080. Open `.env` privately and paste `API_KEY` into the console. The key is not a RabbitMQ or Grafana password. This lab uses a shared administrator key, not separate user accounts. Capture the console and `docker compose ps`.

## 1. Storage node operations

Run each line separately, capturing its PASS output before continuing:

```powershell
.\.venv\Scripts\python.exe scripts/test_storage_steps.py put
.\.venv\Scripts\python.exe scripts/test_storage_steps.py get
.\.venv\Scripts\python.exe scripts/test_storage_steps.py head
.\.venv\Scripts\python.exe scripts/test_storage_steps.py invalid-put
.\.venv\Scripts\python.exe scripts/test_storage_steps.py isolation
.\.venv\Scripts\python.exe scripts/test_storage_steps.py persistence
.\.venv\Scripts\python.exe scripts/test_storage_steps.py corruption
.\.venv\Scripts\python.exe scripts/test_storage_steps.py delete
```

Use `.\.venv\Scripts\python.exe scripts/test_storage_steps.py --help` if a phase name changes. The deliberate corruption test modifies only its own fixture. Screenshot the heartbeat list at http://localhost:8010/nodes as well. Four nodes satisfy the minimum; seven support six RS shards plus a repair spare.

## 2. Metadata, placement and deduplication

```powershell
.\.venv\Scripts\python.exe metadata_db.py
.\.venv\Scripts\python.exe test_fixed_chunking.py
.\.venv\Scripts\python.exe test_cdc.py
.\.venv\Scripts\python.exe test_placement.py
.\.venv\Scripts\python.exe test_object_store.py upload
.\.venv\Scripts\python.exe test_object_store.py dedup
.\.venv\Scripts\python.exe test_object_store.py read
.\.venv\Scripts\python.exe test_object_store.py delete-first
.\.venv\Scripts\python.exe test_object_store.py delete-last
.\.venv\Scripts\python.exe test_cdc_store.py
```

Capture fixed and CDC chunk sizes/hashes, exact reconstruction, partial ring movement, and reference counts before/after deletion. The original placement test simulates 10,000 hash assignments; the later rebalancing test migrates actual bytes. Distinguish them in the report.

## 3. Redundancy and automatic recovery

Run these sequentially, without a concurrent benchmark or rebalance demonstration:

```powershell
.\.venv\Scripts\python.exe test_redundancy_codec.py
.\.venv\Scripts\python.exe test_part4.py replication
.\.venv\Scripts\python.exe test_part4.py erasure
.\.venv\Scripts\python.exe test_part4.py quorum
.\.venv\Scripts\python.exe test_part4.py repair-replication
.\.venv\Scripts\python.exe test_part4.py repair-erasure
.\.venv\Scripts\python.exe test_part4.py scrub
```

Capture each PASS. The failure tests stop a real Docker node, wait for the missed-heartbeat timeout and background worker, verify restored redundancy, and restart the node. Scrubbing deliberately corrupts the fixture and waits for background repair. Allow time for the 15-second failure timeout or the configured scrub interval. Inspect http://localhost:8020/events and the console’s Recovery tab. Do not stop these tests halfway through a failure simulation.

## 4. Gateway, API responses and authentication

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_unit.py -q
$env:RUN_INTEGRATION='1'
.\.venv\Scripts\python.exe -m pytest tests/test_gateway.py -q
.\.venv\Scripts\python.exe scripts/test-http.py --phase crud
.\.venv\Scripts\python.exe scripts/test-http.py --phase search
.\.venv\Scripts\python.exe scripts/test-http.py --phase cluster
.\.venv\Scripts\python.exe scripts/test-http.py --phase balance
```

Pytest covers invalid API keys, shared rate limiting, upload/download, versions, HEAD, byte ranges including HTTP 416, metadata, tags, invalid inputs, empty objects, oversized requests, RS multichunk reads and deduplication. The HTTP script exercises real Nginx routing. Capture the status codes and PASS lines. The integration tests intentionally skip unless `RUN_INTEGRATION=1` is set; skipped tests do not count as successful integration evidence.

Interactive docs: http://localhost:8080/api/docs. Click **Authorize** and enter the API key. A basic PowerShell curl example uses an environment variable rather than writing a secret into a screenshot:

```powershell
curl.exe -i http://localhost:8080/api/buckets -H "X-API-Key: $env:STORAGE_API_KEY"
```

Set `STORAGE_API_KEY` privately from `.env` first. The lab is S3-like: bucket/key/version operations with JSON and an API key. AWS SigV4/XML and full AWS SDK compatibility are outside this implementation.

## 5. React interface and Elasticsearch

1. Create a replication bucket and an erasure-coded bucket.
2. Upload a small JSON file with tags such as `{"course":"distributed"}`; select fixed or CDC chunking.
3. Browse objects, inspect ordered hashes and placement, and download the object.
4. Search by key prefix, minimum/maximum size, creation dates and tags. Wait for the asynchronous search outbox to drain. Capture a search result labeled **Elasticsearch**.
5. Open Cluster and capture node health and chunk distribution. Open Recovery and capture a completed job and repair traffic.

Screenshots should show actual results, not just empty pages or source code. Search includes object versions and is eventually consistent. PostgreSQL remains authoritative.

## 6. Required workload comparison

First verify the experiment with a smoke run:

```powershell
.\.venv\Scripts\python.exe benchmark_redundancy.py --profile smoke --output test-results/benchmark-smoke.json
```

Then run the assignment-sized workload:

```powershell
.\.venv\Scripts\python.exe benchmark_redundancy.py --profile full --workers 4 --output test-results/benchmark-full.json
```

Full means **5,000 JSON files of 1–50 KiB** and **10 binary files of 100 MiB each**, each evaluated under replication and RS 4+2. The same input files are used for both policies. Every download is checked by SHA-256 before and after repair. The test stops a real node, measures lost payload and repair reads/writes, restores it and removes only its own objects. Run it alone; it temporarily pauses automatic scanning for controlled per-policy repair measurement. The automatic path is demonstrated separately in phase 3.

The workload experiment calls the coordinator and storage APIs directly. Gateway streaming, authentication and byte ranges are evaluated separately with the HTTP tests. To capture a real 100 MiB gateway transfer, run `.\.venv\Scripts\python.exe scripts/test-large-gateway.py` after the benchmark finishes.

If the benchmark process was forcibly closed, restore stopped storage containers first. Check `http://localhost:8020/status`. After confirming no benchmark is still running, use `Invoke-RestMethod -Method Post http://localhost:8020/resume` to resume automatic repair. The pause flag is persistent so a worker restart cannot invalidate a running benchmark.

Capture the full profile’s four comparison rows and final PASS. Include logical bytes, stored payload bytes, storage factor, repair read/write bytes, total repair traffic and traffic per lost byte. Payload metrics include fragment framing/padding and exclude HTTP/TCP overhead, control traffic and filesystem allocation. Approximately 3× versus 1.5× are theoretical expectations; report the measured values, especially for tiny files where headers matter. The default four workers favor a modest development computer; runtime depends on disk and Docker performance.

## 7. Bonus evidence

```powershell
docker compose --profile scale up -d --no-build --wait storage-node-8
.\.venv\Scripts\python.exe scripts/test-rebalance.py
```

Capture actual moved-fragment percentages for adding and removing node-8 and successful downloads after movement. In Recovery, submit repair/scrub/GC jobs and capture their worker IDs and completed status. `.\.venv\Scripts\python.exe scripts/test-http.py --phase balance` demonstrates two gateway instances.

For gateway failover, run `.\.venv\Scripts\python.exe scripts/test-gateway-failover.py`. It verifies both gateways, stops gateway-1, checks authentication and routing through gateway-2, performs authenticated CRUD/range/integrity checks, and restores gateway-1 in a `finally` block. Capture the stopped/running service status and PASS messages. Nginx uses a 2-second connection timeout so an unreachable gateway cannot consume the client's 10-second authentication-request timeout. Large-transfer read/send timeouts remain separate; an upload body already sent upstream is not automatically replayed.

Open Grafana at http://localhost:3000, log in using the local Grafana credentials, and open **Distributed Object Storage**. Generate traffic and capture populated panels. A flat zero value can be valid; an empty panel with no series needs investigation. Prometheus is at http://localhost:9090.

Kubernetes, PostgreSQL HA, cloud deployment and HPA evidence are described in DEPLOYMENT.md. Prepared manifests do not count as a deployed cloud system.

## 8. Report

Keep the existing single Word report. Add phase headings, the command/test purpose, one or more readable screenshots, the observed result and a short explanation. Include the actual benchmark table. Use the write, read and failure/repair paths in `docs/framework-flowchart.mmd` as the implementation-aligned flowchart source; compare it with your previously prepared diagram before inserting the final version. Do not describe an unrun test or an undeployed cloud feature as completed.

Three compact diagrams are also rendered as `docs/write-path.png`, `docs/read-path.png` and `docs/repair-path.png`, with SVG versions for vector-quality insertion in Word. Each phase can occupy its own figure. The original report file is retained.

## Validation recorded on 5 November 2026

- 21 pytest unit/integration tests passed, plus real HTTP CRUD, Elasticsearch search, two-gateway balancing and operation with one gateway stopped.
- A real 100 MiB HTTP transfer passed full SHA-256 and suffix-range verification.
- An isolated two-worker RabbitMQ test physically moved 20 of 120 fixture fragments when adding node-8, and 20 when removing it; reconstructed bytes matched.
- Isolated automatic recovery tests passed for replication and RS, including new EC uploads using healthy targets during an outage and background corruption repair.
- Kubernetes pod replacement preserved the original fragment on its PVC and returned the original object bytes.
- The gateway HPA reached six available replicas. A three-instance PostgreSQL cluster was enabled; deleting its primary led to a different primary and a successful download with the same hash (observed recovery: 47.99 seconds in this run).
- After the final redeployment, Kubernetes HTTP upload/download, range reads, Elasticsearch search and deletion passed against the HA database. A 45-second load check recorded 493 successful requests and zero failures, with six ready gateways. RabbitMQ's readiness check was changed to a lightweight TCP probe after CLI-probe timeouts were observed under CPU limits.
- The smoke workload comparison passed. Assignment-sized results are written incrementally to `test-results/benchmark-full.json`; count it as complete only when its log contains the final workload PASS and all four rows have verified downloads. These checks are local; no cloud-provider deployment is claimed.

Run phase 3 failure experiments only after the full benchmark completes. Logs in `test-results/` record actual checks; they do not replace the screenshots you capture for the report.

## Validation recorded on 6 November 2026

The full benchmark completed all four comparisons with verified downloads before and after repair. The small-file storage factors were 3.005658 for replication and 1.510709 for RS 4+2; the large-file factors were 3.000035 and 1.500066. The measured repair payload per lost byte was 2.0 for replication and 5.0 for RS in both workloads.

A recovery bug was fixed after a failed screenshot test: intact fragments retained stale `missing` labels after temporary heartbeat loss. Maintenance now fetches and hash-verifies returning fragments before restoring their healthy state and using them as repair sources. Both policy regression cases and the full suite of 23 unit/integration tests passed. The real `repair-replication` test also completed automatic repair onto a different node, verified the original file SHA-256 and restored the stopped container. Its recorded output is `test-results/repair-replication-fixed.log`.

The stopped-gateway test exposed an Nginx connection wait exceeding the client's timeout. With the 2-second connection limit, eight fresh authentication checks and eight health checks passed through the surviving gateway, followed by authenticated CRUD, byte-range and SHA-256 checks. The slowest authentication/health pair in this run took 2.250 seconds. Gateway-1 was restored and both containers were healthy. Evidence is in `test-results/gateway-failover-fixed.log`; use the automatic failover script for a fresh screenshot.
