import React, { useEffect, useState, useRef } from "react";
import { createRoot } from "react-dom/client";
import "./style.css";

const size = (n) =>
  n == null
    ? "—"
    : n < 1024
      ? `${n} B`
      : `${(n / 1024 ** Math.floor(Math.log(n) / Math.log(1024))).toFixed(1)} ${["B", "KiB", "MiB", "GiB", "TiB"][Math.floor(Math.log(n) / Math.log(1024))]}`;
const pathFor = (row) =>
  `/buckets/${encodeURIComponent(row.bucket_name)}/objects/${row.object_key.split("/").map(encodeURIComponent).join("/")}?version=${row.version}`;

function App() {
  const fileInput = useRef(null);
  const [key, setKey] = useState(
    sessionStorage.getItem("storage-api-key") || "",
  );
  const [draftKey, setDraftKey] = useState("");
  const [tab, setTab] = useState("Objects");
  const [buckets, setBuckets] = useState([]),
    [bucket, setBucket] = useState("");
  const [objects, setObjects] = useState([]),
    [cluster, setCluster] = useState(null);
  const [error, setError] = useState(""),
    [notice, setNotice] = useState(""),
    [busy, setBusy] = useState(false);
  const [newBucket, setNewBucket] = useState(""),
    [policy, setPolicy] = useState("replication");
  const [prefix, setPrefix] = useState(""),
    [text, setText] = useState(""),
    [minSize, setMinSize] = useState(""),
    [maxSize, setMaxSize] = useState("");
  const [after, setAfter] = useState(""),
    [before, setBefore] = useState(""),
    [tags, setTags] = useState("{}");
  const [files, setFiles] = useState([]),
    [uploadTags, setUploadTags] = useState("{}"),
    [chunking, setChunking] = useState("fixed");
  const [progress, setProgress] = useState(null),
    [details, setDetails] = useState(null),
    [engine, setEngine] = useState("PostgreSQL listing");
  const [nextOffset, setNextOffset] = useState(null),
    [offset, setOffset] = useState(0);

  async function api(path, options = {}) {
    const response = await fetch("/api" + path, {
      ...options,
      headers: { "X-API-Key": key, ...options.headers },
    });
    if (!response.ok) {
      const body = await response
        .json()
        .catch(() => ({ detail: response.statusText }));
      throw new Error(
        `${response.status}: ${typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail)}`,
      );
    }
    return response.status === 204 ? null : response.json();
  }
  async function action(fn) {
    setError("");
    setBusy(true);
    try {
      await fn();
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  }
  async function loadBuckets() {
    const r = await api("/buckets");
    setBuckets(r.buckets);
    if (!bucket && r.buckets.length) setBucket(r.buckets[0].bucket_name);
  }
  async function loadObjects(page = 0) {
    if (!bucket) {
      setObjects([]);
      return;
    }
    const r = await api(
      `/buckets/${encodeURIComponent(bucket)}/objects?limit=100&offset=${page}&prefix=${encodeURIComponent(prefix)}`,
    );
    setObjects(r.objects);
    setNextOffset(r.next_offset);
    setOffset(page);
    setEngine("PostgreSQL listing");
  }
  async function loadCluster() {
    setCluster(await api("/cluster"));
  }
  useEffect(() => {
    if (key)
      action(async () => {
        await loadBuckets();
        await loadCluster();
      });
  }, [key]);
  useEffect(() => {
    if (key && bucket) action(() => loadObjects());
  }, [bucket]);
  useEffect(() => {
    if (!key) return;
    const timer = setInterval(
      () => loadCluster().catch((e) => setError(e.message)),
      10000,
    );
    return () => clearInterval(timer);
  }, [key]);
  async function search(e) {
    e.preventDefault();
    await action(async () => {
      JSON.parse(tags);
      const p = new URLSearchParams({ prefix, q: text, tags, limit: "100" });
      if (bucket) p.set("bucket", bucket);
      if (minSize) p.set("min_size", minSize);
      if (maxSize) p.set("max_size", maxSize);
      if (after) p.set("created_after", new Date(after).toISOString());
      if (before) p.set("created_before", new Date(before).toISOString());
      const r = await api("/search?" + p);
      setObjects(r.objects);
      setEngine(
        `Elasticsearch · ${r.indexed_total} indexed matches · ${r.pending_updates} pending updates`,
      );
      setNextOffset(null);
      setOffset(0);
    });
  }
  async function createBucket(e) {
    e.preventDefault();
    await action(async () => {
      await api("/buckets/" + encodeURIComponent(newBucket), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: policy }),
      });
      setBucket(newBucket);
      setNewBucket("");
      await loadBuckets();
      setNotice(
        "Bucket created with " +
          (policy === "erasure"
            ? "Reed–Solomon 4+2"
            : "replication N=3, W=2, R=2"),
      );
    });
  }
  async function upload(e) {
    e.preventDefault();
    await action(async () => {
      JSON.parse(uploadTags);
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        await new Promise((resolve, reject) => {
          const xhr = new XMLHttpRequest();
          xhr.open(
            "PUT",
            `/api/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(file.name)}?chunking=${chunking}`,
          );
          xhr.setRequestHeader("X-API-Key", key);
          xhr.setRequestHeader(
            "Content-Type",
            file.type || "application/octet-stream",
          );
          xhr.setRequestHeader(
            "X-Object-Tags",
            JSON.stringify(JSON.parse(uploadTags)).replace(
              /[^\x00-\x7F]/g,
              (c) => "\\u" + c.charCodeAt(0).toString(16).padStart(4, "0"),
            ),
          );
          xhr.upload.onprogress = (event) =>
            setProgress({
              name: file.name,
              file: i + 1,
              total: files.length,
              percent: event.lengthComputable
                ? Math.round((100 * event.loaded) / event.total)
                : 0,
            });
          xhr.onerror = () => reject(new Error("Upload connection failed"));
          xhr.onload = () =>
            xhr.status === 201
              ? resolve()
              : reject(new Error(xhr.responseText));
          xhr.send(file);
        });
      }
      setNotice(
        `${files.length} object(s) stored. Search indexing follows asynchronously.`,
      );
      setFiles([]);
      if (fileInput.current) fileInput.current.value = "";
      setProgress(null);
      await loadObjects();
      await loadCluster();
    });
  }
  async function download(row) {
    await action(async () => {
      const r = await fetch("/api" + pathFor(row), {
        headers: { "X-API-Key": key },
      });
      if (!r.ok) throw new Error(`Download failed: ${r.status}`);
      const url = URL.createObjectURL(await r.blob());
      const link = document.createElement("a");
      link.href = url;
      link.download = row.object_key.split("/").pop();
      link.click();
      setTimeout(() => URL.revokeObjectURL(url), 10000);
      setNotice(
        "Download complete; the gateway verified stored chunks before serving the file.",
      );
    });
  }
  async function remove(row) {
    if (!confirm(`Delete version ${row.version} of ${row.object_key}?`)) return;
    await action(async () => {
      await api(pathFor(row), { method: "DELETE" });
      await loadObjects();
      await loadCluster();
      setNotice(
        "Version deleted. Chunks with remaining references are retained.",
      );
    });
  }
  async function inspect(row) {
    await action(async () =>
      setDetails(
        await api(
          `/buckets/${encodeURIComponent(row.bucket_name)}/metadata/${row.object_key.split("/").map(encodeURIComponent).join("/")}?version=${row.version}`,
        ),
      ),
    );
  }
  async function job(kind) {
    await action(async () => {
      const r = await api("/jobs/" + kind, { method: "POST" });
      setNotice(`${kind} job accepted: ${r.job_id}`);
      await loadCluster();
    });
  }
  if (!key)
    return (
      <div className="login">
        <div className="login-card">
          <div className="brandmark">
            A<span>↗</span>
          </div>
          <p className="eyebrow">DISTRIBUTED OBJECT STORAGE</p>
          <h1>
            A place for
            <br />
            every piece.
          </h1>
          <p>
            Explore your objects, storage nodes and recovery jobs from one
            console.
          </p>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              sessionStorage.setItem("storage-api-key", draftKey);
              setKey(draftKey);
            }}
          >
            <label htmlFor="api-key">Gateway API key</label>
            <input
              id="api-key"
              type="password"
              required
              value={draftKey}
              onChange={(e) => setDraftKey(e.target.value)}
              placeholder="Use API_KEY from your local .env"
            />
            <button>Connect to cluster →</button>
          </form>
          <small>
            Your key stays in this browser tab’s session. All authenticated
            users share the lab cluster.
          </small>
        </div>
        <div className="login-art">
          <div className="orbit one" />
          <div className="orbit two" />
          <div className="orbit three" />
          <div className="art-label">
            One object.
            <br />
            Many resilient pieces.
          </div>
          <span className="art-node n1">01</span>
          <span className="art-node n2">02</span>
          <span className="art-node n3">03</span>
          <span className="art-node n4">04</span>
        </div>
      </div>
    );
  const online =
    cluster?.nodes.filter((n) => n.status === "online" && n.active).length || 0;
  return (
    <div className="shell">
      <aside>
        <a className="brand" href="#">
          A<span>ATLAS</span>
        </a>
        <p className="sidebar-label">WORKSPACE</p>
        <nav>
          {["Objects", "Cluster", "Recovery", "Test guide"].map((label, i) => (
            <button
              key={label}
              className={tab === label ? "selected" : ""}
              onClick={() => {
                setTab(label);
                setDetails(null);
              }}
            >
              <span>{["▧", "◉", "↻", "☷"][i]}</span>
              {label}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <span className="live-dot" /> Local lab cluster
          <small>SHA-256 · consistent hashing</small>
          <button
            className="quiet"
            onClick={() => {
              sessionStorage.removeItem("storage-api-key");
              setKey("");
            }}
          >
            Disconnect
          </button>
        </div>
      </aside>
      <main>
        <header>
          <div>
            <span className="breadcrumb">WORKSPACE / {tab.toUpperCase()}</span>
            <h1>
              {tab === "Objects"
                ? "Your object library"
                : tab === "Cluster"
                  ? "The storage landscape"
                  : tab === "Recovery"
                    ? "Resilience in motion"
                    : "Evidence, one phase at a time"}
            </h1>
          </div>
          <div className="header-actions">
            <span className="pill">
              <span className="live-dot" />
              {online} nodes online
            </span>
            <button
              className="secondary"
              disabled={busy}
              onClick={() =>
                action(async () => {
                  await loadBuckets();
                  await loadObjects();
                  await loadCluster();
                })
              }
            >
              ↻ Refresh
            </button>
          </div>
        </header>
        {error && (
          <div className="alert" role="alert">
            {error}
            <button onClick={() => setError("")}>Dismiss</button>
            {error.startsWith("401") && (
              <button onClick={() => setKey("")}>Change API key</button>
            )}
          </div>
        )}
        {notice && (
          <div className="notice" role="status">
            {notice}
            <button onClick={() => setNotice("")}>×</button>
          </div>
        )}
        <section className="stats">
          <article>
            <span>OBJECT VERSIONS</span>
            <strong>{cluster?.object_versions ?? "—"}</strong>
            <small>Versioned and addressable</small>
          </article>
          <article>
            <span>LOGICAL DATA</span>
            <strong>{size(cluster?.logical_bytes)}</strong>
            <small>{cluster?.unique_chunks ?? 0} unique chunks</small>
          </article>
          <article>
            <span>ACTIVE NODES</span>
            <strong>
              {online}
              <em> / {cluster?.nodes.filter((n) => n.active).length || 0}</em>
            </strong>
            <small>Independent persistent volumes</small>
          </article>
          <article>
            <span>DEGRADED OBJECTS</span>
            <strong className={cluster?.degraded_objects ? "warn" : "green"}>
              {cluster?.degraded_objects ?? "—"}
            </strong>
            <small>{cluster?.repairs_completed ?? 0} completed repairs</small>
          </article>
        </section>
        {tab === "Objects" && (
          <>
            <div className="object-layout">
              <section className="panel">
                <div className="panel-title">
                  <h2>Browse objects</h2>
                  <span className="subtle">Latest version per key</span>
                </div>
                <div className="browse-controls">
                  <label>
                    Bucket
                    <select
                      value={bucket}
                      onChange={(e) => setBucket(e.target.value)}
                    >
                      <option value="">Select a bucket</option>
                      {buckets.map((b) => (
                        <option key={b.bucket_name}>{b.bucket_name}</option>
                      ))}
                    </select>
                  </label>
                  <button
                    className="secondary"
                    onClick={() => action(() => loadObjects())}
                  >
                    List objects
                  </button>
                </div>
                <form className="search-form" onSubmit={search}>
                  <label>
                    Key prefix
                    <input
                      value={prefix}
                      onChange={(e) => setPrefix(e.target.value)}
                      placeholder="reports/"
                    />
                  </label>
                  <label>
                    Search key text
                    <input
                      value={text}
                      onChange={(e) => setText(e.target.value)}
                      placeholder="project"
                    />
                  </label>
                  <details>
                    <summary>Size, dates & tags</summary>
                    <div className="filter-grid">
                      <label>
                        Minimum bytes
                        <input
                          type="number"
                          min="0"
                          value={minSize}
                          onChange={(e) => setMinSize(e.target.value)}
                        />
                      </label>
                      <label>
                        Maximum bytes
                        <input
                          type="number"
                          min="0"
                          value={maxSize}
                          onChange={(e) => setMaxSize(e.target.value)}
                        />
                      </label>
                      <label>
                        Created after
                        <input
                          type="datetime-local"
                          value={after}
                          onChange={(e) => setAfter(e.target.value)}
                        />
                      </label>
                      <label>
                        Created before
                        <input
                          type="datetime-local"
                          value={before}
                          onChange={(e) => setBefore(e.target.value)}
                        />
                      </label>
                      <label className="wide">
                        Tags (JSON)
                        <input
                          value={tags}
                          onChange={(e) => setTags(e.target.value)}
                          placeholder={'{"course":"distributed"}'}
                        />
                      </label>
                    </div>
                  </details>
                  <button disabled={busy}>Search with Elasticsearch ↗</button>
                </form>
                <p className="engine">{engine}</p>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Object key</th>
                        <th>Size</th>
                        <th>Version / created</th>
                        <th>Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {objects.map((row) => (
                        <tr key={row.object_id}>
                          <td>
                            <span className="file-icon">▤</span>
                            <button
                              className="text-button"
                              onClick={() => inspect(row)}
                            >
                              {row.object_key}
                            </button>
                            <small className="tags">
                              {Object.entries(row.tags || {}).map(([k, v]) => (
                                <span key={k}>
                                  {k}: {v}
                                </span>
                              ))}
                            </small>
                          </td>
                          <td>{size(row.size_bytes)}</td>
                          <td>
                            <b>v{row.version}</b>
                            <small>
                              {new Date(row.created_at).toLocaleString()}
                            </small>
                          </td>
                          <td>
                            <button
                              className="icon-button"
                              aria-label={`Download ${row.object_key}`}
                              onClick={() => download(row)}
                            >
                              ↓
                            </button>
                            <button
                              className="icon-button danger"
                              aria-label={`Delete ${row.object_key}`}
                              onClick={() => remove(row)}
                            >
                              ×
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {!objects.length && (
                    <div className="empty">
                      <span>▧</span>
                      <h3>A fresh space for your files</h3>
                      <p>Create a bucket, then upload your first object.</p>
                    </div>
                  )}
                </div>
                <div className="pagination">
                  <button
                    className="secondary"
                    disabled={!offset}
                    onClick={() =>
                      action(() => loadObjects(Math.max(0, offset - 100)))
                    }
                  >
                    Previous
                  </button>
                  <span>Showing {objects.length} objects</span>
                  <button
                    className="secondary"
                    disabled={nextOffset == null}
                    onClick={() => action(() => loadObjects(nextOffset))}
                  >
                    Next
                  </button>
                </div>
              </section>
              <div className="right-stack">
                <section className="panel">
                  <p className="eyebrow">NEW STORAGE SPACE</p>
                  <h2>Create a bucket</h2>
                  <form onSubmit={createBucket}>
                    <label>
                      Bucket name
                      <input
                        required
                        pattern="[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]"
                        value={newBucket}
                        onChange={(e) => setNewBucket(e.target.value)}
                        placeholder="my-course-files"
                      />
                    </label>
                    <label>
                      Protection policy
                      <select
                        value={policy}
                        onChange={(e) => setPolicy(e.target.value)}
                      >
                        <option value="replication">
                          Replication · 3 copies
                        </option>
                        <option value="erasure">Reed–Solomon · 4+2</option>
                      </select>
                    </label>
                    <button disabled={busy}>Create bucket +</button>
                  </form>
                </section>
                <section className="panel upload-panel">
                  <p className="eyebrow">ADD TO YOUR LIBRARY</p>
                  <h2>Upload objects</h2>
                  <form onSubmit={upload}>
                    <label className="dropzone">
                      ↑
                      <strong>
                        {files.length
                          ? `${files.length} file(s) selected`
                          : "Choose your files"}
                      </strong>
                      <small>Streamed to the gateway · up to 2 GiB each</small>
                      <input
                        aria-label="Select files"
                        ref={fileInput}
                        type="file"
                        multiple
                        onChange={(e) => setFiles([...e.target.files])}
                      />
                    </label>
                    <label>
                      Chunking
                      <select
                        value={chunking}
                        onChange={(e) => setChunking(e.target.value)}
                      >
                        <option value="fixed">Fixed · 4 MiB</option>
                        <option value="cdc">
                          Content defined · rolling hash
                        </option>
                      </select>
                    </label>
                    <label>
                      Tags (JSON)
                      <input
                        value={uploadTags}
                        onChange={(e) => setUploadTags(e.target.value)}
                      />
                    </label>
                    <button disabled={busy || !bucket || !files.length}>
                      Upload to {bucket || "a bucket"} →
                    </button>
                    {progress && (
                      <div className="upload-progress">
                        <progress value={progress.percent} max="100" />
                        <small>
                          {progress.file}/{progress.total} · {progress.name} ·{" "}
                          {progress.percent === 100
                            ? "Verifying and storing…"
                            : progress.percent + "%"}
                        </small>
                      </div>
                    )}
                  </form>
                </section>
              </div>
            </div>
            {details && (
              <section className="panel metadata-panel">
                <div className="panel-title">
                  <h2>Object metadata & chunk placement</h2>
                  <button className="quiet" onClick={() => setDetails(null)}>
                    Close ×
                  </button>
                </div>
                <pre>{JSON.stringify(details, null, 2)}</pre>
              </section>
            )}
          </>
        )}
        {tab === "Cluster" && (
          <section className="panel">
            <div className="panel-title">
              <h2>Nodes & chunk distribution</h2>
              <span className="subtle">Refreshes every 10 seconds</span>
            </div>
            <div className="node-grid">
              {cluster?.nodes.map((n) => (
                <article className="node-card" key={n.node_id}>
                  <div>
                    <h3>{n.node_id}</h3>
                    <span className={"status " + n.status}>{n.status}</span>
                  </div>
                  <p>{n.active ? "Ring member" : "Outside placement ring"}</p>
                  <strong>
                    {n.chunk_count} <small>chunks on disk</small>
                  </strong>
                  <div className="distribution">
                    <i
                      style={{
                        width: `${Math.max(2, (100 * n.fragments) / Math.max(1, ...cluster.nodes.map((x) => x.fragments)))}%`,
                      }}
                    />
                  </div>
                  <dl>
                    <dt>Assigned fragments</dt>
                    <dd>{n.fragments}</dd>
                    <dt>Assigned payload</dt>
                    <dd>{size(n.stored_bytes)}</dd>
                    <dt>Filesystem free</dt>
                    <dd>{size(n.free_space_bytes)}</dd>
                    <dt>Last heartbeat</dt>
                    <dd>{n.seconds_since_heartbeat.toFixed(1)} s ago</dd>
                  </dl>
                </article>
              ))}
            </div>
            <p className="footnote">{cluster?.free_space_note}</p>
          </section>
        )}
        {tab === "Recovery" && (
          <>
            <section className="panel">
              <div className="panel-title">
                <h2>Maintenance workers</h2>
                <span className="subtle">
                  Durable RabbitMQ queue · two workers
                </span>
              </div>
              <div className="job-actions">
                {["repair", "scrub", "rebalance", "gc", "search"].map(
                  (kind) => (
                    <button
                      className="secondary"
                      key={kind}
                      disabled={busy}
                      onClick={() => job(kind)}
                    >
                      Run {kind} ↗
                    </button>
                  ),
                )}
              </div>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Job</th>
                      <th>Status</th>
                      <th>Worker</th>
                      <th>Updated</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cluster?.jobs.map((j) => (
                      <tr key={j.job_id}>
                        <td>
                          {j.kind}
                          <small>{j.job_id.slice(0, 8)}</small>
                        </td>
                        <td>
                          <span className={"status " + j.status}>
                            {j.status}
                          </span>
                          {j.error && <small className="warn">{j.error}</small>}
                        </td>
                        <td>{j.worker_id || "Waiting for worker"}</td>
                        <td>{new Date(j.updated_at).toLocaleTimeString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
            <section className="panel event-panel">
              <h2>Failure & integrity events</h2>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Cause / policy</th>
                      <th>Result</th>
                      <th>Repair read</th>
                      <th>Repair write</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cluster?.events.map((e) => (
                      <tr key={e.event_id}>
                        <td>
                          {e.cause}
                          <small>{e.policy_key}</small>
                        </td>
                        <td>
                          {e.outcome}
                          <small>{e.detail}</small>
                        </td>
                        <td>{size(e.read_bytes)}</td>
                        <td>{size(e.written_bytes)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          </>
        )}
        {tab === "Test guide" && (
          <section className="panel guide">
            <p className="eyebrow">REPORT EVIDENCE</p>
            <h2>Capture what you actually observe.</h2>
            <p>
              Use TESTING.md in the repository for commands, expected results
              and screenshot checkpoints. Keep benchmark evidence separate from
              small smoke tests.
            </p>
            <ol>
              <li>
                <b>Storage nodes:</b> PUT, GET, HEAD, invalid upload, isolation,
                persistence, corruption and DELETE.
              </li>
              <li>
                <b>Metadata:</b> fixed and rolling-hash chunking, exact
                reconstruction, ring movement and reference counts.
              </li>
              <li>
                <b>Recovery:</b> quorum rejection, failed-node repair, erasure
                reconstruction and background scrub repair.
              </li>
              <li>
                <b>Gateway:</b> authenticated CRUD, versions, range responses,
                filters and rate limits.
              </li>
              <li>
                <b>Interface:</b> upload, download, Elasticsearch search and
                cluster state.
              </li>
              <li>
                <b>Evaluation:</b> 5,000 small files and 10 large files; compare
                measured storage and repair traffic.
              </li>
              <li>
                <b>Deployment:</b> two gateways, queued jobs, StatefulSet
                volumes and autoscaling evidence.
              </li>
            </ol>
            <a href="/api/docs" target="_blank" rel="noreferrer">
              Open interactive API documentation ↗
            </a>
            <p className="footnote">
              This is an S3-like educational API. It uses an API key and JSON
              responses; AWS SDK/SigV4 compatibility is not claimed.
            </p>
          </section>
        )}
        <footer>
          ATLAS / DISTRIBUTED STORAGE LAB
          <span>Content addressed. Independently verified.</span>
        </footer>
      </main>
    </div>
  );
}
createRoot(document.getElementById("root")).render(<App />);
