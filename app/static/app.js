// Fetches /status every 3 seconds and redraws. Every value goes in with textContent, never
// innerHTML, because tenant and job names come from API clients and must not be treated as HTML.

const STATES = ["ready", "retry_wait", "leased", "succeeded", "dead"];

function cell(row, text, className) {
  const td = document.createElement("td");
  td.textContent = text;
  if (className) td.className = className;
  row.appendChild(td);
}

function fillTable(id, rows, emptyText, makeRow) {
  const body = document.querySelector("#" + id + " tbody");
  body.replaceChildren();
  if (rows.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 8;
    td.className = "empty";
    td.textContent = emptyText;
    tr.appendChild(td);
    body.appendChild(tr);
    return;
  }
  for (const item of rows) {
    const tr = document.createElement("tr");
    makeRow(tr, item);
    body.appendChild(tr);
  }
}

function time(iso) {
  return iso ? new Date(iso).toLocaleTimeString() : "no";
}

function render(data) {
  const counts = document.getElementById("counts");
  counts.replaceChildren();
  for (const state of STATES) {
    const group = document.createElement("div");
    const label = document.createElement("dt");
    const value = document.createElement("dd");
    label.textContent = state;
    value.textContent = data.run_counts[state] || 0;
    value.className = "state-" + state;
    group.append(label, value);
    counts.appendChild(group);
  }

  fillTable("tenants", data.tenants, "no tenants yet", (tr, t) => {
    cell(tr, t.name);
    cell(tr, t.waiting, "n");
    cell(tr, t.leased + " / " + t.max_concurrency, "n");
    cell(tr, t.succeeded, "n");
    cell(tr, t.dead, t.dead > 0 ? "n bad" : "n");
    cell(tr, t.burst_rps, "n");
    cell(tr, t.points_quota_per_hour, "n");
  });

  fillTable("runs", data.recent_runs, "no runs yet", (tr, r) => {
    cell(tr, time(r.created_at));
    cell(tr, r.tenant);
    cell(tr, r.job);
    cell(tr, r.job_type);
    cell(tr, r.state, "state-" + r.state);
    cell(tr, r.attempt, "n");
    cell(tr, r.lease_owner || "", "mono");
  });

  fillTable("dead", data.dead_letters, "none", (tr, d) => {
    cell(tr, time(d.moved_at));
    cell(tr, d.tenant);
    cell(tr, d.job);
    cell(tr, d.reason, "reason");
    cell(tr, d.redriven_at ? "yes, " + time(d.redriven_at) : "no");
  });

  document.getElementById("meta").textContent = "updated " + time(data.as_of);
}

async function load() {
  try {
    const response = await fetch("/status");
    if (!response.ok) throw new Error("HTTP " + response.status);
    render(await response.json());
  } catch (error) {
    document.getElementById("meta").textContent = "cannot reach the API: " + error.message;
  }
}

load();
setInterval(() => {
  if (!document.getElementById("pause").checked) load();
}, 3000);
