// 共享前端工具
async function api(path, options = {}) {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const err = new Error(data.error || `HTTP ${resp.status}`);
    err.detail = data.detail;
    throw err;
  }
  return data;
}

function esc(v) {
  if (v === null || v === undefined) return '<span class="muted">∅</span>';
  return String(v)
    .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function badge(status) {
  return `<span class="badge ${esc(status)}">${esc(status)}</span>`;
}

function pretty(obj) {
  return `<pre class="json">${esc(JSON.stringify(obj, null, 2))}</pre>`;
}

function toast(msg, ok = true) {
  let box = document.querySelector(".toast");
  if (!box) {
    box = document.createElement("div");
    box.className = "toast";
    document.body.appendChild(box);
  }
  const el = document.createElement("div");
  el.className = `t ${ok ? "ok" : "err"}`;
  el.textContent = msg;
  box.appendChild(el);
  setTimeout(() => el.remove(), 4200);
}

function markActiveNav() {
  const page = location.pathname.split("/").pop() || "index.html";
  document.querySelectorAll("nav a").forEach((a) => {
    if (a.getAttribute("href") === page) a.classList.add("active");
  });
}

function headerHtml(title) {
  return `
  <header>
    <h1>🔁 ${esc(title)}</h1>
    <nav>
      <a href="index.html">总览</a>
      <a href="receiver.html">接收服务 · 8071</a>
      <a href="pairing.html">配对服务 · 8072</a>
      <a href="target.html">目标仓 · 8073</a>
    </nav>
    <span class="spacer"></span>
    <span id="health" class="badge pulse">检查中…</span>
  </header>`;
}

async function checkHealth(path) {
  const el = document.getElementById("health");
  try {
    const h = await api(path);
    el.textContent = "● 在线 " + (h.checkpoint !== undefined ? `消费位置=${h.checkpoint}` : "");
    el.classList.remove("pulse");
    el.style.borderColor = "var(--green)";
    el.style.color = "var(--green)";
  } catch (e) {
    el.textContent = "● 离线";
    el.classList.remove("pulse");
    el.style.borderColor = "var(--red)";
    el.style.color = "var(--red)";
  }
}
