/* The browser never calls whoami/work/ACK. Reads are idempotent snapshots. */
(function (global) {
  "use strict";
  const state = { catalog: [], view: null, csrf: null, authenticated: false, selectedCheckpoint: "", cursor: null, generation: 0, busy: false, validation: null, decision: null };
  const unknown = value => value === null || value === undefined || value === "" ? "неизвестно / не сохранено" : typeof value === "object" ? JSON.stringify(value, null, 2) : String(value);
  const uniqueEvents = events => Array.from(new Map((events || []).map(event => [event.event_id, event])).values());
  const proposalText = proposal => JSON.stringify(proposal || {});
  const lines = value => String(value || "").split(/\r?\n/).map(line => line.trim()).filter(Boolean);
  const editable = request => ["pending", "pending_decision", "requested", "approved_pending_application"].includes(request.status);
  const existingAuthorization = request => !!(request.authorization_provenance && request.authorization_provenance.kind === "existing_human_authorization");
  const decisionChoices = request => existingAuthorization(request) ? [["record_existing_authorization", "Применить ранее выданное разрешение"], ["reject", "Отклонить применение"], ["edit", "Изменить границы (новое решение)"]] : [["approve", "Разрешить"], ["reject", "Отклонить"], ["edit", "Изменить границы"]];
  const hasDocument = typeof document !== "undefined";
  const el = id => document.getElementById(id);
  function make(tag, text, className) { const node = document.createElement(tag); if (text !== undefined) node.textContent = unknown(text); if (className) node.className = className; return node; }
  function replace(id, children) { el(id).replaceChildren(...children); }
  function valueNode(value) {
    if (Array.isArray(value)) { if (!value.length) return make("span", "Нет сохранённых записей"); const list = make("ul"); value.forEach(item => { const li = make("li"); li.append(valueNode(item)); list.append(li); }); return list; }
    if (value && typeof value === "object") { const details = make("details"); details.append(make("summary", "Связанные данные (" + Object.keys(value).length + ")"), fields(value)); return details; }
    return make("span", value);
  }
  function fields(value, keys) { const list = make("dl"); for (const key of keys || Object.keys(value || {})) { const item = make("dd"); item.append(valueNode(value && value[key])); list.append(make("dt", key), item); } return list; }
  function badge(text) { return make("span", text, "badge " + String(text || "unknown").replace(/[^a-z_-]/g, "")); }
  function detail(title, data) {
    el("detail-title").textContent = title; el("detail-content").replaceChildren(fields(data));
    if (state.view && (data.event_id || data.result_key)) {
      const links = make("div", undefined, "actions"), metadata = data.metadata || {};
      for (const id of new Set([data.assignment_id, data.source_assignment_id, metadata.assignment_id, metadata.source_assignment_id].filter(Boolean))) {
        const assignment = state.view.assignments.find(item => item.assignment_id === id);
        if (assignment) { const button = make("button", "Assignment в выбранном snapshot: " + id); button.addEventListener("click", () => detail("Assignment · выбранный snapshot (не реконструкция прошлого события)", assignment)); links.append(button); }
      }
      const request = state.view.scope_requests.find(item => item.request_id === (data.request_id || metadata.request_id));
      if (request) { const button = make("button", "Связанный scope request"); button.addEventListener("click", () => { detail("Scope request", request); el("detail-content").append(decisionCard(request)); }); links.append(button); }
      if (data.reviewed_result) { const button = make("button", "Точный reviewed result / evidence"); button.addEventListener("click", () => detail("Сохранённый reviewed result", data.reviewed_result)); links.append(button); }
      if (links.childNodes.length) el("detail-content").append(make("h3", "Связанные записи"), links);
    }
    if (!el("detail").open) el("detail").showModal();
  }
  function detailButton(title, data) { const button = make("button", title, "link"); button.type = "button"; button.addEventListener("click", () => detail(title, data)); return button; }
  function empty(message) { return make("p", message, "empty"); }
  function error(message) { el("error").hidden = !message; el("error").textContent = message || ""; }
  function base() { return "/api/v1/projects/" + encodeURIComponent(el("project").value) + "/sprints/" + encodeURIComponent(el("sprint").value); }
  async function api(path, options = {}) {
    const response = await fetch(path, { credentials: "same-origin", cache: "no-store", ...options, headers: { ...(options.body ? { "Content-Type": "application/json", "X-Nginx-QA-CSRF": state.csrf || "" } : {}), ...(options.headers || {}) } });
    let data; try { data = await response.json(); } catch (_) { throw new Error("Сервер вернул не JSON (HTTP " + response.status + ")"); }
    if (!response.ok) { const cause = data.detail || data.error || data; const problem = new Error("HTTP " + response.status + ": " + unknown(cause.message || cause.error || cause)); problem.status = response.status; throw problem; }
    return data;
  }
  function graph(view) {
    const wrapper = make("div");
    if (!view.topology.known) wrapper.append(empty("Полная топология не записана для этого runtime. Показаны только известные узлы."));
    const nodes = view.topology.nodes || [], ns = "http://www.w3.org/2000/svg", svg = document.createElementNS(ns, "svg");
    const width = 650, rows = Math.ceil(nodes.length / 3), height = Math.max(170, rows * 105 + 20);
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`); svg.setAttribute("role", "group"); svg.setAttribute("aria-label", "Топология спринта, текущие и посещённые узлы");
    const positions = new Map(nodes.map((node, i) => [node.id, { x: 10 + i % 3 * 215, y: 12 + Math.floor(i / 3) * 105 }]));
    for (const edge of view.topology.edges || []) {
      const source = positions.get(edge.source), target = positions.get(edge.target);
      if (!source || !target) continue;
      const line = document.createElementNS(ns, "path"); line.setAttribute("d", `M${source.x + 90},${source.y + 60} L${target.x + 90},${target.y}`); line.setAttribute("fill", "none"); line.setAttribute("stroke", "#b8c6d9"); svg.append(line);
    }
    nodes.forEach(node => {
      const p = positions.get(node.id), group = document.createElementNS(ns, "g"), rect = document.createElementNS(ns, "rect"), label = document.createElementNS(ns, "text"), status = document.createElementNS(ns, "text");
      group.setAttribute("class", "node"); group.setAttribute("role", "button"); group.setAttribute("tabindex", "0"); group.setAttribute("aria-label", node.id + ": " + node.position);
      rect.setAttribute("x", p.x); rect.setAttribute("y", p.y); rect.setAttribute("width", "192"); rect.setAttribute("height", "65"); rect.setAttribute("rx", "7"); rect.setAttribute("fill", node.position === "current" ? "#dceaff" : node.position === "visited" ? "#e6f2ed" : "#f4f6fa"); rect.setAttribute("stroke", "#a9bad1");
      label.setAttribute("x", p.x + 8); label.setAttribute("y", p.y + 23); label.textContent = String(node.id).slice(0, 26);
      status.setAttribute("x", p.x + 8); status.setAttribute("y", p.y + 45); status.textContent = node.position + (node.terminal_definition || node.type === "terminal" ? " · terminal node" : "");
      group.append(rect, label, status);
      const open = () => {
        detail("Узел " + node.id, { ...node, visits: view.visits.filter(visit => visit.node_id === node.id), assignments: view.assignments.filter(item => item.node_id === node.id) });
        const requests = view.pending_decisions.filter(item => (item.proposal && item.proposal.node_ids || []).includes(node.id));
        if (requests.length) { el("detail-content").append(make("h3", "Pending decisions")); for (const request of requests) el("detail-content").append(decisionCard(request)); }
      };
      group.addEventListener("click", open); group.addEventListener("keydown", event => { if (["Enter", " "].includes(event.key)) { event.preventDefault(); open(); } }); svg.append(group);
    });
    wrapper.append(svg);
    for (const edge of view.topology.edges || []) wrapper.append(make("div", unknown(edge.source) + " — " + unknown(edge.outcome) + " → " + unknown(edge.target), "edge"));
    if ((view.topology.reviewer_roles || []).length) { wrapper.append(make("h3", "Роли reviewer gate")); for (const role of view.topology.reviewer_roles) wrapper.append(detailButton(unknown(role.name || role.id), role)); }
    return wrapper;
  }
  function assignmentCard(item) {
    const card = make("article", undefined, "card " + (item.is_current ? "current" : ""));
    card.append(detailButton(unknown(item.node_id || item.assignment_id), item), badge(item.status), fields(item, ["assignment_id", "agent_id", "agent_phone", "phase", "visit", "git_branch", "delivery_status"]));
    const scope = item.scope || {};
    card.append(make("p", "Effective scope revision: " + unknown(scope.effective_revision) + " · ACK: " + unknown(scope.ack_status)));
    if (scope.known) card.append(detailButton("Effective scope / точный ACK", scope));
    return card;
  }
  function decisionCard(request) {
    const card = make("article", undefined, "card"); card.dataset.requestId = request.request_id;
    card.append(detailButton("Scope request " + request.request_id, request), badge(request.status));
    card.append(make("p", request.reason), make("p", (request.proposal || {}).instructions, "scope-text"));
    card.append(make("strong", "Сохраняемые ограничения")); const ul = make("ul"); for (const restriction of (request.proposal || {}).retained_restrictions || []) ul.append(make("li", restriction)); card.append(ul);
    card.append(fields(request, ["assignment_id", "base_scope_revision", "dependencies"]));
    if (existingAuthorization(request)) {
      card.append(make("p", request.status === "approved_pending_application" ? "Решение уже принято — ожидает инфраструктурного применения. Новое согласие на эти границы не требуется." : "Сохранено ранее выданное разрешение; его provenance не разрешает другие границы."), fields(request, ["authorization_provenance"]));
    }
    if (request.decision) card.append(detailButton("Решение и provenance", request.decision));
    if (editable(request)) {
      const actions = make("div", undefined, "actions");
      for (const [action, label] of decisionChoices(request)) {
        const button = make("button", label); button.disabled = !state.authenticated || !!state.selectedCheckpoint;
        button.addEventListener("click", () => openDecision(request, action)); actions.append(button);
      }
      card.append(actions);
    }
    return card;
  }
  function render(view) {
    replace("execution", [fields(view.execution)]);
    replace("attention", [badge(view.attention.state), fields(view.attention, ["pending_request_ids", "pending_application_ids", "pending_ack_assignment_ids", "other_gates"]), make("p", "ACK снимает только scope-блокировку. Он не означает transition, completion, dequeue или resume.", "hint")]);
    replace("graph", [graph(view)]);
    replace("visits", view.visits.length ? view.visits.map(visit => { const card = make("article", undefined, "card"); card.append(detailButton(unknown(visit.node_id) + " · visit " + unknown(visit.visit || visit.generation || visit.occurrence_id), visit), badge(visit.state), fields(visit, ["assignment_ids"])); return card; }) : [empty("Visits не записаны")]);
    const current = view.assignments.filter(item => item.is_current); replace("current", current.length ? current.map(assignmentCard) : [empty("Текущее assignment не сохранено / отсутствует. Это не вывод о terminal state.")]);
    replace("decisions", view.pending_decisions.length ? view.pending_decisions.map(decisionCard) : [empty("Нет сохранённых ожидающих решений")]);
    replace("reviews", view.review_gates.length ? view.review_gates.map(gate => { const card = make("article", undefined, "card"); card.append(detailButton("Result " + unknown(gate.result_key), gate), fields(gate, ["source_assignment_id", "result_commit", "required_approvals", "applicable_approvals", "status"])); for (const review of gate.reviews) card.append(detailButton(unknown(review.reviewer_id || review.reviewer_agent_id) + ": " + unknown(review.decision), review)); return card; }) : [empty("Сохранённые review gates отсутствуют")]);
    const queue = [view.queue.known ? detailButton("Снимок очередей (чтение без извлечения)", view.queue) : empty("Очередь неизвестна для этого снимка")];
    queue.push(...view.assignments.filter(item => !item.is_current).map(assignmentCard)); replace("queue", queue);
    replace("timeline", uniqueEvents(view.timeline).reverse().map(event => { const row = make("article", undefined, "timeline-event"); row.append(detailButton(event.kind, event), make("small", unknown(event.timestamp) + " · execution revision: " + unknown(event.execution_revision) + (event.observed_at_execution_revision !== undefined ? " · впервые сохранено при revision " + unknown(event.observed_at_execution_revision) : "")), make("span", event.assignment_id || event.node_id || event.event_id)); return row; }));
    const selected = state.selectedCheckpoint; const options = [new Option("Live — текущее выполнение", "")];
    for (const point of view.history.available || []) options.push(new Option("Revision " + unknown(point.execution_revision) + " · " + unknown(point.recorded_at), point.checkpoint_id));
    el("checkpoint").replaceChildren(...options); el("checkpoint").value = selected;
    el("sync").className = selected ? "historical" : "";
    el("sync").textContent = (selected ? "Исторический снимок" : "Синхронизировано") + " · revision " + unknown(view.execution.revision) + " · " + new Date().toLocaleTimeString();
  }
  async function refresh() {
    if (state.busy || !el("project").value || !el("sprint").value) return;
    const generation = state.generation; state.busy = true;
    try {
      const previousAuth = state.authenticated;
      await refreshSession();
      const params = new URLSearchParams(); if (state.selectedCheckpoint) params.set("at_checkpoint", state.selectedCheckpoint); if (state.cursor) params.set("cursor", state.cursor);
      const view = await api(base() + "/observability?" + params.toString());
      if (generation !== state.generation) return;
      if (state.validation && (view.execution.revision !== state.validation.execution_revision || scopeRevision(view) !== state.validation.scope_revision)) invalidateValidation("Состояние изменилось. Требуются новая validation и exact diff.");
      const rerender = !view.unchanged || !state.view || previousAuth !== state.authenticated;
      state.view = view; state.cursor = view.cursor; error(null);
      if (rerender) render(view);
      else el("sync").textContent = (state.selectedCheckpoint ? "Исторический снимок" : "Синхронизировано") + " · revision " + unknown(view.execution.revision) + " · " + new Date().toLocaleTimeString();
    } catch (err) { if (generation === state.generation) { el("sync").textContent = "Связь потеряна — повторное подключение автоматически"; error(err.message); } }
    finally { state.busy = false; }
  }
  function scopeRevision(view) { return view.scope_revision !== undefined ? view.scope_revision : Math.max(0, ...view.assignments.map(item => Number((item.scope || {}).effective_revision) || 0)); }
  function invalidateValidation(reason) { state.validation = null; el("confirm").disabled = true; if (reason) el("validation").textContent = reason; }
  function selectedProposal() { return { ...state.decision.request.proposal, instructions: el("instructions").value, retained_restrictions: lines(el("restrictions").value) }; }
  function openDecision(request, action) {
    if (el("detail").open) el("detail").close();
    state.decision = { request, action, idempotency_key: global.crypto.randomUUID(), base: base() }; invalidateValidation();
    el("instructions").value = request.proposal.instructions; el("restrictions").value = (request.proposal.retained_restrictions || []).join("\n"); el("decision-reason").value = "";
    el("instructions").readOnly = action !== "edit"; el("restrictions").readOnly = action !== "edit";
    el("decision-summary").replaceChildren(fields(request, ["request_id", "assignment_id", "reason", "dependencies", "source", "authorization_provenance"]));
    el("validate").hidden = action === "reject"; el("confirm").disabled = action !== "reject"; el("confirm").textContent = action === "reject" ? "Подтвердить отказ" : "Подтвердить проверенное решение";
    el("validation").textContent = action === "reject" ? "Отказ сохранится в истории; scope и прежний ACK не изменятся." : "Сначала проверьте точный resulting diff.";
    if (action === "edit" && existingAuthorization(request)) el("validation").textContent = "Это новое явное semantic decision для изменённых границ. Прежнее разрешение не наследуется. Сначала проверьте exact resulting diff.";
    el("decision-dialog").showModal();
  }
  async function validateDecision() {
    invalidateValidation(); el("validate").disabled = true;
    try {
      const proposal = selectedProposal(), request = state.decision.request;
      const validated = await api(state.decision.base + "/scope-requests/" + encodeURIComponent(request.request_id) + "/validate", { method: "POST", body: JSON.stringify({ proposal, expected_execution_revision: state.view.execution.revision, expected_scope_revision: scopeRevision(state.view) }) });
      state.validation = { ...validated, proposal_fingerprint: proposalText(proposal) };
      const section = make("div"); section.append(make("h3", "Exact resulting scope diff"), fields(validated, ["execution_revision", "scope_revision"]));
      const diff = make("div", undefined, "diff"); const raw = validated.diff || {};
      for (const [label, content] of [["До (current effective)", validated.before_effective || raw.before || raw.current || raw.previous], ["После (validated result)", validated.after_effective || raw.after || raw.proposed || validated.proposal]]) { const pane = make("div"); pane.append(make("h4", label), valueNode(content)); diff.append(pane); }
      section.append(diff, make("pre", validated.diff)); el("validation").replaceChildren(section); el("confirm").disabled = false;
    } catch (err) { invalidateValidation(err.message); }
    finally { el("validate").disabled = false; }
  }
  async function submitDecision(event) {
    event.preventDefault(); const decision = state.decision; if (!decision || !state.authenticated || state.selectedCheckpoint) return;
    if (decision.action !== "reject" && (!state.validation || state.validation.proposal_fingerprint !== proposalText(selectedProposal()))) { invalidateValidation("Изменённое содержание требует новой проверки."); return; }
    el("confirm").disabled = true;
    const body = { action: decision.action, reason: el("decision-reason").value, idempotency_key: decision.idempotency_key, expected_execution_revision: state.validation ? state.validation.execution_revision : state.view.execution.revision, expected_scope_revision: state.validation ? state.validation.scope_revision : scopeRevision(state.view) };
    if (state.validation) body.validation_id = state.validation.validation_id;
    try { await api(decision.base + "/scope-requests/" + encodeURIComponent(decision.request.request_id) + "/decisions", { method: "POST", body: JSON.stringify(body) }); el("decision-dialog").close(); invalidateValidation(); await refresh(); }
    catch (err) { if (err.status === 409) { invalidateValidation("Конфликт/stale: другое решение или новая revision. Ничего не применено этой попыткой. " + err.message); await refresh(); } else { el("validation").textContent = err.message + " Повтор использует тот же idempotency key."; el("confirm").disabled = false; } }
  }
  async function chooseProject(initialSprint) {
    state.generation++; state.cursor = null; state.selectedCheckpoint = "";
    el("sync").textContent = "Загрузка выбранного проекта…";
    const project = state.catalog.find(item => String(item.project_id) === el("project").value), sprints = project && project.sprints || [];
    el("sprint").replaceChildren(...sprints.map(item => new Option(item.title || item.sprint_id, item.sprint_id)));
    if (initialSprint && sprints.some(item => item.sprint_id === initialSprint)) el("sprint").value = initialSprint;
    if (!sprints.length) { error("В выбранном проекте нет сохранённых sprint snapshots."); return; } await refresh();
  }
  async function refreshSession() {
    try { const session = await api("/api/v1/operator/session"); state.csrf = session.csrf_token || session.csrf; state.authenticated = session.authenticated === true && !!state.csrf;
      el("auth").textContent = state.authenticated ? "Операторская сессия активна. Scope decisions не заменяют reviews и не продвигают граф." : "Режим чтения. Подтвердите эту браузерную сессию штатной локальной операторской обёрткой. Код привязки: " + unknown(session.pairing_code) + ". Это не bearer credential; токены остаются в защищённом хранилище.";
    } catch (_) { state.authenticated = false; state.csrf = null; el("auth").textContent = "Операторская сессия недоступна. Чтение остаётся доступным; решения запрещены."; }
  }
  async function start() {
    el("project").addEventListener("change", () => chooseProject());
    el("sprint").addEventListener("change", () => { state.generation++; state.cursor = null; state.selectedCheckpoint = ""; el("sync").textContent = "Загрузка выбранного спринта…"; invalidateValidation(); refresh(); });
    el("checkpoint").addEventListener("change", () => { state.generation++; state.selectedCheckpoint = el("checkpoint").value; state.cursor = null; el("sync").textContent = "Загрузка выбранного момента…"; invalidateValidation(); refresh(); });
    el("validate").addEventListener("click", validateDecision); el("decision-form").addEventListener("submit", submitDecision);
    for (const id of ["instructions", "restrictions"]) el(id).addEventListener("input", () => invalidateValidation("Содержание изменено — повторите validation."));
    el("cancel").addEventListener("click", () => el("decision-dialog").close());
    await refreshSession();
    const query = new URLSearchParams(global.location.search);
    try { const catalog = await api("/api/v1/execution-catalog"); state.catalog = catalog.projects || []; }
    catch (err) {
      if (!query.get("project_id") || !query.get("sprint_id")) { error(err.message); return; }
      state.catalog = [{ project_id: query.get("project_id"), name: query.get("project_id"), sprints: [{ sprint_id: query.get("sprint_id") }] }];
    }
    el("project").replaceChildren(...state.catalog.map(item => new Option(item.name || item.project_id, item.project_id)));
    if (query.get("project_id") && state.catalog.some(item => String(item.project_id) === query.get("project_id"))) el("project").value = query.get("project_id");
    await chooseProject(query.get("sprint_id")); global.setInterval(refresh, 2500);
  }
  // Pure helpers are exported for isolated browser-contract tests (no DOM/API).
  if (typeof module !== "undefined" && module.exports) module.exports = { uniqueEvents, proposalText, lines, editable, decisionChoices, unknown };
  if (hasDocument) start();
})(typeof window !== "undefined" ? window : globalThis);
