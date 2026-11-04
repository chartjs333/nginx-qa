/* Setup and Diagnostics Wizard v1 — Client UX.
 * Untrusted strings are rendered using textContent.
 * Strict Plan & Apply flow with explicit confirmation dialogs.
 */
(function (global) {
  "use strict";

  const state = {
    csrf: null,
    authenticated: false,
    actor: null,
    attempt: null,
    attemptOperationId: null,
    storageBlocked: false,
    inspecting: false,
    planning: false,
    retryPermitted: true,
    currentProject: null,
    currentSnapshot: null,
    busy: false,
    pendingPlan: null,
    planReturnFocus: null,
    applyKey: null,
    applying: false,
    advising: false,
    adviceRequest: null,
    sessionGeneration: 0,
    epoch: 0,
    receipts: [],
  };

  const CHECK_LABELS = {
    "project.slot": "Слот проекта и непрерывность",
    "git.repository": "Репозиторий Git и происхождение",
    "git.worktree": "Рабочее дерево Git и чистота",
    "secrets.provider_access": "Секреты: доступ к провайдеру",
    "secrets.telegram": "Секреты: Telegram бот",
    "worker.configuration": "Конфигурация исполнителя",
    "worker.connection": "Сетевое соединение исполнителя",
    "worker.capabilities": "Совместимость и возможности исполнителя",
    "telegram.configuration": "Конфигурация Telegram Launcher",
    "telegram.connectivity": "Сетевое соединение с Telegram API",
    "telegram.webhook": "Tunnel и Webhook для Telegram",
    "engine.durable_state": "База состояния Managed Sprint Engine",
    "engine.leases": "Leases веток и координация",
    "engine.supervisor": "Supervisor и мониторинг процессов",
    "engine.continuity": "Continuity и восстановление после сбоев",
    "selftest.execution": "Проверочное задание в тестовом проекте",
  };

  function el(id) {
    return document.getElementById(id);
  }

  function make(tag, text, className) {
    const node = document.createElement(tag);
    if (text !== undefined && text !== null) {
      node.textContent = String(text);
    }
    if (className) {
      node.className = className;
    }
    return node;
  }

  function showNotice(text) {
    el("auth").textContent = text;
  }

  function showError(text) {
    const err = el("error");
    if (text) {
      err.textContent = text;
      err.hidden = false;
    } else {
      err.textContent = "";
      err.hidden = true;
    }
  }

  const ATTEMPT_STORAGE_KEY = "nginx-qa.setup.pending-apply.v1";
  const opaque = value => typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(value);
  const digest = value => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
  const projectId = value => typeof value === "string" && /^[0-9]{4}$/.test(value);

  function validAttempt(value) {
    return value && typeof value === "object" && !Array.isArray(value) &&
      Object.keys(value).sort().join(",") === "actor_id,idempotency_key,plan_digest,plan_id,project_id,schema_version" &&
      value.schema_version === 1 && opaque(value.actor_id) && opaque(value.plan_id) &&
      projectId(value.project_id) && digest(value.plan_digest) &&
      typeof value.idempotency_key === "string" && /^ui-apply-[a-f0-9-]{36}$/.test(value.idempotency_key);
  }

  function restoreAttempt() {
    try {
      const raw = global.sessionStorage.getItem(ATTEMPT_STORAGE_KEY);
      if (raw === null) return;
      if (raw.length > 2048) throw new Error("Stored request is too large");
      const attempt = JSON.parse(raw);
      if (!validAttempt(attempt)) throw new Error("Invalid stored request");
      state.attempt = attempt;
    } catch (_) { state.storageBlocked = true; }
  }

  function persistAttempt(plan, key) {
    const attempt = {schema_version: 1, actor_id: plan.actor_id, project_id: plan.project_id,
      plan_id: plan.plan_id, plan_digest: plan.plan_digest, idempotency_key: key};
    if (!validAttempt(attempt)) throw new Error("Некорректный контекст исходного запроса");
    const raw = JSON.stringify(attempt);
    global.sessionStorage.setItem(ATTEMPT_STORAGE_KEY, raw);
    if (global.sessionStorage.getItem(ATTEMPT_STORAGE_KEY) !== raw) throw new Error("Ключ запроса не сохранён");
    state.attempt = attempt;
  }

  function clearAttempt() {
    try {
      global.sessionStorage.removeItem(ATTEMPT_STORAGE_KEY);
      if (global.sessionStorage.getItem(ATTEMPT_STORAGE_KEY) !== null) throw new Error("Stored request remains");
      state.attempt = null;
      state.attemptOperationId = null;
      state.storageBlocked = false;
      return true;
    } catch (_) {
      state.storageBlocked = true;
      return false;
    }
  }

  function renderRecovery() {
    const attempt = state.attempt;
    el("apply-recovery").hidden = !attempt && !state.storageBlocked;
    let message = "";
    if (state.storageBlocked) message = "Сохранённый контекст запроса недоступен. Новые операции заблокированы; диагностика и вход доступны. Проверьте сохранённую историю с оператором, не удаляйте неизвестный запрос для повторного запуска.";
    else if (attempt) message = "Сохранён исходный запрос для проекта " + attempt.project_id + ". Пока его результат не подтверждён, новые операции заблокированы. Закрытие окна не отменяет отправленный запрос." +
      (state.actor !== attempt.actor_id ? " Подтвердите вход тем же оператором, который отправил запрос." : "");
    el("apply-recovery-message").textContent = message;
    el("apply-inspect").disabled = !attempt || state.storageBlocked || !state.authenticated ||
      state.actor !== attempt.actor_id || state.applying || state.inspecting;
  }

  function syncControls() {
    const ready = state.authenticated && !!state.currentSnapshot;
    const confirmation = !!state.pendingPlan && !state.attempt;
    el("refresh").disabled = !state.authenticated || !state.currentProject || state.busy || state.applying || confirmation;
    el("project").disabled = !state.authenticated || state.busy || state.applying || confirmation;
    el("advisor-submit").disabled = !ready || state.advising;
    el("session-check").disabled = state.applying || confirmation;
    document.querySelectorAll(".btn-chip").forEach(n => { n.disabled = !ready || state.advising; });
    for (const selector of [".btn-op", ".btn-action"]) document.querySelectorAll(selector).forEach(n => {
      n.disabled = !ready || state.planning || !!state.pendingPlan || !!state.attempt || state.storageBlocked;
    });
    el("plan-apply-btn").disabled = state.applying || !state.pendingPlan || !state.authenticated ||
      state.pendingPlan.actor_id !== state.actor || !state.retryPermitted || state.storageBlocked;
    el("plan-cancel-btn").textContent = state.attempt ? "Закрыть, сохранив запрос" : "Отмена";
    renderRecovery();
  }
  function invalidateSnapshot() {
    state.currentSnapshot = null;
    state.adviceRequest = null;
    state.epoch++;
    el("advisor-result").hidden = true;
    el("checks-list").replaceChildren();
    el("operations-toolbar").replaceChildren();
    state.receipts = [];
    el("receipts-list").replaceChildren();
    el("snapshot-meta").textContent = "";
    syncControls();
  }

  async function api(path, options) {
    const opts = options || {};
    const headers = { ...(opts.headers || {}) };

    if (!headers["Content-Type"] && opts.body) {
      headers["Content-Type"] = "application/json";
    }

    if (state.csrf) {
      headers["X-Nginx-QA-CSRF"] = state.csrf;
    }

    opts.headers = headers;
    opts.credentials = "same-origin";
    opts.cache = "no-store";

    const response = await fetch(path, opts);
    let data = null;
    try {
      data = await response.json();
    } catch (_) {
      // Non-JSON response
    }

    if (!response.ok) {
      const errMsg = (data && (data.message || data.error || (data.detail && (data.detail.message || data.detail.error)))) ||
        ("HTTP error " + response.status);
      const err = new Error(errMsg);
      err.status = response.status;
      err.data = data;
      throw err;
    }

    return data;
  }

  async function checkSession() {
    if (state.applying || (state.pendingPlan && !state.attempt)) return;
    const generation = ++state.sessionGeneration;
    const preferredProject = state.currentProject;
    const returnFocus = closePlan({restoreFocus: false});
    state.authenticated = false;
    state.actor = null;
    state.csrf = null;
    state.currentProject = null;
    el("project").replaceChildren();
    // Older requests may still finish, but cannot keep the new session busy or
    // commit their success, failure or finally handlers into its UI context.
    state.busy = state.planning = state.advising = state.inspecting = false;
    invalidateSnapshot();
    showError(null);
    try {
      const session = await api("/api/v1/operator/session", {method: "GET"});
      if (generation !== state.sessionGeneration) return;
      if (session.authenticated === true && session.csrf_token) {
        if (!opaque(session.actor_id)) throw new Error("Контекст оператора не подтверждён");
        state.authenticated = true;
        state.actor = session.actor_id;
        state.csrf = session.csrf_token;
        showNotice("Операторская сессия подтверждена. Изменения защищены CSRF.");
        el("pairing").hidden = true;
        await loadProjects(generation, preferredProject);
        if (generation === state.sessionGeneration && state.currentProject) await refreshSnapshot(generation);
      } else {
        showNotice("Подтвердите сессию через локальный операторский инструмент.");
        el("pairing-code").textContent = session.pairing_code || "Код недоступен";
        el("pairing").hidden = false;
        el("sync").textContent = "Ожидается подтверждение";
      }
    } catch (err) {
      if (generation !== state.sessionGeneration) return;
      state.authenticated = false;
      state.actor = state.csrf = state.currentProject = null;
      el("project").replaceChildren();
      invalidateSnapshot();
      showNotice("Доступ не подтверждён");
      showError(err.message);
    } finally {
      if (generation === state.sessionGeneration) { syncControls(); restorePlanFocus(returnFocus); }
    }
  }

  async function loadProjects(generation, preferredProject) {
    const data = await api("/api/v1/setup/projects");
    if (generation !== state.sessionGeneration) return;
    const select = el("project");
    select.replaceChildren();
    const projects = data.projects || [];
    projects.forEach(p => {
      const opt = make("option", p.project_id + " · " + (p.name || ""));
      opt.value = String(p.project_id); select.append(opt);
    });
    if (projects.some(p => String(p.project_id) === preferredProject)) {
      select.value = preferredProject;
    }
    state.currentProject = select.value || null;
    if (!state.currentProject) showError("Нет доступных проектов. Проверьте реестр и права.");
  }

  async function refreshSnapshot(generation = state.sessionGeneration) {
    if (generation !== state.sessionGeneration || state.busy || state.applying || !state.authenticated || !state.currentProject || (state.pendingPlan && !state.attempt)) return;
    state.busy = true; invalidateSnapshot();
    const epoch = state.epoch;
    const project = state.currentProject;
    const current = () => generation === state.sessionGeneration && epoch === state.epoch && project === state.currentProject;
    el("sync").textContent = "Загрузка диагностики…";
    showError(null);
    try {
      const snapshot = await api("/api/v1/projects/" + encodeURIComponent(project) + "/setup/snapshot");
      if (!current()) return;
      state.currentSnapshot = snapshot;
      renderSnapshot(snapshot);
      renderOperations(snapshot);
      el("sync").textContent = "Диагностика обновлена";
    } catch (err) {
      if (!current()) return;
      showError("Диагностика недоступна: " + err.message);
      el("sync").textContent = "Данные не подтверждены";
    } finally {
      if (current()) {
        await loadRecordedOperations(project, epoch, generation);
      }
      if (current()) { state.busy = false; syncControls(); }
    }
  }

  async function loadRecordedOperations(project, epoch, generation) {
    try {
      const history = await api("/api/v1/projects/" + encodeURIComponent(project) + "/setup/operations");
      if (generation !== state.sessionGeneration || epoch !== state.epoch || project !== state.currentProject) return;
      state.receipts = [];
      el("receipts-list").replaceChildren();
      const operations = (history.operations || []).slice(0, 20);
      operations.reverse().forEach(receipt => recordReceipt(receipt, project, true));
      if (!operations.length) el("receipts-list").append(make("p", "У этого оператора нет сохранённых операций в проекте.", "hint"));
    } catch (err) {
      if (generation === state.sessionGeneration && epoch === state.epoch && project === state.currentProject) {
        el("receipts-list").replaceChildren(make("p", "История операций недоступна: " + err.message, "hint"));
      }
    }
  }

  function renderOperations(snapshot) {
    const actions = {
      "diagnostics.refresh": ["Обновить проверки", []],
      "worker.probe": ["Опросить исполнителя", ["candidate_id"]],
      "worker.bind": ["Привязать исполнителя", ["candidate_id", "probe_receipt_id"]],
      "provider.probe": ["Проверить провайдера", ["provider_id"]],
      "telegram.probe": ["Проверить Telegram", []],
      "setup.open": ["Открыть инструкцию", ["topic"]],
      "selftest.start": ["Запустить самопроверку", ["test_profile_id"]],
      "selftest.stop": ["Остановить самопроверку", ["self_test_id"]],
      "recovery.open": ["Показать восстановление", ["instance_id"]],
      "git.probe": ["Проверить Git", ["repository_id"]],
    };
    const labels = {
      candidate_id: "ID настроенного исполнителя", probe_receipt_id: "ID успешной проверки",
      provider_id: "ID провайдера", topic: "Раздел инструкции",
      test_profile_id: "ID разрешённого тестового профиля", self_test_id: "ID самопроверки",
      instance_id: "ID экземпляра", repository_id: "ID репозитория",
    };
    const toolbar = el("operations-toolbar"); toolbar.replaceChildren();
    (snapshot.available_action_ids || []).filter(a => actions[a]).forEach(action => {
      const [label, fields] = actions[action];
      const form = make("form", undefined, "operation-form");
      const inputs = {};
      form.append(make("strong", label));
      fields.forEach(field => {
        const labelNode = make("label", labels[field]);
        const input = make(field === "topic" ? "select" : "input");
        if (field === "topic") {
          ["worker", "provider", "git", "telegram", "tunnel", "authorization"].forEach(topic => {
            const opt = make("option", topic); opt.value = topic; input.append(opt);
          });
        } else {
          input.type = "text"; input.required = true; input.maxLength = 128;
          input.pattern = "[A-Za-z0-9][A-Za-z0-9._-]{0,127}"; input.autocomplete = "off";
        }
        input.name = field; inputs[field] = input;
        labelNode.append(input); form.append(labelNode);
      });
      const btn = make("button", "Сформировать план", "btn-op"); btn.type = "submit";
      form.append(btn);
      form.addEventListener("submit", event => {
        event.preventDefault();
        const args = {};
        fields.forEach(field => { args[field] = inputs[field].value.trim(); });
        createPlan(action, args, btn);
      });
      toolbar.append(form);
    });
    if (!toolbar.childNodes.length) toolbar.append(make("p", "Разрешённых операций нет.", "hint"));
  }

  function renderSnapshot(snapshot) {
    el("snapshot-meta").textContent = "ID: " + snapshot.snapshot_id.substring(0, 12) + "… | Сгенерирован: " + new Date(snapshot.generated_at).toLocaleTimeString();

    const list = el("checks-list");
    list.replaceChildren();

    const checks = snapshot.checks || [];
    if (!checks.length) {
      list.append(make("p", "Нет доступных проверок.", "hint"));
      return;
    }

    checks.forEach(check => {
      const card = make("div", undefined, "check-item");

      const header = make("div", undefined, "check-header");
      const titleSpan = make("span", CHECK_LABELS[check.check_id] || check.check_id, "check-id");
      const statusBadge = make("span", check.status, "badge " + check.status);
      header.append(titleSpan, statusBadge);

      const details = make("div", undefined, "check-details");
      const freshnessBadge = make("span", check.freshness, "badge " + check.freshness);
      const observedText = check.observed_at ? ("Зафиксировано: " + new Date(check.observed_at).toLocaleTimeString()) : "Не опрашивалось";
      details.append(freshnessBadge, make("span", " · " + observedText));

      if (check.error_code) {
        const errDiv = make("div", "Код ошибки: " + check.error_code, "hint");
        details.append(errDiv);
      }

      card.append(header, details);

      if (check.evidence && check.evidence.length) {
        const evBox = make("div", JSON.stringify(check.evidence, null, 2), "check-evidence");
        card.append(evBox);
      }

      list.append(card);
    });
  }

  async function requestAdvice(question) {
    if (!state.currentSnapshot || state.advising) {
      showError("Сначала необходимо загрузить диагностический снимок");
      return;
    }

    const q = (question || el("advisor-question").value || "").trim();
    if (!q) {
      showError("Пожалуйста, введите вопрос для LLM-помощника");
      return;
    }
    if (new TextEncoder().encode(q).length > 4096) {
      showError("Вопрос превышает 4 КиБ");
      return;
    }

    const generation = state.sessionGeneration, epoch = state.epoch;
    state.advising = true;
    syncControls();
    showError(null);
    el("sync").textContent = "Помощник готовит рекомендацию. Первый ответ локальной модели может занять до 4 минут; действия не выполняются.";

    const snapshotId = state.currentSnapshot.snapshot_id;
    const project = state.currentProject;
    const previous = state.adviceRequest;
    const reqBody = previous && previous.question === q && previous.snapshot_id === snapshotId ? previous : {
      schema_version: 1,
      snapshot_id: state.currentSnapshot.snapshot_id,
      question: q,
      idempotency_key: "ui-advice-" + (global.crypto.randomUUID ? global.crypto.randomUUID() : Math.random().toString(36).substring(2)),
    };

    state.adviceRequest = reqBody;
    try {
      const advice = await api(
        "/api/v1/projects/" + encodeURIComponent(state.currentProject) + "/setup/advice",
        {
          method: "POST",
          body: JSON.stringify(reqBody),
        }
      );
      if (generation !== state.sessionGeneration || epoch !== state.epoch || !state.currentSnapshot || advice.snapshot_id !== state.currentSnapshot.snapshot_id || project !== state.currentProject) return;
      renderAdvice(advice);
      el("sync").textContent = "Совет получен";
    } catch (err) {
      if (generation !== state.sessionGeneration || epoch !== state.epoch) return;
      showError("Ошибка получения совета LLM: " + err.message);
      el("sync").textContent = "Ошибка консультации";
    } finally {
      if (generation === state.sessionGeneration) { state.advising = false; syncControls(); }
    }
  }

  function renderAdvice(advice) {
    const container = el("advisor-result");
    container.hidden = false;

    // 1. Explanation — strict textContent, no HTML execution
    el("advisor-explanation").textContent = advice.explanation || "Нет разъяснений";

    // 2. Claims
    const claimsList = el("advisor-claims");
    claimsList.replaceChildren();

    const claims = advice.claims || [];
    if (!claims.length) {
      claimsList.append(make("p", "Нет зафиксированных утверждений.", "hint"));
    } else {
      claims.forEach(c => {
        const card = make("div", undefined, "claim-card");
        const badge = make("span", c.certainty, "badge " + c.certainty);
        const text = make("p", c.text);
        card.append(badge, text);
        claimsList.append(card);
      });
    }

    // 3. Proposals
    const proposalsList = el("advisor-proposals");
    proposalsList.replaceChildren();

    const proposals = advice.proposals || [];
    if (!proposals.length) {
      proposalsList.append(make("p", "Нет предложенных действий.", "hint"));
    } else {
      proposals.forEach(p => {
        const card = make("div", undefined, "proposal-card");

        const info = make("div", undefined, "proposal-info");
        const title = make("strong", p.action_id);
        const rationale = make("span", p.reason, "hint");
        info.append(title, rationale);

        const btn = make("button", "Сформировать план", "btn-action");
        btn.type = "button";
        btn.addEventListener("click", () => {
          if (state.currentSnapshot && advice.snapshot_id === state.currentSnapshot.snapshot_id) createPlan(p.action_id, p.arguments || {}, btn);
          else showError("Совет относится к предыдущему снимку.");
        });

        card.append(info, btn);
        proposalsList.append(card);
      });
    }
  }

  async function createPlan(actionId, actionArgs, opener) {
    if (!state.currentSnapshot || state.planning || state.pendingPlan || state.attempt || state.storageBlocked) {
      showError("Диагностический снимок недоступен");
      return;
    }

    if (!(state.currentSnapshot.available_action_ids || []).includes(actionId)) { showError("Операция отсутствует в разрешённом каталоге"); return; }
    const project = state.currentProject;
    const snapshotId = state.currentSnapshot.snapshot_id;
    const generation = state.sessionGeneration, epoch = state.epoch;
    state.planning = true; syncControls();
    showError(null);
    el("sync").textContent = "Формирование плана " + actionId + "…";

    const planReq = {
      schema_version: 1,
      action_id: actionId,
      arguments: actionArgs || {},
      snapshot_id: state.currentSnapshot.snapshot_id,
    };

    try {
      const plan = await api(
        "/api/v1/projects/" + encodeURIComponent(state.currentProject) + "/setup/plans",
        {
          method: "POST",
          body: JSON.stringify(planReq),
        }
      );
      if (generation !== state.sessionGeneration || epoch !== state.epoch || !state.currentSnapshot || snapshotId !== state.currentSnapshot.snapshot_id || project !== state.currentProject) return;
      if (plan.project_id !== project || plan.actor_id !== state.actor || plan.snapshot_id !== snapshotId || !digest(plan.plan_digest)) throw new Error("План не соответствует проекту или снимку");
      state.pendingPlan = plan;
      state.retryPermitted = true;
      state.applyKey = "ui-apply-" + global.crypto.randomUUID();
      syncControls();
      showPlanDialog(plan, opener);
      el("sync").textContent = "План сформирован";
    } catch (err) {
      if (generation !== state.sessionGeneration || epoch !== state.epoch) return;
      showError("Не удалось сформировать план: " + err.message);
      el("sync").textContent = "Ошибка создания плана";
    } finally { if (generation === state.sessionGeneration) { state.planning = false; syncControls(); } }
  }

  function showPlanDialog(plan, opener) {
    const dl = el("plan-details");
    dl.replaceChildren();

    const addRow = (key, val) => {
      dl.append(make("dt", key), make("dd", typeof val === "object" ? JSON.stringify(val) : String(val)));
    };

    addRow("Идентификатор плана", plan.plan_id);
    addRow("Действие", plan.action_id);
    addRow("Аргументы", plan.arguments);
    addRow("Условия", plan.preconditions);
    addRow("Эффекты", plan.effects);
    addRow("Digest", plan.plan_digest);
    addRow("Действителен до", plan.expires_at);
    addRow("Целевой объект", plan.target_project_id || plan.project_id);
    addRow("Затрагиваемые ресурсы", JSON.stringify(plan.resources || []) || "Нет");
    addRow("Требует подтверждения", plan.confirmation_required ? "Да (явное подтверждение)" : "Нет");

    const warning = el("plan-warning");
    if (plan.dangerous || plan.confirmation_required) {
      warning.textContent = "Внимание: данное действие изменяет состояние или запускает внешний процесс.";
      warning.hidden = false;
    } else {
      warning.hidden = true;
    }

    const dialog = el("plan-dialog");
    if (!dialog.open) {
      // Capture the invoking control explicitly: it is disabled before the
      // asynchronous plan returns, so the dialog's native previous focus may
      // already be BODY. The node is only a return candidate, never authority.
      state.planReturnFocus = {node: opener, generation: state.sessionGeneration, project: state.currentProject};
      dialog.showModal();
    }
  }

  function planMatchesAttempt(plan, attempt) {
    return plan && plan.kind === "setup_action_plan" && plan.schema_version === 1 &&
      plan.plan_id === attempt.plan_id && plan.plan_digest === attempt.plan_digest &&
      plan.project_id === attempt.project_id && plan.actor_id === attempt.actor_id &&
      typeof plan.action_id === "string" && Array.isArray(plan.effects) && Array.isArray(plan.resources);
  }

  function receiptMatchesPlan(receipt, plan) {
    const fields = ["kind", "schema_version", "operation_id", "plan_id", "plan_digest", "project_id", "action_id", "state",
      "created_at", "updated_at", "native_operation_id", "native_sprint_id", "evidence_ids", "error_code", "retryable", "deduplicated"];
    return receipt && typeof receipt === "object" && !Array.isArray(receipt) &&
      Object.keys(receipt).sort().join(",") === fields.sort().join(",") &&
      receipt.kind === "setup_operation_receipt" && receipt.schema_version === 1 && opaque(receipt.operation_id) &&
      receipt.plan_id === plan.plan_id && receipt.plan_digest === plan.plan_digest &&
      receipt.project_id === plan.project_id && receipt.action_id === plan.action_id &&
      ["PLANNED", "APPLYING", "SUCCEEDED", "FAILED", "BLOCKED", "UNKNOWN"].includes(receipt.state) &&
      typeof receipt.created_at === "string" && Number.isFinite(Date.parse(receipt.created_at)) &&
      typeof receipt.updated_at === "string" && Number.isFinite(Date.parse(receipt.updated_at)) &&
      [receipt.native_operation_id, receipt.native_sprint_id].every(v => v === null || opaque(v)) &&
      Array.isArray(receipt.evidence_ids) && receipt.evidence_ids.length <= 32 && receipt.evidence_ids.every(opaque) &&
      new Set(receipt.evidence_ids).size === receipt.evidence_ids.length &&
      (receipt.error_code === null || (typeof receipt.error_code === "string" && /^[A-Z][A-Z0-9_]{0,79}$/.test(receipt.error_code))) &&
      typeof receipt.retryable === "boolean" && typeof receipt.deduplicated === "boolean";
  }

  function receiptResolvesAttempt(receipt) {
    // An acknowledged native job is no longer an uncertain HTTP attempt.
    // Keep its normal owned Stop/recovery flow usable while agents are running.
    return ["SUCCEEDED", "FAILED", "BLOCKED"].includes(receipt.state) ||
      (receipt.state === "APPLYING" && ["selftest.start", "selftest.stop"].includes(receipt.action_id) &&
       opaque(receipt.native_operation_id));
  }

  async function inspectPendingApply() {
    const attempt = state.attempt;
    if (!attempt || state.applying || state.inspecting || !state.authenticated || state.actor !== attempt.actor_id) return;
    const generation = state.sessionGeneration, epoch = state.epoch, project = state.currentProject, actor = state.actor;
    const current = () => generation === state.sessionGeneration && state.attempt === attempt &&
      state.actor === actor && state.epoch === epoch && state.currentProject === project;
    state.inspecting = true; syncControls(); showError(null);
    try {
      const result = await api("/api/v1/projects/" + encodeURIComponent(attempt.project_id) +
        "/setup/plans/" + encodeURIComponent(attempt.plan_id) + "/inspect", {method: "POST", body: JSON.stringify({
          schema_version: 1, plan_digest: attempt.plan_digest, idempotency_key: attempt.idempotency_key})});
      // A delayed inspection cannot replace another page/session's confirmation.
      if (!current()) return;
      if (!result || result.kind !== "setup_apply_inspection" || result.schema_version !== 1 ||
          !planMatchesAttempt(result.plan, attempt) || typeof result.retry_permitted !== "boolean") {
        throw new Error("Ответ проверки не соответствует исходному запросу");
      }
      if (result.receipt !== null) {
        if (!receiptMatchesPlan(result.receipt, result.plan)) throw new Error("Квитанция не соответствует исходному запросу");
        state.attemptOperationId = result.receipt.operation_id;
        if (receiptResolvesAttempt(result.receipt)) clearAttempt();
        closePlan();
        recordReceipt(result.receipt, attempt.project_id, true);
        el("sync").textContent = receiptResolvesAttempt(result.receipt) ? "Найдена сохранённая квитанция исходного запроса" :
          "Операция ещё не завершена. Исходный ключ сохранён; проверьте текущий статус операции.";
      } else {
        // No recorded intent does not rule out a delayed in-flight request.
        state.pendingPlan = result.plan;
        state.applyKey = attempt.idempotency_key;
        state.retryPermitted = result.retry_permitted;
        showPlanDialog(result.plan, el("apply-inspect"));
        showError(result.retry_permitted ? "Квитанция пока не найдена. Можно явно подтвердить повтор исходного запроса с тем же ключом." :
          "Права на действие отозваны. Запрос сохранён; доступно обновление диагностики и повторное подтверждение сессии.");
      }
    } catch (err) {
      if (!current()) return;
      showError("Проверка исходного запроса недоступна: " + err.message + ". Запрос сохранён.");
      if (err.status === 401 || err.status === 403) await checkSession();
    } finally { if (generation === state.sessionGeneration) { state.inspecting = false; syncControls(); } }
  }

  async function applyPendingPlan() {
    if (!state.pendingPlan || state.applying || !state.authenticated || state.pendingPlan.actor_id !== state.actor ||
        !state.retryPermitted || state.storageBlocked) return;
    const plan = state.pendingPlan, key = state.applyKey;
    if (state.attempt && (!planMatchesAttempt(plan, state.attempt) || key !== state.attempt.idempotency_key)) return;
    // Fail before sending if an uncertain request cannot survive a page reload.
    try { persistAttempt(plan, key); }
    catch (_) { showError("Запрос не отправлен: не удалось сохранить его ключ в этой вкладке. Проверьте доступность хранилища браузера."); return; }
    const focusedApply = document.activeElement === el("plan-apply-btn");
    state.applying = true; syncControls(); showError(null);
    // Disabling the focused Apply button must not strand keyboard focus on BODY
    // while its response is pending. The native dialog remains the focus scope.
    if (focusedApply) el("plan-dialog").focus();
    el("sync").textContent = "Применение плана " + plan.plan_id + "…";
    let refresh = false, reauthenticate = false, rejected = false, returnFocus = null;
    try {
      const receipt = await api("/api/v1/projects/" + encodeURIComponent(plan.project_id) + "/setup/plans/" + encodeURIComponent(plan.plan_id) + "/apply", {
        method: "POST", body: JSON.stringify({schema_version: 1, plan_digest: plan.plan_digest, confirmed: true, idempotency_key: key})});
      if (!receiptMatchesPlan(receipt, plan)) throw new Error("Некорректная или чужая квитанция; результат не подтверждён");
      state.attemptOperationId = receipt.operation_id;
      if (receiptResolvesAttempt(receipt)) clearAttempt();
      state.pendingPlan = null; state.applyKey = null;
      returnFocus = closePlanDialog();
      recordReceipt(receipt, plan.project_id);
      el("sync").textContent = "Состояние операции (" + receipt.state + ")";
      refresh = true;
    } catch (err) {
      const detail = err.data && err.data.detail;
      const notStarted = err.status === 409 && detail && detail.error === "PLAN_STALE" &&
        detail.apply_outcome === "NOT_STARTED" && detail.plan_id === plan.plan_id && detail.plan_digest === plan.plan_digest &&
        receiptMatchesPlan(detail.receipt, plan) && detail.receipt.state === "BLOCKED" && detail.receipt.error_code === "PLAN_STALE";
      returnFocus = closePlanDialog();
      state.pendingPlan = null;
      if (notStarted) {
        clearAttempt(); state.applyKey = null;
        recordReceipt(detail.receipt, plan.project_id);
        rejected = true;
        el("sync").textContent = "Сервер подтвердил: устаревший план не выполнялся";
        refresh = true;
        showError("План устарел и не выполнялся. Обновите диагностику и сформируйте новый план.");
      } else {
        // Including401/403: authorization can be revoked after a committed effect.
        showError("Результат исходного запроса не подтверждён: " + err.message + ". Запрос и ключ сохранены; проверьте результат ниже.");
        el("sync").textContent = "Требуется восстановление исходного запроса";
        reauthenticate = err.status === 401 || err.status === 403;
      }
    } finally {
      state.applying = false; syncControls();
      const recoveryGeneration = state.sessionGeneration + (reauthenticate ? 1 : 0);
      if (reauthenticate) await checkSession();
      else if (refresh) await refreshSnapshot();
      if (rejected) showError("План устарел и не выполнялся. Для нового действия сформируйте новый план. " + (el("error").hidden ? "" : el("error").textContent));
      if (recoveryGeneration === state.sessionGeneration) restorePlanFocus(returnFocus);
    }
  }

  function recordReceipt(receipt, project, recorded = false) {
    state.receipts = state.receipts.filter(r => r.receipt.operation_id !== receipt.operation_id);
    state.receipts.unshift({receipt, project, recorded}); state.receipts = state.receipts.slice(0, 20);
    const list = el("receipts-list"); list.replaceChildren();
    state.receipts.forEach(({receipt: r, project: p, recorded: saved}) => {
      const card = make("div", undefined, "receipt-card");
      card.append(make("strong", r.action_id + " · " + r.state),
                  make("p", "Операция: " + r.operation_id + " · проект " + p, "hint"),
                  make("pre", JSON.stringify(r, null, 2), "check-evidence"));
      if (saved) card.append(make("p", "Сохранённое состояние. Для текущего состояния движка нажмите «Обновить статус».", "hint"));
      if (r.action_id === "setup.open" && r.state === "SUCCEEDED") {
        const help = {
          worker: "Настройте локальный Managed Client и защищённую ссылку на его токен в конфигурации сервера. Выполните worker.probe для default-executor. ID успешной проверки из evidence_ids используйте в worker.bind. Привязка не запускает спринт.",
          provider: "Проверьте доступ выбранного исполнителя к его провайдеру. Для внешнего советника нужны отдельная явная настройка модели и защищённая ссылка на ключ. Если изоляция инструментов Codex не подтверждена, мастер покажет детерминированную подсказку.",
          git: "Укажите репозиторий и его канонический Git-контекст в реестре сервера. git.probe проверяет чтение удалённых ссылок; это не подтверждение прав публикации.",
          telegram: "Настройте bot token и webhook secret через защищённые ссылки в конфигурации сервера. telegram.probe читает getMe и getWebhookInfo. Секреты в вопрос советнику вводить не нужно.",
          tunnel: "Запустите штатный шлюз и туннель для выбранного экземпляра. Сверьте владельца и свежесть heartbeat. Операторская страница /setup остаётся доступна только на частном адресе.",
          authorization: "Подтвердите код сессии через локальный операторский инструмент с постоянным именем оператора, например madoev. При повторном входе используйте то же имя, чтобы сохранить владение привязкой исполнителя и историей операций. Подтверждение без имени допускает чтение и проверки, но не привязку исполнителя или запуск самопроверки. Права на проекты и операции задаются в setup-policy.json; самопроверке нужен отдельный тестовый проект.",
        };
        (r.evidence_ids || []).forEach(id => {
          const topic = id.startsWith("setup-help-") ? id.slice(11) : "";
          if (help[topic]) card.append(make("p", help[topic], "notice"));
        });
      }
      const update = make("button", "Обновить статус"); update.type = "button";
      update.addEventListener("click", async () => {
        const generation = state.sessionGeneration, epoch = state.epoch;
        update.disabled = true;
        try {
          const value = await api("/api/v1/projects/" + encodeURIComponent(p) +
            "/setup/operations/" + encodeURIComponent(r.operation_id));
          if (generation !== state.sessionGeneration || epoch !== state.epoch || p !== state.currentProject) return;
          if (!receiptMatchesPlan(value, r) || value.operation_id !== r.operation_id) throw new Error("Квитанция не соответствует операции");
          const attempt = state.attempt;
          if (attempt && value.operation_id === state.attemptOperationId && state.actor === attempt.actor_id && value.project_id === attempt.project_id &&
              value.plan_id === attempt.plan_id && value.plan_digest === attempt.plan_digest && receiptResolvesAttempt(value)) {
            clearAttempt(); closePlan();
          }
          recordReceipt(value, p);
        } catch (err) {
          if (generation !== state.sessionGeneration || epoch !== state.epoch || p !== state.currentProject) return;
          showError("Статус недоступен: " + err.message); update.disabled = false;
        }
      });
      card.append(update); list.append(card);
    });
  }
  function closePlanDialog() {
    const dialog = el("plan-dialog");
    if (!dialog.open) return null;
    const target = state.planReturnFocus;
    state.planReturnFocus = null;
    dialog.close();
    return target;
  }

  function restorePlanFocus(target) {
    if (!target || document.querySelectorAll("dialog[open]").length) return;
    const current = target.generation === state.sessionGeneration && target.project === state.currentProject;
    const candidates = [current ? target.node : null, el("apply-inspect"), el("refresh"), el("session-check")];
    for (const node of candidates) {
      if (!node || !node.isConnected || node.disabled || node.matches(":disabled") ||
          node.closest("[hidden], [inert]") || !node.getClientRects().length) continue;
      node.focus();
      if (document.activeElement === node) return;
    }
  }

  function closePlan({restoreFocus = true} = {}) {
    if (state.applying) return;
    state.pendingPlan = null;
    if (!state.attempt) state.applyKey = null;
    // Enable the origin before closing, then restore an attached, current and
    // visible target. Never focus background content while another dialog is open.
    syncControls();
    const target = closePlanDialog();
    if (restoreFocus) restorePlanFocus(target);
    return target;
  }

  function init() {
    el("refresh").addEventListener("click", () => refreshSnapshot());

    el("project").addEventListener("change", () => {
      state.currentProject = el("project").value;
      refreshSnapshot();
    });

    el("advisor-form").addEventListener("submit", (e) => {
      e.preventDefault();
      requestAdvice();
    });

    // Quick questions
    document.querySelectorAll(".btn-chip").forEach(btn => {
      btn.addEventListener("click", () => {
        el("advisor-question").value = btn.dataset.q;
        requestAdvice(btn.dataset.q);
      });
    });

    el("plan-dialog-close").addEventListener("click", closePlan);
    el("plan-cancel-btn").addEventListener("click", closePlan);
    el("plan-dialog").addEventListener("cancel", event => { event.preventDefault(); closePlan(); });
    el("plan-apply-btn").addEventListener("click", applyPendingPlan);
    el("session-check").addEventListener("click", checkSession);
    el("apply-inspect").addEventListener("click", inspectPendingApply);

    // A bookmark is never authority and never automatically replays an action.
    restoreAttempt();
    syncControls();
    checkSession();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})(typeof window !== "undefined" ? window : globalThis);
