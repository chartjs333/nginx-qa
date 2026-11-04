/* Setup and Diagnostics Wizard v1 — Client UX.
 * Untrusted strings are rendered using textContent.
 * Strict Plan & Apply flow with explicit confirmation dialogs.
 */
(function (global) {
  "use strict";

  const state = {
    csrf: null,
    authenticated: false,
    currentProject: null,
    currentSnapshot: null,
    busy: false,
    pendingPlan: null,
    applyKey: null,
    applying: false,
    advising: false,
    adviceRequest: null,
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

  function syncControls() {
    const ready = state.authenticated && !!state.currentSnapshot;
    el("refresh").disabled = !state.authenticated || !state.currentProject || state.busy || !!state.pendingPlan;
    el("project").disabled = state.busy || !!state.pendingPlan;
    el("advisor-submit").disabled = !ready || state.advising;
    el("session-check").disabled = !!state.pendingPlan;
    document.querySelectorAll(".btn-chip").forEach(n => { n.disabled = !ready || state.advising; });
    document.querySelectorAll(".btn-op").forEach(n => { n.disabled = !ready || !!state.pendingPlan; });
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
    if (state.pendingPlan) return;
    state.authenticated = false;
    state.csrf = null;
    invalidateSnapshot();
    showError(null);
    try {
      const session = await api("/api/v1/operator/session", {method: "GET"});
      if (session.authenticated === true && session.csrf_token) {
        state.authenticated = true;
        state.csrf = session.csrf_token;
        showNotice("Операторская сессия подтверждена. Изменения защищены CSRF.");
        el("pairing").hidden = true;
        await loadProjects();
        if (state.currentProject) await refreshSnapshot();
      } else {
        showNotice("Подтвердите сессию через локальный операторский инструмент.");
        el("pairing-code").textContent = session.pairing_code || "Код недоступен";
        el("pairing").hidden = false;
        el("sync").textContent = "Ожидается подтверждение";
      }
    } catch (err) {
      showNotice("Доступ не подтверждён");
      showError(err.message);
    } finally { syncControls(); }
  }

  async function loadProjects() {
    const data = await api("/api/v1/setup/projects");
    const select = el("project");
    select.replaceChildren();
    const projects = data.projects || [];
    projects.forEach(p => {
      const opt = make("option", p.project_id + " · " + (p.name || ""));
      opt.value = String(p.project_id); select.append(opt);
    });
    if (projects.some(p => String(p.project_id) === state.currentProject)) {
      select.value = state.currentProject;
    }
    state.currentProject = select.value || null;
    if (!state.currentProject) showError("Нет доступных проектов. Проверьте реестр и права.");
  }

  async function refreshSnapshot() {
    if (state.busy || !state.authenticated || !state.currentProject || state.pendingPlan) return;
    state.busy = true; invalidateSnapshot();
    const epoch = state.epoch;
    const project = state.currentProject;
    el("sync").textContent = "Загрузка диагностики…";
    showError(null);
    try {
      const snapshot = await api("/api/v1/projects/" + encodeURIComponent(project) + "/setup/snapshot");
      if (epoch !== state.epoch || project !== state.currentProject) return;
      state.currentSnapshot = snapshot;
      renderSnapshot(snapshot);
      renderOperations(snapshot);
      el("sync").textContent = "Диагностика обновлена";
    } catch (err) {
      showError("Диагностика недоступна: " + err.message);
      el("sync").textContent = "Данные не подтверждены";
    } finally {
      if (epoch === state.epoch && project === state.currentProject) {
        await loadRecordedOperations(project, epoch);
      }
      state.busy = false;
      syncControls();
    }
  }

  async function loadRecordedOperations(project, epoch) {
    try {
      const history = await api("/api/v1/projects/" + encodeURIComponent(project) + "/setup/operations");
      if (epoch !== state.epoch || project !== state.currentProject) return;
      state.receipts = [];
      el("receipts-list").replaceChildren();
      const operations = (history.operations || []).slice(0, 20);
      operations.reverse().forEach(receipt => recordReceipt(receipt, project, true));
      if (!operations.length) el("receipts-list").append(make("p", "У этого оператора нет сохранённых операций в проекте.", "hint"));
    } catch (err) {
      if (epoch === state.epoch && project === state.currentProject) {
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
        createPlan(action, args);
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
      if (!state.currentSnapshot || advice.snapshot_id !== state.currentSnapshot.snapshot_id || project !== state.currentProject) return;
      renderAdvice(advice);
      el("sync").textContent = "Совет получен";
    } catch (err) {
      showError("Ошибка получения совета LLM: " + err.message);
      el("sync").textContent = "Ошибка консультации";
    } finally {
      state.advising = false;
      syncControls();
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
          if (state.currentSnapshot && advice.snapshot_id === state.currentSnapshot.snapshot_id) createPlan(p.action_id, p.arguments || {});
          else showError("Совет относится к предыдущему снимку.");
        });

        card.append(info, btn);
        proposalsList.append(card);
      });
    }
  }

  async function createPlan(actionId, actionArgs) {
    if (!state.currentSnapshot || state.pendingPlan) {
      showError("Диагностический снимок недоступен");
      return;
    }

    if (!(state.currentSnapshot.available_action_ids || []).includes(actionId)) { showError("Операция отсутствует в разрешённом каталоге"); return; }
    const project = state.currentProject;
    const snapshotId = state.currentSnapshot.snapshot_id;
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
      if (!state.currentSnapshot || snapshotId !== state.currentSnapshot.snapshot_id || project !== state.currentProject) return;
      if (plan.project_id !== project || plan.snapshot_id !== snapshotId || !plan.plan_digest) throw new Error("План не соответствует проекту или снимку");
      state.pendingPlan = plan;
      state.applyKey = "ui-apply-" + global.crypto.randomUUID();
      syncControls();
      showPlanDialog(plan);
      el("sync").textContent = "План сформирован";
    } catch (err) {
      showError("Не удалось сформировать план: " + err.message);
      el("sync").textContent = "Ошибка создания плана";
    }
  }

  function showPlanDialog(plan) {
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
      dialog.showModal();
    }
  }

  async function applyPendingPlan() {
    if (!state.pendingPlan || state.applying) return;
    state.applying = true;
    const plan = state.pendingPlan;

    el("plan-apply-btn").disabled = true;
    showError(null);
    el("sync").textContent = "Применение плана " + plan.plan_id + "…";

    const applyReq = {
      schema_version: 1,
      plan_digest: plan.plan_digest,
      confirmed: true,
      idempotency_key: state.applyKey,
    };

    try {
      const receipt = await api(
        "/api/v1/projects/" + encodeURIComponent(plan.project_id) + "/setup/plans/" + encodeURIComponent(plan.plan_id) + "/apply",
        {
          method: "POST",
          body: JSON.stringify(applyReq),
        }
      );

      el("plan-dialog").close();
      state.pendingPlan = null;
      state.applyKey = null;
      recordReceipt(receipt, plan.project_id);
      el("sync").textContent = "Состояние операции (" + receipt.state + ")";

      // Automatically refresh snapshot to reflect applied effects
      await refreshSnapshot();
    } catch (err) {
      showError("Ответ о применении не получен: " + err.message + ". Повтор использует тот же ключ.");
      el("sync").textContent = "Сбой применения";
    } finally {
      state.applying = false;
      el("plan-apply-btn").disabled = false;
      syncControls();
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
        const epoch = state.epoch;
        update.disabled = true;
        try {
          const value = await api("/api/v1/projects/" + encodeURIComponent(p) +
            "/setup/operations/" + encodeURIComponent(r.operation_id));
          if (epoch !== state.epoch || p !== state.currentProject) return;
          recordReceipt(value, p);
        } catch (err) { showError("Статус недоступен: " + err.message); update.disabled = false; }
      });
      card.append(update); list.append(card);
    });
  }
  function closePlan() {
    if (state.applying) return;
    el("plan-dialog").close(); state.pendingPlan = null; state.applyKey = null; syncControls();
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

    // Initial check
    syncControls();
    checkSession();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})(typeof window !== "undefined" ? window : globalThis);
