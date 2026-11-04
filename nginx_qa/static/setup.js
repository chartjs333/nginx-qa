/* Setup and Diagnostics Wizard v1 — Client UX.
 * Pure DOM construction with textContent to guarantee complete XSS immunity.
 * Strict Plan & Apply flow with explicit confirmation dialogs.
 */
(function (global) {
  "use strict";

  const state = {
    csrf: null,
    authenticated: false,
    currentProject: "9000",
    currentSnapshot: null,
    busy: false,
    pendingPlan: null,
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

  function getCookie(name) {
    const matches = document.cookie.match(
      new RegExp("(?:^|; )" + name.replace(/([\.$?*|{}\(\)\[\]\\\/\+^])/g, "\\$1") + "=([^;]*)")
    );
    return matches ? decodeURIComponent(matches[1]) : null;
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
    try {
      // 1. Check existing operator session cookie or query /api/v1/operator/session
      const sessionData = await api("/api/v1/operator/session", { method: "POST" });
      if (sessionData && sessionData.csrf_token) {
        state.csrf = sessionData.csrf_token;
        state.authenticated = true;
        showNotice("Операторская сессия активна · CSRF защищён");
        return;
      }
    } catch (_) {
      // Fallback: check cookie directly
      const cookieCsrf = getCookie("nginx_qa_csrf") || getCookie("csrf_token");
      if (cookieCsrf) {
        state.csrf = cookieCsrf;
      }
    }

    try {
      // Try to load projects to see if authorized
      await loadProjects();
      state.authenticated = true;
      showNotice("Операторская сессия подтверждена");
    } catch (err) {
      showNotice("Требуется авторизация оператора (сессия или токен)");
    }
  }

  async function loadProjects() {
    try {
      const data = await api("/api/v1/setup/projects");
      const select = el("project");
      select.replaceChildren();

      const projects = data.projects || [];
      if (!projects.length) {
        const opt = make("option", "Проект 9000 (по умолчанию)");
        opt.value = "9000";
        select.append(opt);
      } else {
        projects.forEach(p => {
          const opt = make("option", p.project_id + " (" + p.name + ")");
          opt.value = p.project_id;
          if (p.project_id === state.currentProject) {
            opt.selected = true;
          }
          select.append(opt);
        });
      }
      state.currentProject = select.value || "9000";
    } catch (err) {
      // Default to 9000 if projects endpoint fails
      const select = el("project");
      if (!select.childNodes.length) {
        const opt = make("option", "9000");
        opt.value = "9000";
        select.append(opt);
      }
    }
  }

  async function refreshSnapshot() {
    if (state.busy) return;
    state.busy = true;
    el("sync").textContent = "Загрузка диагностики…";
    el("refresh").disabled = true;
    showError(null);

    try {
      const project = el("project").value || state.currentProject;
      const snapshot = await api("/api/v1/projects/" + encodeURIComponent(project) + "/setup/snapshot");
      state.currentSnapshot = snapshot;
      state.currentProject = project;

      renderSnapshot(snapshot);
      el("sync").textContent = "Снимок обновлён (" + new Date().toLocaleTimeString() + ")";
      el("advisor-submit").disabled = false;
    } catch (err) {
      showError("Ошибка получения диагностического снимка: " + err.message);
      el("sync").textContent = "Сбой обновления";
    } finally {
      state.busy = false;
      el("refresh").disabled = false;
    }
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
        errDiv.style.color = "#a72235";
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
    if (!state.currentSnapshot) {
      showError("Сначала необходимо загрузить диагностический снимок");
      return;
    }

    const q = (question || el("advisor-question").value || "").trim();
    if (!q) {
      showError("Пожалуйста, введите вопрос для LLM-помощника");
      return;
    }

    el("advisor-submit").disabled = true;
    showError(null);
    el("sync").textContent = "Запрос консультации LLM…";

    const reqBody = {
      schema_version: 1,
      snapshot_id: state.currentSnapshot.snapshot_id,
      question: q,
      idempotency_key: "ui-advice-" + (global.crypto.randomUUID ? global.crypto.randomUUID() : Math.random().toString(36).substring(2)),
    };

    try {
      const advice = await api(
        "/api/v1/projects/" + encodeURIComponent(state.currentProject) + "/setup/advice",
        {
          method: "POST",
          body: JSON.stringify(reqBody),
        }
      );
      renderAdvice(advice);
      el("sync").textContent = "Совет получен";
    } catch (err) {
      showError("Ошибка получения совета LLM: " + err.message);
      el("sync").textContent = "Ошибка консультации";
    } finally {
      el("advisor-submit").disabled = false;
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
        const rationale = make("span", p.rationale, "hint");
        info.append(title, rationale);

        const btn = make("button", "Сформировать план", "btn-action");
        btn.type = "button";
        btn.addEventListener("click", () => {
          createPlan(p.action_id, p.arguments || {});
        });

        card.append(info, btn);
        proposalsList.append(card);
      });
    }
  }

  async function createPlan(actionId, actionArgs) {
    if (!state.currentSnapshot) {
      showError("Диагностический снимок недоступен");
      return;
    }

    showError(null);
    el("sync").textContent = "Формирование плана " + actionId + "…";

    const planReq = {
      schema_version: 1,
      action_id: actionId,
      arguments: actionArgs || {},
      snapshot_id: state.currentSnapshot.snapshot_id,
      authority_fingerprint: state.currentSnapshot.authority_fingerprint,
    };

    try {
      const plan = await api(
        "/api/v1/projects/" + encodeURIComponent(state.currentProject) + "/setup/plans",
        {
          method: "POST",
          body: JSON.stringify(planReq),
        }
      );
      state.pendingPlan = plan;
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
    addRow("Описание", plan.description);
    addRow("Целевой объект", plan.target);
    addRow("Затрагиваемые ресурсы", (plan.affected_resources || []).join(", ") || "Нет");
    addRow("Требует подтверждения", plan.requires_confirmation ? "Да (явное подтверждение)" : "Нет");

    const warning = el("plan-warning");
    if (plan.dangerous || plan.requires_confirmation) {
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
    if (!state.pendingPlan) return;
    const plan = state.pendingPlan;

    el("plan-apply-btn").disabled = true;
    showError(null);
    el("sync").textContent = "Применение плана " + plan.plan_id + "…";

    const applyReq = {
      schema_version: 1,
      plan_id: plan.plan_id,
      confirmation: true,
      idempotency_key: "ui-apply-" + (global.crypto.randomUUID ? global.crypto.randomUUID() : Math.random().toString(36).substring(2)),
    };

    try {
      const receipt = await api(
        "/api/v1/projects/" + encodeURIComponent(state.currentProject) + "/setup/plans/" + encodeURIComponent(plan.plan_id) + "/apply",
        {
          method: "POST",
          body: JSON.stringify(applyReq),
        }
      );

      el("plan-dialog").close();
      recordReceipt(receipt);
      el("sync").textContent = "План успешно применён (" + receipt.status + ")";

      // Automatically refresh snapshot to reflect applied effects
      await refreshSnapshot();
    } catch (err) {
      showError("Ошибка применения плана: " + err.message);
      el("sync").textContent = "Сбой применения";
    } finally {
      el("plan-apply-btn").disabled = false;
      state.pendingPlan = null;
    }
  }

  function recordReceipt(receipt) {
    state.receipts.unshift(receipt);
    const list = el("receipts-list");
    list.replaceChildren();

    state.receipts.slice(0, 10).forEach(r => {
      const card = make("div", undefined, "receipt-card");
      const title = make("strong", "Операция: " + r.action_id + " (" + r.status + ")");
      const time = make("div", "Завершена: " + new Date(r.completed_at || r.started_at).toLocaleTimeString(), "hint");
      const effects = make("div", "Применённые эффекты: " + JSON.stringify(r.effects_applied || {}), "hint");
      card.append(title, time, effects);
      list.append(card);
    });
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

    // Operations toolbar
    document.querySelectorAll(".btn-op").forEach(btn => {
      btn.addEventListener("click", () => {
        const action = btn.dataset.action;
        createPlan(action, {});
      });
    });

    // Dialog handlers
    el("plan-dialog-close").addEventListener("click", () => {
      el("plan-dialog").close();
      state.pendingPlan = null;
    });
    el("plan-cancel-btn").addEventListener("click", () => {
      el("plan-dialog").close();
      state.pendingPlan = null;
    });
    el("plan-apply-btn").addEventListener("click", () => {
      applyPendingPlan();
    });

    // Initial check
    checkSession().then(() => {
      refreshSnapshot();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})(typeof window !== "undefined" ? window : globalThis);
