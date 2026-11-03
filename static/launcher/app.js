'use strict';
(() => {
  const handle = location.pathname.split('/').pop();
  const notice = document.getElementById('notice');
  const workspace = document.getElementById('workspace');
  let csrf = null, timer = null, busy = false;
  const path = '/launcher/instances/' + encodeURIComponent(handle);
  async function request(url, body, mutation = false) {
    const headers = {};
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    if (mutation && csrf) headers['X-CSRF-Token'] = csrf;
    const response = await fetch(url, {method: body === undefined ? 'GET' : 'POST', headers,
      credentials: 'same-origin', cache: 'no-store', body: body === undefined ? undefined : JSON.stringify(body)});
    const data = await response.json();
    if (!response.ok) {
      const error = new Error(data.detail?.message || 'Запрос недоступен. Откройте Mini App заново.');
      error.status = response.status;
      throw error;
    }
    return data;
  }
  function items(id, values, format) {
    const target = document.getElementById(id);
    target.replaceChildren();
    if (!values.length) { target.textContent = 'Пока нет данных.'; return; }
    for (const value of values) {
      const row = document.createElement('pre');
      row.className = 'item';
      row.textContent = format(value);
      target.append(row);
    }
  }
  function show(data) {
    workspace.hidden = false;
    document.getElementById('state').textContent = data.status;
    document.getElementById('mode').textContent = 'Проект ' + data.project_id + ' · ' + (data.autonomous_mode ? 'Автономный запуск' : 'Ручной запуск');
    document.getElementById('counts').textContent = 'Заданий: ' + data.assignment_count + ' · Reviews: ' + data.review_count;
    document.getElementById('blocked').textContent = data.blocked_reason ? 'Ожидание: ' + data.blocked_reason : '';
    items('assignments', data.assignments, a => [a.node_id, 'Исполнитель: ' + a.agent_phone, a.status + (a.live ? ' · выполняется' : ''), a.result_commit || ''].filter(Boolean).join('\n'));
    items('reviews', data.reviews, r => ['Reviewer ' + r.reviewer_phone, r.decision || r.status, r.result_commit].join('\n'));
    items('reports', data.reports, r => ['Отчёт агента · ' + r.commit, ...r.checks.map(c => c.name + ': ' + c.status), ...r.artifacts.map(a => a.path + ' @ ' + a.commit), r.pr_url || ''].filter(Boolean).join('\n'));
    if (!data.reports.length) document.getElementById('reports').textContent = 'Результаты проверок не представлены (NOT_REPORTED).';
    if (data.csrf_token) csrf = data.csrf_token;
    notice.textContent = 'Состояние обновлено. Доступ проверен сервером.';
  }
  async function refresh() { show(await request(path + '/status')); }
  async function login() {
    try { await refresh(); return; } catch (error) { if (error.status !== 401) throw error; }
    const initData = window.Telegram?.WebApp?.initData;
    if (!initData) throw new Error('Откройте кнопку «Открыть Web UI» в личном чате Telegram.');
    const challenge = await request('/launcher/auth/challenge', {handle});
    const exchange = await request('/launcher/auth/telegram', {handle, challenge: challenge.challenge, init_data: initData});
    const session = await request('/launcher/auth/exchange', {handle, challenge: challenge.challenge, exchange_token: exchange.exchange_token, init_data: initData});
    csrf = session.csrf_token;
    await refresh();
  }
  async function act(action) {
    if (busy) return;
    busy = true;
    document.querySelectorAll('button').forEach(button => button.disabled = true);
    try {
      if (action === 'refresh') await refresh();
      else if (action === 'logout') {
        await request(path + '/logout', {}, true);
        clearInterval(timer); csrf = null; workspace.hidden = true;
        notice.textContent = 'Вы вышли. Для нового входа снова откройте Mini App из Telegram.';
      } else { show(await request(path + '/' + action, {}, true)); }
    } catch (error) { notice.textContent = error.message; }
    finally { busy = false; document.querySelectorAll('button').forEach(button => button.disabled = false); }
  }
  for (const action of ['start', 'stop', 'refresh', 'logout']) document.getElementById(action).addEventListener('click', () => act(action));
  login().then(() => {
    window.Telegram?.WebApp?.ready();
    timer = setInterval(() => { if (!busy) refresh().catch(error => { notice.textContent = error.message; if ([401,403].includes(error.status)) {clearInterval(timer); workspace.hidden = true;} }); }, 5000);
  }).catch(error => { workspace.hidden = true; notice.textContent = error.message; });
})();
