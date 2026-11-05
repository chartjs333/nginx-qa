/* User drafts and native receipts only. No browser scheduler or model authority. */
(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  let csrf = null, actor = null, current = null, busy = false, epoch = 0;
  const errors = {
    TASK_PROJECT_SETUP_REQUIRED: 'Этот репозиторий ещё не подключён. Выберите подключённый проект; настройка исполнителя и доступа выполняется один раз.',
    TASK_EXECUTOR_NOT_READY: 'Исполнитель недоступен или не готов. Задание сохранено. Проверьте настройки и повторите запуск этого же задания.',
    TASK_SOURCE_CHANGED: 'Основная ветка изменилась после подготовки. Подготовьте новый план по актуальной версии.',
    TASK_PLAN_CHANGED: 'Настройки или план изменились. Подготовьте план заново.',
    PROJECT_BUSY: 'В этом проекте уже есть активная работа. Дождитесь её завершения или откройте текущее задание.',
    OPERATOR_SESSION_REQUIRED: 'Подтвердите вход, затем обновите статус сохранённого задания.',
    OPERATOR_CSRF_REQUIRED: 'Сессия изменилась. Нажмите «Проверить подтверждение сессии».',
    WORKBOOK_ACTIVE_CONTENT_FORBIDDEN: 'В файле обнаружены макросы или внешние ссылки. Нужен обычный Excel без активного содержимого.',
    TASK_OPERATION_FAILED: 'Не удалось завершить операцию. Проверьте статус этого же задания: ответ мог потеряться после начала запуска.'
  };
  function message(text, error=false) { const el = $(error ? 'error' : 'notice'); el.textContent = text; el.hidden = !text; }
  function text(tag, content, cls) { const e = document.createElement(tag); e.textContent = content; if (cls) e.className=cls; return e; }
  async function api(path, options={}) {
    const headers = {'Accept':'application/json', ...(options.body ? {'Content-Type':'application/json'} : {})};
    if (csrf) headers['X-Nginx-QA-CSRF'] = csrf;
    let res;
    try { res = await fetch(path, {...options, headers, credentials:'same-origin', cache:'no-store'}); }
    catch (_) { throw new Error('Ответ сервера недоступен. Сохранённое задание можно проверить после восстановления связи.'); }
    const data = await res.json();
    if (!res.ok) { const code = data.detail?.error || data.error || 'TASK_OPERATION_FAILED'; throw new Error(errors[code] || code); }
    return data;
  }
  function controls() {
    for (const id of ['project','repository','task','attachment','preview']) $(id).disabled = !actor || busy;
    $('start').disabled = !actor || busy || !current || ['Running','Completed','Stopped','Failed'].includes(current.status);
    $('refresh').disabled = !actor || busy || !current;
    $('resume').hidden = !current?.retry_available;
    $('resume').disabled = !actor || busy || !current?.retry_available;
    $('stop').disabled = !actor || busy || !current?.operation_id || ['Completed','Stopped','Failed'].includes(current.status);
  }
  async function action(fn) {
    if (busy) return;
    busy=true; controls(); message('', true);
    try { await fn(); } catch(e) { message(e.message, true); }
    finally { busy=false; controls(); }
  }
  async function session() {
    const generation=++epoch;
    const data=await api('/api/v1/operator/session');
    if(generation!==epoch)return;
    csrf=data.authenticated ? data.csrf_token : null; actor=data.authenticated ? data.actor : null;
    // Session endpoint exposes actor_id in some supported versions.
    if(data.authenticated && !actor) actor=data.actor_id || 'authenticated';
    $('session-message').textContent=data.authenticated ? 'Вход подтверждён.' : 'Подтвердите вход локальной командой.';
    $('pair-instructions').hidden=data.authenticated;
    $('pair-code').textContent=data.pairing_code || '';
    current=null; controls();
    if(!actor)return;
    const catalog=await api('/api/v1/tasks/projects');
    if(generation!==epoch)return;
    $('project').replaceChildren();
    for(const p of catalog.projects){const o=text('option',p.name);o.value=p.repository_url;$('project').append(o);}
    if(!$('repository').value)$('repository').value=$('project').value;
    await history();
    const id=location.hash.slice(1);
    if(/^task-[0-9a-f]{32}$/.test(id)) await load(id);
  }
  async function history() {
    const data=await api('/api/v1/tasks'); $('history').replaceChildren();
    for(const task of data.tasks){ const b=text('button',task.task.slice(0,90));b.type='button';b.className='secondary';b.addEventListener('click',()=>action(()=>load(task.task_id)));$('history').append(b);}
    if(!data.tasks.length)$('history').append(text('p','Пока нет заданий.'));
  }
  function render(data) {
    current=data; location.hash=data.task_id; $('plan').hidden=$('progress').hidden=false;
    $('plan-details').replaceChildren(text('p',data.repository_url),text('p','База: '+data.source_commit.slice(0,12)));
    if(data.attachment)$('plan-details').append(text('p',`Файл: ${data.attachment.name}; строк с данными: ${data.attachment.rows}.`));
    for(const effect of data.effects || [])$('plan-details').append(text('p',effect));
    if(data.executor?.status==='UNAVAILABLE')$('plan-details').append(text('p',data.executor.action));
    const labels={Draft:'План готов к запуску',Created:'Подготовка запуска',Prepared:'Подготовлено',Waiting:'Ожидание исполнителя',Running:'Выполняется',Completed:'Завершено и принято',Failed:'Работа требует внимания',Stopped:'Остановлено'};
    $('status').textContent=labels[data.status] || data.status;
    if(data.blocked_reason || data.last_error || data.attention)$('status').append(text('p',data.attention || errors[data.blocked_reason || data.last_error] || data.blocked_reason || data.last_error));
    $('steps').replaceChildren(...(data.steps || []).map(s=>text('span',s,'badge')));
    $('assignments').replaceChildren(...(data.assignments || []).map(a=>text('div',`${a.node_id}: ${a.status}${a.result_commit ? ' · '+a.result_commit.slice(0,12) : ''}`,'row')));
    $('reviews').replaceChildren(...(data.reviews || []).map(r=>text('div',`${r.reviewer_id || r.reviewer_phone || 'Reviewer'}: ${r.decision || 'ожидается'} · ${(r.result_commit || '').slice(0,12)}`,'row')));
    if(!data.reviews?.length)$('reviews').append(text('p','Отзывы ещё не получены.'));
    $('results').replaceChildren();
    for(const link of data.result_links || []){if(!/^https:\/\/github\.com\//.test(link.url))continue;const a=text('a',link.label);a.href=link.url;a.target='_blank';a.rel='noopener noreferrer';const row=text('p','');row.append(a);$('results').append(row);}
    $('validation').textContent=data.tests_status==='AGENT_REPORTED' ? 'Результаты тестов сообщены исполнителем; ниже сохранены отдельно от независимых отзывов.' : (data.summaries || []).length ? 'Отчёт исполнителя приведён выше. Результаты тестов оцениваются вместе с независимыми отзывами.' : 'Проверки реализации пока не представлены. Статус запуска сам по себе не означает прохождение тестов.';
    for(const report of data.reports || [])$('results').append(text('pre',JSON.stringify(report,null,2)));
    for(const summary of data.summaries || [])$('results').append(text('pre',summary.text));
    $('execution').href='/execution?project_id='+encodeURIComponent(data.project_id);
    controls();
  }
  async function load(id){render(await api('/api/v1/tasks/'+encodeURIComponent(id)));}
  $('session-check').addEventListener('click',()=>action(session));
  $('project').addEventListener('change',()=>{$('repository').value=$('project').value;});
  $('task-form').addEventListener('submit',event=>{event.preventDefault();action(async()=>{
    const request={repository_url:$('repository').value.trim(),task:$('task').value};
    const file=$('attachment').files[0];
    if(file){if(file.size>2*1024*1024)throw new Error('Файл больше 2 МБ.');const bytes=new Uint8Array(await file.arrayBuffer());let binary='';for(let i=0;i<bytes.length;i+=8192)binary+=String.fromCharCode(...bytes.subarray(i,i+8192));request.attachment_name=file.name;request.attachment_base64=btoa(binary);}
    message('Проверяем доступ к репозиторию и исходный файл…');
    render(await api('/api/v1/tasks/preview',{method:'POST',body:JSON.stringify(request)}));message('План подготовлен. Проверьте его и нажмите «Запустить работу».');await history();
  });});
  $('start').addEventListener('click',()=>action(async()=>{const saved=current;message('Запускаем. При потере ответа проверяйте это же задание.');try{render(await api('/api/v1/tasks/'+saved.task_id+'/start',{method:'POST',body:JSON.stringify({plan_digest:saved.plan_digest})}));message('Запрос обработан. Состояние ниже получено от движка.');}catch(e){try{await load(saved.task_id);}catch(_){}throw e;}}));
  $('refresh').addEventListener('click',()=>action(()=>load(current.task_id)));
  $('resume').addEventListener('click',()=>action(async()=>{render(await api('/api/v1/tasks/'+current.task_id+'/resume',{method:'POST',body:JSON.stringify({plan_digest:current.plan_digest})}));}));
  $('stop').addEventListener('click',()=>action(async()=>{if(!confirm('Остановить именно это задание? Уже созданные коммиты и история сохранятся.'))return;render(await api('/api/v1/tasks/'+current.task_id+'/stop',{method:'POST',body:'{}'}));}));
  setInterval(()=>{if(actor && current?.operation_id && !busy && !['Completed','Failed','Stopped'].includes(current.status))action(()=>load(current.task_id));},10000);
  action(session);
})();
