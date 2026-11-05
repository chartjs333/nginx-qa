/* User drafts and native receipts only. No browser scheduler or model authority. */
(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  let csrf = null, actor = null, current = null, busy = false, epoch = 0;
  let projects = [], revisionOf = null, formDirty = false;
  const errors = {
    TASK_PROJECT_SETUP_REQUIRED: 'Этот репозиторий ещё не подключён. Выберите подключённый проект; настройка исполнителя и доступа выполняется один раз.',
    TASK_EXECUTOR_NOT_READY: 'Исполнитель недоступен или не готов. Задание сохранено. Проверьте настройки и повторите запуск этого же задания.',
    TASK_SOURCE_CHANGED: 'Выбранная ветка изменилась после подготовки. Подготовьте новый план по актуальной версии.',
    TASK_SOURCE_INVALID: 'Выберите ветку на origin или полный SHA коммита. Выражения вроде HEAD~1 и адрес другого репозитория не принимаются.',
    TASK_SOURCE_NOT_REMOTE: 'Ветка или коммит недоступны в ветках этого origin. Обновите список и проверьте выбранную базу.',
    TASK_REVISION_PROJECT_MISMATCH: 'Новая попытка должна принадлежать тому же проекту, что и исходное задание.',
    TASK_ATTACHMENT_MISSING: 'У исходного задания нет сохранённого Excel. Прикрепите новый файл или выберите «Без вложения».',
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
    for (const id of ['project','repository','task','attachment','preview','source-kind','source-value','sources-refresh','attachment-action','new-task']) $(id).disabled = !actor || busy;
    $('project').disabled = !actor || busy || !!revisionOf;
    $('repository').readOnly = !!revisionOf;
    $('attachment').disabled = !actor || busy || $('attachment-action').value !== 'replace';
    $('start').disabled = !actor || busy || formDirty || !current || ['Running','Completed','Stopped','Failed'].includes(current.status);
    $('revise').disabled = !actor || busy || !current;
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
    projects=catalog.projects;
    for(const p of projects){const o=text('option',p.name);o.value=p.repository_url;$('project').append(o);}
    if(!$('repository').value)$('repository').value=$('project').value;
    if(!$('source-value').value)$('source-value').value=(projects[0]?.source_ref || 'refs/heads/main').replace(/^refs\/heads\//,'');
    await history();
    const id=location.hash.slice(1);
    if(/^task-[0-9a-f]{32}$/.test(id)) await load(id);
  }
  async function history() {
    const data=await api('/api/v1/tasks'); $('history').replaceChildren();
    for(const task of data.tasks){ const b=text('button',`Попытка ${task.attempt || 1} · ${task.task_id.slice(-8)} · ${task.task.slice(0,90)}`);b.type='button';b.className='secondary';b.addEventListener('click',()=>action(()=>load(task.task_id)));$('history').append(b);}
    if(!data.tasks.length)$('history').append(text('p','Пока нет заданий.'));
  }
  function render(data) {
    current=data; location.hash=data.task_id; $('plan').hidden=$('progress').hidden=false;
    $('plan-details').replaceChildren(text('p',`Попытка ${data.attempt || 1} · ${data.task_id}`),text('p',data.repository_url),text('p','База: '+data.source_commit));
    if(data.source_selection)$('plan-details').append(text('p','Выбранный источник: '+data.source_selection.value));
    if(data.parent_task_id){const previous=text('button','Предыдущая попытка');previous.type='button';previous.className='secondary';previous.addEventListener('click',()=>action(()=>load(data.parent_task_id)));$('plan-details').append(previous);}
    $('plan-details').append(text('pre',data.task));
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
  async function sources() {
    const project=projects.find(p=>p.repository_url===$('repository').value.trim());
    if(!project)throw new Error(errors.TASK_PROJECT_SETUP_REQUIRED);
    const data=await api('/api/v1/tasks/projects/'+encodeURIComponent(project.project_id)+'/sources');
    $('source-branches').replaceChildren(...data.branches.map(row=>{const option=text('option',row.commit.slice(0,12));option.value=row.ref.replace(/^refs\/heads\//,'');return option;}));
    message(`Доступно веток: ${data.branches.length}. При подготовке план закрепит точный коммит.`);
  }
  async function revise() {
    const saved=current;
    const preserve=formDirty;
    if(preserve && !confirm('В форме есть несохранённые правки. Использовать введённый текст, выбранную базу и Excel для новой попытки этого задания? Отмена оставит всё как есть.'))return;
    if(preserve && $('repository').value.trim()!==saved.repository_url)throw new Error(errors.TASK_REVISION_PROJECT_MISMATCH);
    const seed=await api('/api/v1/tasks/'+saved.task_id+'/revision');
    revisionOf=seed.parent_task_id;
    $('project').value=seed.repository_url; $('repository').value=seed.repository_url;
    const keep=$('attachment-action').querySelector('option[value="keep"]');keep.disabled=!seed.attachment;
    $('retained-attachment').textContent=seed.attachment ? `Прежний файл: ${seed.attachment.name}; SHA256 ${seed.attachment.sha256}` : 'У исходного задания нет вложения.';
    $('retained-attachment').hidden=false;
    if(!preserve){
      $('task').value=seed.task; $('source-kind').value=seed.source.kind; $('source-value').value=seed.source.value;
      $('attachment').value=''; $('attachment-action').value=seed.attachment ? 'keep' : 'none';
    }
    $('editing').textContent=`Редактируется новая попытка ${seed.attempt} на основе ${seed.parent_task_id}. Прежняя история сохранится. Сначала подготовьте новый план, затем запустите его.`;
    formDirty=true; controls(); $('task').focus();
    message('Новая попытка ещё не сохранена и не запущена. Уточните текст, источник и вложение.');
    await sources();
  }
  $('session-check').addEventListener('click',()=>action(session));
  $('project').addEventListener('change',()=>{$('repository').value=$('project').value;const p=projects.find(p=>p.repository_url===$('project').value);$('source-kind').value='branch';$('source-value').value=(p?.source_ref || 'refs/heads/main').replace(/^refs\/heads\//,'');$('source-branches').replaceChildren();});
  $('task-form').addEventListener('input',()=>{formDirty=true;controls();});
  $('task-form').addEventListener('change',()=>{formDirty=true;controls();});
  $('sources-refresh').addEventListener('click',()=>action(sources));
  $('revise').addEventListener('click',()=>action(revise));
  $('new-task').addEventListener('click',()=>{
    if(formDirty && !confirm('Очистить несохранённую редакцию и начать другое задание?'))return;
    revisionOf=null;formDirty=false;$('task').value='';$('attachment').value='';$('attachment-action').value='replace';
    $('attachment-action').querySelector('option[value="keep"]').disabled=true;$('retained-attachment').hidden=true;
    $('editing').textContent='Новое задание. Подготовка плана ничего не запускает.';
    current=null;$('plan').hidden=$('progress').hidden=true;location.hash='';controls();$('task').focus();
  });
  window.addEventListener('beforeunload',event=>{if(formDirty){event.preventDefault();event.returnValue='';}});
  $('task-form').addEventListener('submit',event=>{event.preventDefault();action(async()=>{
    const value=$('source-value').value.trim(),kind=$('source-kind').value;
    const request={repository_url:$('repository').value.trim(),task:$('task').value,source:{kind,value:kind==='branch' && !value.startsWith('refs/') ? 'refs/heads/'+value : value}};
    if(revisionOf)request.parent_task_id=revisionOf;
    request.attachment_action=$('attachment-action').value;
    const file=request.attachment_action==='replace' ? $('attachment').files[0] : null;
    if(request.attachment_action==='replace' && !file){if(revisionOf)throw new Error('Выберите новый Excel или явно выберите «Без вложения».');request.attachment_action='none';}
    if(file){if(file.size>2*1024*1024)throw new Error('Файл больше 2 МБ.');const bytes=new Uint8Array(await file.arrayBuffer());let binary='';for(let i=0;i<bytes.length;i+=8192)binary+=String.fromCharCode(...bytes.subarray(i,i+8192));request.attachment_name=file.name;request.attachment_base64=btoa(binary);}
    message('Проверяем доступ к репозиторию и исходный файл…');
    const prepared=await api('/api/v1/tasks/preview',{method:'POST',body:JSON.stringify(request)});
    formDirty=false;render(prepared);$('editing').textContent='Форма сохранена в плане '+prepared.task_id+'. Любые новые правки требуют новой подготовки.';message('План подготовлен. Проверьте его и нажмите «Запустить работу».');await history();
  });});
  $('start').addEventListener('click',()=>action(async()=>{const saved=current;message('Запускаем. При потере ответа проверяйте это же задание.');try{render(await api('/api/v1/tasks/'+saved.task_id+'/start',{method:'POST',body:JSON.stringify({plan_digest:saved.plan_digest})}));message('Запрос обработан. Состояние ниже получено от движка.');}catch(e){try{await load(saved.task_id);}catch(_){}throw e;}}));
  $('refresh').addEventListener('click',()=>action(()=>load(current.task_id)));
  $('resume').addEventListener('click',()=>action(async()=>{render(await api('/api/v1/tasks/'+current.task_id+'/resume',{method:'POST',body:JSON.stringify({plan_digest:current.plan_digest})}));}));
  $('stop').addEventListener('click',()=>action(async()=>{if(!confirm('Остановить именно это задание? Уже созданные коммиты и история сохранятся.'))return;render(await api('/api/v1/tasks/'+current.task_id+'/stop',{method:'POST',body:'{}'}));}));
  setInterval(()=>{if(actor && current?.operation_id && !busy && !['Completed','Failed','Stopped'].includes(current.status))action(()=>load(current.task_id));},10000);
  action(session);
})();
