/* Offline refresh scheduling contract: synthetic DOM/fetch/timers only. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const asset = process.argv[2], scenario = process.argv[3];
class Element {
  constructor(tag, id = '') {
    this.tagName = tag; this.id = id; this.children = []; this.listeners = {};
    this.dataset = {}; this.attributes = {}; this.disabled = false;
    this.hidden = false; this.open = false; this.value = ''; this._text = '';
  }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map(child => child.textContent).join(''); }
  get childNodes() { return this.children; }
  append(...children) {
    this.children.push(...children);
    if (this.tagName === 'select' && !this.value && children.length) this.value = String(children[0].value);
  }
  replaceChildren(...children) {
    this.children = []; this._text = '';
    if (this.tagName === 'select') this.value = '';
    this.append(...children);
  }
  setAttribute(key, value) { this.attributes[key] = value; }
  addEventListener(type, handler) { (this.listeners[type] ??= []).push(handler); }
  async emit(type) {
    if (type === 'click' && this.disabled) return;
    for (const handler of this.listeners[type] || []) await handler({ preventDefault() {} });
  }
  showModal() { this.open = true; }
  close() { this.open = false; }
}

const ids = ['project', 'sprint', 'checkpoint', 'refresh', 'sync', 'auth', 'error',
  'email-decision', 'execution', 'attention', 'graph', 'visits', 'current',
  'decisions', 'reviews', 'queue', 'timeline', 'detail', 'detail-title',
  'detail-content', 'decision-dialog', 'decision-summary', 'decision-form',
  'instructions', 'restrictions', 'decision-reason', 'validate', 'confirm',
  'cancel', 'validation'];
const elements = Object.fromEntries(ids.map(id => [id, new Element(
  ['project', 'sprint', 'checkpoint'].includes(id) ? 'select' : id === 'refresh' ? 'button' : 'div', id)]));
const timers = [], calls = [];
let holdNext = false, held = null, activeObservations = 0, maxActiveObservations = 0;
const clone = value => JSON.parse(JSON.stringify(value));
const notificationId = '11111111-2222-4333-8444-555555555555';
const notification = { notification_id: notificationId, project_id: 'synthetic',
  sprint_id: 'sprint-a', request_id: 'synthetic-request', expires_at: 4070908800,
  binding: { execution_revision: 1, base_scope_revision: 1 } };
function view(path) {
  const revision = path.includes('/sprint-c/') ? 3 : path.includes('/sprint-b/') ? 2 : 1;
  return { execution: { revision }, scope_revision: 1, attention: { state: 'none_recorded' },
    topology: { known: true, nodes: [], edges: [] }, visits: [], assignments: [],
    pending_decisions: [], scope_requests: [], review_gates: [], queue: { known: true },
    timeline: [], history: { available: [] }, cursor: 'synthetic-cursor-' + revision };
}
async function fetch(path, options = {}) {
  const method = options.method || 'GET';
  calls.push({ path, method });
  assert.equal(method, 'GET', 'Refresh must not mutate execution or scope');
  assert.ok(!/whoami|\/work|\/ack/.test(path), 'Refresh must not call execution endpoints');
  const reply = data => ({ ok: true, status: 200, json: async () => clone(data) });
  if (path === '/api/v1/operator/session') return reply({ authenticated: false, pairing_code: 'synthetic' });
  if (path === '/api/v1/execution-catalog') return reply({ projects: [{ project_id: 'synthetic',
    sprints: ['a', 'b', 'c'].map(suffix => ({ sprint_id: 'sprint-' + suffix })) }] });
  if (path === '/api/v1/decision-notifications/' + notificationId) {
    return reply({ notification, current: true, request: null, scope_revision: 1, execution_revision: 1 });
  }
  if (path.includes('/observability?')) {
    activeObservations++;
    maxActiveObservations = Math.max(maxActiveObservations, activeObservations);
    try {
      if (holdNext) {
        holdNext = false;
        await new Promise((resolve, reject) => { held = { resolve, reject }; });
      }
      return reply(view(path));
    } finally { activeObservations--; }
  }
  throw new Error('Unexpected synthetic request: ' + path);
}
const context = {
  document: { getElementById: id => elements[id], createElement: tag => new Element(tag),
    createElementNS: (ns, tag) => new Element(tag) },
  Option: function(text, value) { const option = new Element('option'); option.textContent = text;
    option.value = String(value); return option; },
  URL, URLSearchParams, fetch, console,
  crypto: { randomUUID: () => 'synthetic-uuid' },
  location: { search: scenario === 'notification' ? '?notification=' + notificationId : '' },
  setInterval: (callback, delay) => { timers.push({ callback, delay }); return timers.length; },
};
context.window = context;
vm.runInNewContext(fs.readFileSync(asset, 'utf8'), context, { filename: 'execution.js' });
const flush = async () => { for (let i = 0; i < 12; i++) await new Promise(resolve => setImmediate(resolve)); };
const observations = () => calls.filter(call => call.path.includes('/observability?'));
async function tick() { await timers[0].callback(); await flush(); }
async function beginHeldPoll() {
  holdNext = true; held = null;
  const pending = timers[0].callback();
  await flush();
  assert.ok(held, 'Poll must reach the synthetic pending request');
  return { pending };
}

(async () => {
  await flush();
  assert.equal(observations().length, 1, 'Initial load is immediate, without a timer tick');
  assert.equal(timers.length, 1, 'Start one auto-refresh timer');
  if (scenario === 'initial' || scenario === 'notification') {
    assert.equal(timers[0].delay, 120000, 'Auto refresh uses two minutes');
    await tick();
    assert.equal(observations().length, 2);
    if (scenario === 'notification') {
      assert.equal(calls.some(call => call.path === '/api/v1/execution-catalog'), false);
      assert.equal(elements.project.disabled, true);
      assert.equal(elements.sprint.disabled, true);
    }
  } else if (scenario === 'manual') {
    const html = fs.readFileSync(asset.replace(/\.js$/, '.html'), 'utf8');
    assert.match(html, /<button\b(?=[^>]*\bid="refresh")(?=[^>]*\btype="button")[^>]*>\s*Обновить\s*<\/button>/);
    await elements.refresh.emit('click'); await flush();
    assert.equal(observations().length, 2, 'Manual click refreshes before any timer fires');
    assert.equal(elements.refresh.disabled, false);
  } else if (scenario === 'single-flight') {
    const { pending } = await beginHeldPoll();
    assert.equal(elements.refresh.disabled, true);
    for (let i = 0; i < 5; i++) await tick();
    assert.equal(observations().length, 2, 'Busy polling must not overlap');
    held.resolve(); await pending; await flush();
    assert.equal(observations().length, 2, 'Skipped auto ticks must not queue a trailing refresh');
    assert.equal(maxActiveObservations, 1);
    assert.equal(elements.refresh.disabled, false);
  } else if (scenario === 'selection') {
    const { pending } = await beginHeldPoll();
    elements.sprint.value = 'sprint-b'; await elements.sprint.emit('change');
    elements.sprint.value = 'sprint-c'; await elements.sprint.emit('change');
    await flush();
    assert.equal(observations().length, 2, 'Selection changes must not overlap the active request');
    held.resolve(); await pending; await flush();
    assert.equal(observations().length, 3, 'Coalesce selection changes into one immediate trailing refresh');
    assert.match(observations()[2].path, /\/sprints\/sprint-c\/observability\?/);
    assert.equal(observations().some(call => call.path.includes('/sprints/sprint-b/')), false);
    assert.match(elements.sync.textContent, /revision 3/);
    assert.equal(elements.sprint.value, 'sprint-c');
    assert.equal(maxActiveObservations, 1);
  } else if (scenario === 'error') {
    const { pending } = await beginHeldPoll();
    assert.equal(elements.refresh.disabled, true);
    held.reject(new Error('Synthetic observation failure'));
    await pending; await flush();
    assert.equal(elements.refresh.disabled, false, 'Error must re-enable manual refresh');
    assert.equal(elements.error.hidden, false);
    await elements.refresh.emit('click'); await flush();
    assert.equal(observations().length, 3, 'Manual refresh can recover immediately after failure');
    assert.equal(elements.error.hidden, true);
  } else { throw new Error('Unknown contract scenario: ' + scenario); }
  assert.ok(calls.every(call => call.method === 'GET'));
  console.log('PASS ' + scenario);
})().catch(error => { console.error(error.stack); process.exitCode = 1; });
