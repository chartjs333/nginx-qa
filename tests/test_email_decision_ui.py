"""Offline Node/DOM contracts; no browser, SMTP, live state, or listening port."""
from pathlib import Path
import shutil
import subprocess
import unittest


ASSET = Path(__file__).resolve().parents[1] / "nginx_qa" / "static" / "execution.js"

HARNESS = r"""
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm');
const mode = process.argv[2];
class Element {
  constructor(tag, id='') { this.tagName=tag; this.id=id; this.children=[]; this.listeners={}; this.dataset={}; this.attributes={}; this.disabled=false; this.hidden=false; this.open=false; this.value=''; this._text=''; }
  set textContent(value) { this._text=String(value); this.children=[]; }
  get textContent() { return this._text+this.children.map(child=>child.textContent).join(''); }
  get childNodes() { return this.children; }
  append(...children) { this.children.push(...children); if(this.tagName==='select' && !this.value && children.length) this.value=String(children[0].value); }
  replaceChildren(...children) { this.children=[]; this._text=''; if(this.tagName==='select') this.value=''; this.append(...children); }
  setAttribute(key,value) { this.attributes[key]=value; }
  addEventListener(type,handler) { (this.listeners[type]??=[]).push(handler); }
  async emit(type) { for(const handler of this.listeners[type]||[]) await handler({preventDefault(){}}); }
  showModal() { this.open=true; }
  close() { this.open=false; }
}
const ids=['project','sprint','checkpoint','sync','auth','error','email-decision','execution','attention','graph','visits','current','decisions','reviews','queue','timeline','detail','detail-title','detail-content','decision-dialog','decision-summary','decision-form','instructions','restrictions','decision-reason','validate','confirm','cancel','validation'];
const elements=Object.fromEntries(ids.map(id=>[id,new Element(['project','sprint','checkpoint'].includes(id)?'select':'div',id)]));
const request={request_id:'request-aurora',status:'pending',reason:'Human authority required',assignment_id:'assignment-aurora',base_scope_revision:1,identity:{node_id:'planner',agent_id:'planner-role',agent_phone:'7101'},proposal:{instructions:'Exact requested change',retained_restrictions:[' keep spacing  '],node_ids:['planner'],reviewer_ids:['r1','r2']},source:{url:'https://example.test/evidence',path:'scope/proposal.json'},dependencies:[]};
const notification={notification_id:'11111111-2222-4333-8444-555555555555',project_id:'aurora',sprint_id:'sprint-aurora',request_id:request.request_id,status:mode.includes('retry')?'failed':'delivered',error_code:mode.includes('retry')?'SMTP_DELIVERY_FAILED':null,expires_at:4070908800.125,binding:{execution_revision:7,base_scope_revision:1,assignment_id:request.assignment_id}};
const entry={notification,request,current:!['stale','expired','null-request'].includes(mode),stale_reason:mode==='expired'?'NOTIFICATION_EXPIRED':'NOTIFICATION_STALE',execution_revision:7,scope_revision:1};
if(mode==='null-request') entry.request=null;
if(mode==='missing-binding') delete notification.binding.base_scope_revision;
const another={...request,request_id:'different-request'};
const view={execution:{revision:7,current_node_id:'planner'},scope_revision:1,attention:{state:'waiting_for_human_decision'},topology:{known:true,nodes:[],edges:[]},visits:[],assignments:[],pending_decisions:[{...request,notification},another],scope_requests:[request],review_gates:[],queue:{known:true},timeline:[],notification_events:[{notification_id:notification.notification_id,kind:'delivery_delivered',sequence:3,timestamp:4070905000.5,error_code:null}],history:{available:[{checkpoint_id:'past',execution_revision:6}]},cursor:'cursor-1'};
const calls=[],intervals=[]; let authenticated=mode!=='unpaired', retryFailures=0, decisionFailures=0, validateResolver=null;
const clone = value => JSON.parse(JSON.stringify(value));
async function fetch(path,options={}) {
  const call={path,method:options.method||'GET',body:options.body?JSON.parse(options.body):null,headers:options.headers}; calls.push(call);
  const reply=(data,status=200)=>({ok:status<400,status,json:async()=>clone(data)});
  if(path==='/api/v1/operator/session') return reply({authenticated,csrf_token:authenticated?'synthetic-csrf':null,pairing_code:'1234567890'});
  if(path==='/api/v1/execution-catalog') return reply({projects:[{project_id:'aurora',sprints:[{sprint_id:'sprint-aurora'}]}]});
  if(path===`/api/v1/decision-notifications/${notification.notification_id}`) { if(mode==='missing') return reply({error:'NOTIFICATION_NOT_FOUND'},404); return reply(entry); }
  if(path.includes('/observability?')) return reply(view);
  if(path.endsWith('/validate')) {
    if(mode==='validation-race') await new Promise(resolve=>validateResolver=resolve);
    return reply({validation_id:'validation-1',execution_revision:7,scope_revision:1,before_effective:{scope:'old'},after_effective:{scope:'new'},diff:{before:'old',after:'new'}});
  }
  if(path.endsWith('/decisions')) {
    if(mode==='decision-retry' && decisionFailures++===0) throw new Error('Synthetic network interruption');
    if(mode==='conflict' || mode==='expired-on-submit') { entry.current=false; return reply({error:mode==='conflict'?'NOTIFICATION_STALE':'NOTIFICATION_LINK_EXPIRED'},mode==='conflict'?409:410); }
    return reply({accepted:true});
  }
  if(path.endsWith('/retry')) {
    if(mode==='notification-retry' && retryFailures++===0) throw new Error('Synthetic network interruption');
    notification.status='pending'; return reply({notification});
  }
  throw new Error('Unexpected API '+path);
}
let nonce=0;
const context={document:{getElementById:id=>elements[id],createElement:tag=>new Element(tag),createElementNS:(ns,tag)=>new Element(tag)},Option:function(text,value){const option=new Element('option');option.textContent=text;option.value=String(value);return option;},URL,URLSearchParams,fetch,crypto:{randomUUID:()=>`synthetic-uuid-${++nonce}`},location:{search:mode.startsWith('ordinary')?'':`?notification=${notification.notification_id}&project_id=ignored&sprint_id=ignored`},setInterval:fn=>intervals.push(fn),console};
context.window=context;
vm.runInNewContext(fs.readFileSync(process.argv[1],'utf8'),context,{filename:'execution.js'});
const flush=async()=>{for(let i=0;i<12;i++) await new Promise(resolve=>setImmediate(resolve));};
const walk=node=>[node,...node.children.flatMap(walk)];
const buttons=(id,label)=>walk(elements[id]).filter(node=>node.tagName==='button' && (!label||node.textContent===label));
const button=(id,label)=>{const found=buttons(id,label)[0];assert.ok(found,`Button ${label} absent in ${id}`);return found;};
const mutations=()=>calls.filter(call=>call.method!=='GET');
async function click(id,label) { const target=button(id,label); assert.equal(target.disabled,false); await target.emit('click'); await flush(); }
async function refresh() { for(const fn of intervals) await fn(); await flush(); }
(async()=>{
  await flush();
  assert.equal(mutations().length,0,'GET/page/scanner startup must not POST');
  if(!mode.startsWith('ordinary')) { assert.equal(elements.project.disabled,true); assert.equal(elements.sprint.disabled,true); assert.ok(walk(elements['email-decision']).some(node=>node.href==='/execution')); }
  if(['get','unpaired','stale','expired','missing','null-request'].includes(mode)) {
    await refresh(); assert.equal(mutations().length,0);
    if(mode==='get') { assert.equal(elements.project.value,'aurora'); assert.equal(elements.sprint.value,'sprint-aurora'); assert.equal(buttons('decisions','Изменить границы').length,0); assert.equal(buttons('decisions','Разрешить').length,1); assert.ok(elements['email-decision'].textContent.includes('planner-role')); assert.ok(elements['email-decision'].textContent.includes('2099-01-01T00:00:00.125Z')); assert.match(elements.timeline.textContent,/Notification audit — не переходы графа/); assert.match(elements.timeline.textContent,/delivery_delivered/); }
    if(mode==='unpaired') { assert.ok(elements.auth.textContent.includes('1234567890')); for(const action of ['Разрешить','Отклонить','Просмотреть точный diff']) assert.equal(button('email-decision',action).disabled,true); authenticated=true; await refresh(); assert.equal(button('email-decision','Разрешить').disabled,false); }
    if(['stale','expired','missing','null-request'].includes(mode)) { assert.match(elements['email-decision'].textContent,/устарело|недоступна/); for(const target of buttons('email-decision').filter(item=>['Разрешить','Отклонить','Просмотреть точный diff'].includes(item.textContent))) assert.equal(target.disabled,true); }
  } else if(mode==='ordinary-edit') {
    assert.equal(elements.project.disabled,false); assert.equal(buttons('decisions','Изменить границы').length,2); await click('decisions','Изменить границы'); assert.equal(elements.instructions.readOnly,false); elements.instructions.value='Ordinary edited scope'; await elements.validate.emit('click'); await flush(); assert.equal(mutations()[0].path,'/api/v1/projects/aurora/sprints/sprint-aurora/scope-requests/request-aurora/validate'); assert.equal(mutations()[0].body.proposal.instructions,'Ordinary edited scope');
  } else if(mode==='notification-retry' || mode==='ordinary-retry') {
    const panel=mode.startsWith('ordinary')?'decisions':'email-decision';
    assert.match(elements[panel].textContent,/SMTP_DELIVERY_FAILED/);
    await click(panel,'Повторить уведомление');
    if(mode==='notification-retry') await click(panel,'Повторить уведомление');
    const retries=mutations(); assert.ok(retries.every(call=>call.path.endsWith('/retry'))); if(retries.length>1) assert.deepEqual(retries[0].body,retries[1].body); assert.equal(view.execution.revision,7);
  } else if(mode==='history') {
    elements.checkpoint.value='past'; await elements.checkpoint.emit('change'); await flush(); assert.equal(button('email-decision','Разрешить').disabled,true); assert.equal(button('email-decision','Отклонить').disabled,true); assert.equal(mutations().length,0);
  } else if(mode==='validation-race') {
    await click('email-decision','Разрешить'); elements.validate.emit('click'); await flush(); await click('email-decision','Отклонить'); validateResolver(); await flush(); assert.equal(elements.confirm.textContent,'Подтвердить отказ'); assert.ok(!elements.validation.textContent.includes('Exact resulting scope diff'));
  } else {
    const reject=mode==='reject'; await click('email-decision',reject?'Отклонить':'Разрешить'); assert.equal(mutations().length,0,'Opening confirmation must not POST');
    if(!reject) { assert.equal(elements.instructions.readOnly,true); assert.equal(elements.confirm.disabled,true); await elements.validate.emit('click'); await flush(); }
    if(mode==='missing-binding') { assert.equal(mutations().length,0); assert.match(elements.validation.textContent,/привязку revisions/); return; }
    if(!reject) { assert.equal(mutations()[0].path,`/api/v1/decision-notifications/${notification.notification_id}/validate`); assert.deepEqual(mutations()[0].body.proposal,request.proposal); assert.equal(mutations()[0].body.expected_execution_revision,7); assert.equal(mutations()[0].body.expected_scope_revision,1); assert.match(elements.validation.textContent,/Exact resulting scope diff/); }
    await elements['decision-form'].emit('submit'); await flush();
    if(mode==='decision-retry') { await elements['decision-form'].emit('submit'); await flush(); }
    const decisions=mutations().filter(call=>call.path.endsWith('/decisions')); assert.ok(decisions.length); assert.ok(decisions.every(call=>call.path===`/api/v1/decision-notifications/${notification.notification_id}/decisions`)); assert.equal(decisions[0].body.action,reject?'reject':'approve');
    if(mode==='decision-retry') assert.deepEqual(decisions[0].body,decisions[1].body);
    if(mode==='conflict' || mode==='expired-on-submit') { assert.equal(elements.confirm.disabled,true); assert.match(elements['email-decision'].textContent,/устарело/); }
  }
  assert.ok(calls.every(call=>!JSON.stringify(call).includes('Bearer')));
  assert.ok(calls.every(call=>!call.path.includes('whoami')&&!call.path.includes('/work')&&!call.path.includes('/ack')));
  console.log('PASS '+mode);
})().catch(error=>{console.error(error.stack);process.exitCode=1;});
"""


@unittest.skipUnless(shutil.which("node"), "Node.js unavailable for isolated UI checks")
class EmailDecisionUiTests(unittest.TestCase):
    def run_scenario(self, scenario):
        result = subprocess.run(
            [shutil.which("node"), "-e", HARNESS, str(ASSET), scenario],
            capture_output=True, text=True, encoding="utf-8", timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_page_and_scanner_only_get_exact_bound_non_delta_request(self):
        self.run_scenario("get")

    def test_approve_uses_existing_csrf_workflow_after_explicit_preview(self):
        self.run_scenario("approve")

    def test_reject_requires_explicit_post_without_preview(self):
        self.run_scenario("reject")

    def test_unpaired_session_is_read_only_and_pairing_refresh_enables_actions(self):
        self.run_scenario("unpaired")

    def test_stale_expired_missing_and_drifted_request_fail_closed(self):
        for scenario in ("stale", "expired", "missing", "null-request"):
            with self.subTest(scenario=scenario):
                self.run_scenario(scenario)

    def test_missing_revision_binding_never_rebases_to_current_revision(self):
        self.run_scenario("missing-binding")

    def test_notification_retry_has_stable_idempotency_key(self):
        self.run_scenario("notification-retry")

    def test_normal_ui_notification_retry(self):
        self.run_scenario("ordinary-retry")

    def test_email_and_local_conflict_is_visible_and_blocks_submission(self):
        self.run_scenario("conflict")

    def test_expiration_after_preview_disables_confirmation(self):
        self.run_scenario("expired-on-submit")

    def test_exact_decision_retry_preserves_body_and_key(self):
        self.run_scenario("decision-retry")

    def test_historical_snapshot_has_no_email_decisions(self):
        self.run_scenario("history")

    def test_late_preview_cannot_replace_new_decision_dialog(self):
        self.run_scenario("validation-race")

    def test_ordinary_edit_behavior_remains_unchanged(self):
        self.run_scenario("ordinary-edit")

    def test_safe_rendering_email_choices_and_responsive_assets(self):
        script = r"""const assert=require('node:assert/strict'), ui=require(process.argv[1]);
assert.deepEqual(ui.emailDecisionChoices({status:'pending'}).map(choice=>choice[0]),['approve','reject']);
assert.deepEqual(ui.emailDecisionChoices({authorization_provenance:{kind:'existing_human_authorization'}}),[]);
assert.equal(ui.safeEvidenceUrl('javascript:alert(1)'),null);
assert.equal(ui.safeEvidenceUrl('data:text/html,<script>'),null);
assert.equal(ui.safeEvidenceUrl('https://secret@example.test'),null);
assert.equal(ui.safeEvidenceUrl('https://example.test/evidence'),'https://example.test/evidence');
assert.equal(ui.notificationDate(4070908800.125),'2099-01-01T00:00:00.125Z');
assert.equal(ui.notificationDate('2099-01-01T00:00:00Z'),'2099-01-01T00:00:00.000Z');
assert.match(ui.notificationDate(null),/неизвестно/);
assert.equal(ui.notificationPath('a/b'),'/api/v1/decision-notifications/a%2Fb');"""
        result = subprocess.run([shutil.which("node"), "-e", script, str(ASSET)], capture_output=True, text=True, encoding="utf-8", timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        source = ASSET.read_text(encoding="utf-8")
        html = ASSET.with_suffix(".html").read_text(encoding="utf-8")
        css = ASSET.with_suffix(".css").read_text(encoding="utf-8")
        self.assertNotIn("innerHTML", source)
        self.assertNotIn("onclick=", html)
        self.assertNotIn("<style>", html)
        self.assertIn('name="referrer" content="no-referrer"', html)
        self.assertIn("max-width:520px", css)


if __name__ == "__main__":
    unittest.main()
