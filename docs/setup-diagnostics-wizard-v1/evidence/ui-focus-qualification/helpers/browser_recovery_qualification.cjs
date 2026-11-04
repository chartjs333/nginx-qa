/* Real installed Edge/backend cases; explicitly labelled response fixtures. */
const fs=require('fs'),assert=require('assert/strict');
const {execFileSync}=require('child_process');
const {chromium}=require('C:/nqt37/qualification/browser-tests/node_modules/playwright');
const root='C:/nqt35/client/sdw-ui-recovery-20261104/focus-qualification';
const base='http://127.0.0.1:18038',storageKey='nginx-qa.setup.pending-apply.v1';
const report={kind:'real_installed_edge_recovery_qualification',product:'c9193d7d6cab67f0b191a86f5e0e268bce074f9e',viewport:{width:390,height:844},cases:[],requests:[],page_errors:[]};
const save=()=>fs.writeFileSync(root+'/browser-recovery-evidence.json',JSON.stringify(report,null,2));
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
let page,context;
const pathOf=r=>new URL(r.url()).pathname;
const response=(path,method='GET')=>page.waitForResponse(r=>pathOf(r)===path&&r.request().method()===method,{timeout:15000});
const applyCount=()=>report.requests.filter(r=>r.method==='POST'&&r.path.endsWith('/apply')).length;
const mutatingCount=()=>report.requests.filter(r=>r.method==='POST'&&(r.path.endsWith('/apply')||r.path.endsWith('/plans'))).length;
const bookmark=()=>page.evaluate(key=>sessionStorage.getItem(key),storageKey);
const ready=async(attempt=0)=>{
 await page.waitForFunction(()=>!document.getElementById('refresh').disabled&&(document.querySelectorAll('.check-item').length>=12||!document.getElementById('error').hidden));
 if(await page.locator('.check-item').count()>=12)return;
 const text=await page.locator('#error').innerText();
 assert.ok(attempt<3&&/DIAGNOSTIC_CAPTURE_UNAVAILABLE|SOURCE_BUSY/.test(text),'Unexpected readiness failure: '+text);
 report.natural_busy_recovery=report.natural_busy_recovery||[];report.natural_busy_recovery.push({kind:'ACTUAL_HTTP_BUSY_WITH_EXPLICIT_REFRESH',text});save();
 await delay(700);await page.locator('#refresh').click();return ready(attempt+1);
};
const openForm=()=>page.locator('#operations-toolbar form').filter({hasText:'Открыть инструкцию'});
async function pair(){
 await page.locator('#pairing-code').waitFor({state:'visible'});
 const code=(await page.locator('#pairing-code').textContent()).trim();assert.match(code,/^[A-F0-9]{10}$/);
 execFileSync('C:/nqt37/v/Scripts/python.exe',['-X','utf8','-B',root+'/authorize_wizard_browser.py','--pairing-code',code],{windowsHide:true,timeout:30000,stdio:'pipe'});
 await page.locator('#session-check').click();await ready();
}
async function refresh(){
 const [r]=await Promise.all([response('/api/v1/projects/9000/setup/snapshot'),page.locator('#refresh').click()]);
 assert.equal(r.status(),200);await ready();return await r.json();
}
async function plan(){
 const [r]=await Promise.all([response('/api/v1/projects/9000/setup/plans','POST'),openForm().locator('button').click()]);
 assert.equal(r.status(),200);const value=await r.json();await page.locator('#plan-dialog').waitFor({state:'visible'});return value;
}
async function apply(value){
 const path='/api/v1/projects/9000/setup/plans/'+value.plan_id+'/apply';
 await page.locator('#plan-apply-btn').focus();
 const [r]=await Promise.all([response(path,'POST'),page.keyboard.press('Enter')]);
 return {response:r,body:await r.json(),request:r.request().postDataJSON(),csrf:r.request().headers()['x-nginx-qa-csrf'],path};
}
async function noOverflow(label){
 const result=await page.evaluate(()=>({width:innerWidth,document:document.documentElement.scrollWidth,body:document.body.scrollWidth}));
 assert.ok(result.document<=result.width+1&&result.body<=result.width+1,label+': '+JSON.stringify(result));return result;
}
async function focusInfo(){return await page.evaluate(()=>{const e=document.activeElement,s=getComputedStyle(e);return {id:e.id,tag:e.tagName,text:e.textContent.trim().slice(0,90),documentFocused:document.hasFocus(),focusVisible:e.matches(':focus-visible'),outline:s.outline,shadow:s.boxShadow,inDialog:!!e.closest('#plan-dialog')};});}
async function record(name,kind,evidence){report.cases.push({name,kind,...evidence});save();console.log((evidence.status==='FAIL'?'FAIL: ':'PASS: ')+name);}

(async()=>{
 const browser=await chromium.launch({executablePath:'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',headless:true});
 context=await browser.newContext({viewport:report.viewport});page=await context.newPage();page.setDefaultTimeout(15000);
 page.on('pageerror',e=>report.page_errors.push(e.message));
 page.on('response',r=>{if(r.url().startsWith(base+'/api/'))report.requests.push({path:pathOf(r),method:r.request().method(),status:r.status()});});
 try{
  await page.goto(base+'/setup');await pair();
  const initialSnapshot=await refresh();
  assert.ok(initialSnapshot.checks.some(c=>c.status==='UNKNOWN'&&c.error_code==='STORE_UNAVAILABLE'));
  assert.ok(initialSnapshot.checks.some(c=>c.check_id==='telegram.configuration'&&c.status==='NOT_CONFIGURED'));
  const layout=await noOverflow('mobile dashboard');
  await page.locator('#refresh').focus();await page.keyboard.press('Tab');const tab=await focusInfo();
  assert.notEqual(tab.id,'refresh');assert.equal(tab.focusVisible,true);
  await page.keyboard.press('Shift+Tab');assert.equal((await focusInfo()).id,'refresh');
  const [keyboardRefresh]=await Promise.all([response('/api/v1/projects/9000/setup/snapshot'),page.keyboard.press('Enter')]);
  assert.equal(keyboardRefresh.status(),200);await ready();
  await page.locator('#refresh').scrollIntoViewIfNeeded();await page.screenshot({path:root+'/mobile-dashboard.png'});
  await openForm().locator('button').focus();
  const [keyboardPlan]=await Promise.all([response('/api/v1/projects/9000/setup/plans','POST'),page.keyboard.press('Enter')]);
  assert.equal(keyboardPlan.status(),200);await page.locator('#plan-dialog').waitFor({state:'visible'});
  const focus=[];for(let i=0;i<5;i++){await page.keyboard.press('Tab');focus.push(await focusInfo());}
  for(let i=0;i<4;i++){await page.keyboard.press('Shift+Tab');focus.push(await focusInfo());}
  report.keyboard_focus_probe=focus;save();
  assert.ok(focus.every(f=>(f.inDialog&&f.focusVisible)||(f.tag==='BODY'&&!f.documentFocused)));
  await page.locator('#refresh').evaluate(e=>e.focus());
  assert.equal((await focusInfo()).inDialog,true,'Background page control must remain inert');
  const modalLayout=await noOverflow('mobile plan modal');
  await page.screenshot({path:root+'/mobile-keyboard-modal.png'});
  const beforeCancel=applyCount();await page.keyboard.press('Escape');
  await page.locator('#plan-dialog').waitFor({state:'hidden'});
  assert.equal(applyCount(),beforeCancel);
  const focusReturned=await openForm().locator('button').evaluate(e=>document.activeElement===e);
  const afterEscape=await focusInfo();
  assert.equal(focusReturned,true);assert.equal(afterEscape.focusVisible,true);assert.equal(afterEscape.documentFocused,true);
  if(!focusReturned){report.findings=report.findings||[];report.findings.push({id:'E03_FOCUS_RETURN',message:'Escape closes dialog but does not return focus to its opener',afterEscape});}

  const dismissals=[];
  for(const control of ['#plan-cancel-btn','#plan-dialog-close']){
   await plan();const count=applyCount();await page.locator(control).focus();await page.keyboard.press('Enter');
   await page.locator('#plan-dialog').waitFor({state:'hidden'});
   assert.equal(await openForm().locator('button').evaluate(e=>document.activeElement===e),true);
   const focus=await focusInfo();assert.equal(focus.focusVisible,true);assert.equal(applyCount(),count);
   dismissals.push({control,focus,automatic_apply:0});
  }
  // Hold only the response to a real harmless setup.open, never invent effects.
  const heldPlan=await plan(),heldPath='/api/v1/projects/9000/setup/plans/'+heldPlan.plan_id+'/apply';
  let releaseHeld,seenHeld;const heldGate=new Promise(r=>releaseHeld=r),seenGate=new Promise(r=>seenHeld=r);let heldReceipt;
  await page.route(base+heldPath,async route=>{const actual=await route.fetch();assert.equal(actual.status(),200);heldReceipt=await actual.json();seenHeld();await heldGate;await route.fulfill({response:actual});},{times:1});
  await page.locator('#plan-apply-btn').focus();const pendingResponse=response(heldPath,'POST');await page.keyboard.press('Enter');await seenGate;
  const heldBookmark=await bookmark();assert.ok(heldBookmark);
  const pendingFocus=await focusInfo();assert.equal(pendingFocus.inDialog,true);assert.equal(pendingFocus.focusVisible,true);
  await page.keyboard.press('Escape');assert.equal(await page.locator('#plan-dialog').isVisible(),true);assert.equal(await bookmark(),heldBookmark);
  await page.locator('#plan-cancel-btn').focus();await page.keyboard.press('Enter');assert.equal(await page.locator('#plan-dialog').isVisible(),true);assert.equal(await bookmark(),heldBookmark);
  await page.locator('#plan-dialog-close').focus();await page.keyboard.press('Enter');assert.equal(await page.locator('#plan-dialog').isVisible(),true);assert.equal(await bookmark(),heldBookmark);
  releaseHeld();assert.equal((await pendingResponse).status(),200);await ready();await page.locator('#plan-dialog').waitFor({state:'hidden'});
  assert.equal(await bookmark(),null);const afterPending=await focusInfo();assert.equal(afterPending.focusVisible,true);assert.equal(afterPending.inDialog,false);
  report.pending_apply_focus={kind:'REAL_EFFECT_WITH_RESPONSE_HELD',receipt:heldReceipt,pendingFocus,afterPending,escape_cancel_close_preserve_attempt:true,automatic_apply:0};save();
  await record('mobile layout and keyboard dialog cycle','REAL_INSTALLED_BROWSER',{status:focusReturned?'PASS':'FAIL',layout,modalLayout,tab,focus,afterEscape,escape_returns_focus:focusReturned,cancel_has_no_apply:true,dismissals,pending_apply:report.pending_apply_focus});

  const expiring=await plan(),createdAt=Date.now();
  const wait=Math.max(0,Date.parse(expiring.expires_at)-Date.now()+1500);
  assert.ok(wait>110000&&wait<130000,'Use actual server expiry, not a fabricated clock');
  report.expiry_wait={started_at:new Date(createdAt).toISOString(),expires_at:expiring.expires_at,wait_ms:wait};save();
  for(let remaining=wait;remaining>0;){const step=Math.min(remaining,30000);await delay(step);remaining-=step;console.log('Natural expiry wait remaining ms: '+remaining);}
  const stale=await apply(expiring);assert.equal(stale.response.status(),409);
  const detail=stale.body.detail;assert.equal(detail.error,'PLAN_STALE');assert.equal(detail.apply_outcome,'NOT_STARTED');
  assert.equal(detail.receipt.state,'BLOCKED');assert.equal(detail.receipt.plan_id,expiring.plan_id);
  const busyBefore=(report.natural_busy_recovery||[]).length;
  await ready();assert.equal(await bookmark(),null);assert.equal(await page.locator('#plan-dialog').isVisible(),false);
  const recoveryRefreshed=(report.natural_busy_recovery||[]).length>busyBefore;
  // ready() explicitly refreshes after actual SOURCE_BUSY; that is newer UI work
  // and intentionally clears the prior notice. The authoritative receipt remains.
  if(!recoveryRefreshed){await page.waitForFunction(()=>document.getElementById('error').textContent.includes('не выполнялся'));}
  assert.equal(await page.locator('.receipt-card').filter({hasText:detail.receipt.operation_id}).count(),1);
  report.expiry_notice={explicit_busy_refresh:recoveryRefreshed,notice_visible:!(await page.locator('#error').isHidden()),durable_receipt_visible:true};save();
  const replayResponse=await context.request.post(base+stale.path,{headers:{Origin:base,'X-Nginx-QA-CSRF':stale.csrf},data:stale.request});
  assert.equal(replayResponse.status(),200);const replay=await replayResponse.json();
  assert.equal(replay.operation_id,detail.receipt.operation_id);assert.equal(replay.state,'BLOCKED');assert.equal(replay.deduplicated,true);
  report.natural_expiry_proof={elapsed_ms:Date.now()-createdAt,expired_plan:expiring,blocked:detail.receipt,explicit_delayed_same_key_replay:replay};save();
  // The explicit out-of-band replay invalidates issued snapshots in the real API.
  // Refresh through the UI before asking it to create a subsequent plan.
  await refresh();
  const freshPlan=await plan();assert.notEqual(freshPlan.plan_id,expiring.plan_id);
  const fresh=await apply(freshPlan);assert.equal(fresh.response.status(),200);assert.equal(fresh.body.state,'SUCCEEDED');
  assert.notEqual(fresh.request.idempotency_key,stale.request.idempotency_key);await ready();
  await page.locator('#refresh').scrollIntoViewIfNeeded();await page.screenshot({path:root+'/mobile-expiry-recovered.png'});
  await record('natural server expiry and durable same-key fence','REAL_INSTALLED_BACKEND',{elapsed_ms:Date.now()-createdAt,expired_plan:expiring,blocked:detail.receipt,explicit_delayed_same_key_replay:replay,fresh_receipt:fresh.body,automatic_apply:0});

  const sessionPlan=await plan();await context.clearCookies();
  const lost=await apply(sessionPlan);assert.equal(lost.response.status(),401);
  await page.locator('#pairing-code').waitFor({state:'visible'});
  const retained=await bookmark();assert.ok(retained);assert.equal(JSON.parse(retained).idempotency_key,lost.request.idempotency_key);
  const beforeRepair=applyCount();await pair();assert.equal(applyCount(),beforeRepair);assert.equal(await bookmark(),retained);
  const inspectPath='/api/v1/projects/9000/setup/plans/'+sessionPlan.plan_id+'/inspect';
  const [inspectionResponse]=await Promise.all([response(inspectPath,'POST'),page.locator('#apply-inspect').click()]);
  assert.equal(inspectionResponse.status(),200);const inspection=await inspectionResponse.json();
  await page.locator('#plan-dialog').waitFor({state:'visible'});assert.equal(await bookmark(),retained);
  await page.keyboard.press('Escape');assert.equal(await bookmark(),retained);
  await page.reload();await ready();assert.equal(await bookmark(),retained);assert.equal(applyCount(),beforeRepair);
  const [again]=await Promise.all([response(inspectPath,'POST'),page.locator('#apply-inspect').click()]);assert.equal(again.status(),200);
  const repaired=await apply(sessionPlan);assert.equal(repaired.response.status(),200);assert.equal(repaired.body.state,'SUCCEEDED');
  assert.deepEqual(repaired.request,lost.request);await ready();assert.equal(await bookmark(),null);
  await record('own session loss re-pair Escape reload and explicit same-key retry','REAL_OWN_BROWSER_SESSION_LOSS',{initial_http_status:401,retained_attempt:JSON.parse(retained),inspection,receipt:repaired.body,automatic_apply:0});

  const ambiguityPlan=await plan(),ambiguousPath='/api/v1/projects/9000/setup/plans/'+ambiguityPlan.plan_id+'/apply';let actualReceipt;
  await page.route(base+ambiguousPath,async route=>{const real=await route.fetch();assert.equal(real.status(),200);actualReceipt=await real.json();await route.fulfill({status:403,contentType:'application/json',body:JSON.stringify({detail:{error:'SETUP_ACCESS_DENIED'}})});},{times:1});
  const concealed=await apply(ambiguityPlan);assert.equal(concealed.response.status(),403);assert.equal(actualReceipt.state,'SUCCEEDED');
  await ready();assert.ok(await bookmark());const beforeInspect=applyCount();
  const concealedInspect='/api/v1/projects/9000/setup/plans/'+ambiguityPlan.plan_id+'/inspect';
  const [resolved]=await Promise.all([response(concealedInspect,'POST'),page.locator('#apply-inspect').click()]);
  assert.equal(resolved.status(),200);const resolution=await resolved.json();await ready();
  assert.equal(resolution.receipt.operation_id,actualReceipt.operation_id);assert.equal(await bookmark(),null);assert.equal(applyCount(),beforeInspect);
  await record('post-effect 403 reconciles without second Apply','ROUTED_403_AFTER_REAL_HARMLESS_EFFECT',{actual_backend_receipt:actualReceipt,inspection:resolution,real_permission_revocation:false,automatic_apply:0});

  const snapshotPath=base+'/api/v1/projects/9000/setup/snapshot',beforeFaults=mutatingCount();
  await page.route(snapshotPath,route=>route.fulfill({status:429,contentType:'application/json',body:JSON.stringify({detail:{error:'DIAGNOSTIC_CAPTURE_UNAVAILABLE'}})}),{times:1});
  const [busy]=await Promise.all([response('/api/v1/projects/9000/setup/snapshot'),page.locator('#refresh').click()]);assert.equal(busy.status(),429);
  await page.waitForFunction(()=>!document.getElementById('error').hidden&&!document.getElementById('refresh').disabled);
  assert.match(await page.locator('#error').innerText(),/недоступна/);await refresh();
  await page.route(snapshotPath,route=>route.abort('connectionfailed'),{times:1});
  await page.locator('#refresh').click();await page.waitForFunction(()=>!document.getElementById('error').hidden&&!document.getElementById('refresh').disabled);
  const offlineText=await page.locator('#error').innerText();await refresh();
  const missing=structuredClone(initialSnapshot);
  for(const check of missing.checks){if(['worker.configuration','worker.binding','provider.configuration'].includes(check.check_id)){check.status='NOT_CONFIGURED';check.error_code='NOT_CONFIGURED';check.evidence=[];check.next_action_ids=['setup.open'];}}
  await page.route(snapshotPath,route=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(missing)}),{times:1});await refresh();
  assert.ok((await page.locator('#checks-list').innerText()).includes('NOT_CONFIGURED'));
  const fallback={kind:'setup_advice',schema_version:1,snapshot_id:missing.snapshot_id,explanation:'Детерминированный диагностический отчёт. Ответ модели не получен. Настройте исполнителя и провайдера.',claims:[],proposals:[],uncertainty:'UNKNOWN'};
  await page.route(base+'/api/v1/projects/9000/setup/advice',route=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(fallback)}),{times:1});
  await page.locator('#advisor-question').fill('Почему исполнитель недоступен?');
  const [fallbackResponse]=await Promise.all([response('/api/v1/projects/9000/setup/advice','POST'),page.locator('#advisor-submit').click()]);assert.equal(fallbackResponse.status(),200);
  await page.locator('#advisor-result').waitFor({state:'visible'});assert.match(await page.locator('#advisor-explanation').innerText(),/Ответ модели не получен/);
  await page.locator('#advisor-explanation').scrollIntoViewIfNeeded();await page.screenshot({path:root+'/mobile-missing-fixture.png'});
  assert.equal(mutatingCount(),beforeFaults);await refresh();
  await record('busy unavailable reconnect missing worker/provider and explicit fallback','ROUTED_RESPONSE_AND_NETWORK_FIXTURES',{busy_http:429,offline_text:offlineText,explicit_refresh_restores_real_snapshot:true,missing_statuses:['worker.configuration','worker.binding','provider.configuration'],fallback_is_fixture:true,no_live_provider_claim:true,automatic_apply:0});

  const receiptId=repaired.body.operation_id,beforeReload=applyCount();
  await page.reload();await ready();assert.equal(await page.locator('.receipt-card').filter({hasText:receiptId}).count(),1);
  for(const project of ['9010','9000']){const [r]=await Promise.all([response('/api/v1/projects/'+project+'/setup/snapshot'),page.locator('#project').selectOption(project)]);assert.equal(r.status(),200);await ready();}
  assert.equal(await page.locator('.receipt-card').filter({hasText:receiptId}).count(),1);assert.equal(applyCount(),beforeReload);
  await page.setViewportSize({width:1280,height:900});await noOverflow('desktop dashboard');
  await page.locator('#refresh').scrollIntoViewIfNeeded();await page.screenshot({path:root+'/desktop-final.png'});
  await record('exact receipt after reload and project switch','REAL_INSTALLED_BROWSER',{operation_id:receiptId,projects:['9000','9010','9000'],automatic_apply:0});
  assert.deepEqual(report.page_errors,[]);report.status=report.findings?.length?'FAIL':'PASS';save();console.log(JSON.stringify({status:report.status,cases:report.cases.length,requests:report.requests.length,findings:report.findings||[]}));if(report.status==='FAIL')process.exitCode=1;
 }catch(error){report.status='FAIL';report.error=error.stack;save();await page.screenshot({path:root+'/browser-recovery-failure.png'});throw error;}
 finally{await context.close();await browser.close();}
})().catch(error=>{console.error(error.stack);process.exitCode=1;});
