const fs=require('fs');
const {chromium}=require('C:/nqt37/qualification/browser-tests/node_modules/playwright');
const assert=require('assert/strict');
const {execFileSync}=require('child_process');
const root='C:/nqt35/client/sdw-continuation-20261104/qualification-final';
const base='http://127.0.0.1:18038';
(async()=>{
 const browser=await chromium.launch({executablePath:'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',headless:true});
 const context=await browser.newContext({viewport:{width:1440,height:1050}});
 const page=await context.newPage();page.setDefaultTimeout(10000);const errors=[],calls=[],diagnosticBusyRetries=[];
 page.on('pageerror',error=>errors.push(error.message));
 page.on('response',r=>{if(r.url().startsWith(base+'/api/'))calls.push({path:new URL(r.url()).pathname,method:r.request().method(),status:r.status()});});
 try{
  await page.goto(base+'/setup');
  await page.locator('#pairing-code').waitFor({state:'visible'});
  const code=await page.locator('#pairing-code').textContent();
  assert.match(code,/^[A-F0-9]{10}$/);
  execFileSync('C:/nqt37/v/Scripts/python.exe',['-X','utf8','-B',root+'/authorize_wizard_browser.py','--pairing-code',code],{windowsHide:true,timeout:30000,stdio:'pipe'});
  console.log('Recipient-bound named pairing complete');
  await page.locator('#session-check').click();
  await page.locator('.check-item').first().waitFor({state:'visible'});
  assert.equal(await page.locator('#project option').count(),2);
  assert.equal(await page.locator('#project').inputValue(),'9000');
  const checks=await page.locator('.check-item').count();assert.ok(checks>=12);
  await page.locator('#advisor-question').fill('Как подключить исполнителя?');
  const beforeAdviceCalls=calls.length;
  const [adviceResponse]=await Promise.all([page.waitForResponse(r=>new URL(r.url()).pathname==='/api/v1/projects/9000/setup/advice' && r.request().method()==='POST',{timeout:260000}),page.locator('#advisor-submit').click()]);
  assert.equal(adviceResponse.status(),200);const actualAdvice=await adviceResponse.json();
  assert.ok(actualAdvice.explanation.startsWith('Ответ модели'));
  assert.equal(calls.slice(beforeAdviceCalls).filter(r=>r.method==='POST' && /\/(plans|apply)(\/|$)/.test(r.path)).length,0);
  await page.locator('#advisor-result').waitFor({state:'visible'});
  console.log('Actual Ollama response rendered; no automatic actions');
  await page.screenshot({path:root+'/wizard-model-advice.png',fullPage:true});
  fs.writeFileSync(root+'/browser-model-advice.json',JSON.stringify(actualAdvice,null,2));
  const advice=await page.locator('#advisor-explanation').innerText();
  const operations=await page.locator('#operations-toolbar').innerText();
  // Use the actual server-catalog button. No browser-injected API calls.
  const form=page.locator('#operations-toolbar form').filter({hasText:'Открыть инструкцию'});
  if(await form.count()!==1)throw Error('Expected one setup.open operation: '+operations);
  const isPlan=r=>new URL(r.url()).pathname==='/api/v1/projects/9000/setup/plans' && r.request().method()==='POST';
  const [cancelPlanResponse]=await Promise.all([page.waitForResponse(isPlan),form.locator('button').click()]);
  assert.equal(cancelPlanResponse.status(),200); const cancelledPlan=await cancelPlanResponse.json();
  await page.locator('#plan-dialog').waitFor({state:'visible'});
  await page.locator('#plan-cancel-btn').click();
  const [planResponse]=await Promise.all([page.waitForResponse(isPlan),form.locator('button').click()]);
  assert.equal(planResponse.status(),200);const plan=await planResponse.json();
  assert.notEqual(plan.plan_id,cancelledPlan.plan_id);
  await page.locator('#plan-dialog').waitFor({state:'visible'});
  const applyPath='/api/v1/projects/9000/setup/plans/'+plan.plan_id+'/apply';
  const [applied]=await Promise.all([page.waitForResponse(r=>new URL(r.url()).pathname===applyPath && r.request().method()==='POST'),page.locator('#plan-apply-btn').click()]);
  assert.equal(applied.status(),200);const receipt=await applied.json();
  assert.equal(receipt.plan_id,plan.plan_id);assert.equal(receipt.action_id,'setup.open');
  assert.equal(receipt.state,'SUCCEEDED');assert.equal(receipt.deduplicated,false);
  const exactCard=()=>page.locator('.receipt-card').filter({hasText:receipt.operation_id});
  await exactCard().locator('strong').filter({hasText:'SUCCEEDED'}).waitFor({state:'visible'});
  assert.equal(await exactCard().count(),1);
  const [historyResponse]=await Promise.all([page.waitForResponse(r=>new URL(r.url()).pathname==='/api/v1/projects/9000/setup/operations' && r.request().method()==='GET'),page.reload()]);
  assert.equal(historyResponse.status(),200);const history=await historyResponse.json();
  assert.equal(history.operations.filter(r=>r.operation_id===receipt.operation_id && r.state==='SUCCEEDED').length,1);
  await page.locator('.check-item').first().waitFor({state:'visible'});
  await exactCard().locator('strong').filter({hasText:'SUCCEEDED'}).waitFor({state:'visible'});
  assert.equal(calls.filter(r=>r.path.endsWith('/'+cancelledPlan.plan_id+'/apply')).length,0);
  async function selectProject(project){
   const predicate=r=>new URL(r.url()).pathname==='/api/v1/projects/'+project+'/setup/snapshot' && r.request().method()==='GET';
   let [response]=await Promise.all([page.waitForResponse(predicate),page.locator('#project').selectOption(project)]);
   for(let attempt=0;response.status()===429 && attempt<3;attempt++){
    const busy=await response.json();
    assert.ok(['SOURCE_BUSY','DIAGNOSTIC_CAPTURE_UNAVAILABLE'].includes(busy.detail?.error || busy.error));
    diagnosticBusyRetries.push({project,response:busy});
    await new Promise(resolve=>setTimeout(resolve,1000));
    [response]=await Promise.all([page.waitForResponse(predicate),page.locator('#refresh').click()]);
   }
   assert.equal(response.status(),200);return await response.json();
  }
  const project9010=await selectProject('9010');
  assert.equal(project9010.project_id,'9010');
  await page.locator('.check-item').first().waitFor({state:'visible'});
  assert.equal(await page.locator('#advisor-result').isVisible(),false);
  const project9000=await selectProject('9000');
  assert.equal(project9000.project_id,'9000');
  assert.ok(project9000.checks.some(c=>c.error_code==='STORE_UNAVAILABLE'));
  await exactCard().locator('strong').filter({hasText:'SUCCEEDED'}).waitFor({state:'visible'});
  assert.deepEqual(errors,[]);
  await page.screenshot({path:root+'/wizard-final.png',fullPage:true});
  fs.writeFileSync(root+'/browser-evidence.json',JSON.stringify({kind:'real_headless_edge_live_http',url:base+'/setup',source_commit:'a1841e420a517dfdb9ed2c5a4b44eaeb6b3286ca',actual_model_advice:actualAdvice,advisor_no_automatic_actions:true,project9000,project9010,project_switch_clears_advice:true,diagnosticBusyRetries,paired_named_actor:true,projects:2,checks,advice_text:advice,plan_cancel_apply:true,history_after_reload:true,cancelled_plan_id:cancelledPlan.plan_id,applied_plan_id:plan.plan_id,apply_path:applyPath,apply_response:receipt,exact_operation_after_reload:receipt.operation_id,page_errors:errors,requests:calls},null,2));
  console.log(JSON.stringify({status:'PASS',checks,requests:calls.length,errors}));
 }catch(e){
  await page.screenshot({path:root+'/wizard-failure.png',fullPage:true});
  fs.writeFileSync(root+'/browser-failure.json',JSON.stringify({url:page.url(),error:e.message,page_errors:errors,requests:calls},null,2));
  throw e;
 }finally{await context.close();await browser.close();}
})().catch(e=>{console.error(e.message);process.exitCode=1;});
