const fs=require('fs'),assert=require('assert/strict'),{execFileSync}=require('child_process');
const {chromium}=require('C:/nqt37/qualification/browser-tests/node_modules/playwright');
const root='C:/nqt35/client/sdw-ui-recovery-20261104';
(async()=>{
 const browser=await chromium.launch({executablePath:'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',headless:true});
 const context=await browser.newContext({viewport:{width:1440,height:1050}});const page=await context.newPage();
 try {
  await page.goto('http://127.0.0.1:18038/setup');await page.locator('#pairing-code').waitFor({state:'visible'});
  const code=await page.locator('#pairing-code').textContent();assert.match(code,/^[A-F0-9]{10}$/);
  execFileSync('C:/nqt37/v/Scripts/python.exe',['-X','utf8','-B','C:/nqt35/client/sdw-continuation-20261104/qualification-final/authorize_wizard_browser.py','--pairing-code',code],{windowsHide:true,timeout:30000,stdio:'pipe'});
  await page.locator('#session-check').click();await page.locator('.check-item').first().waitFor({state:'visible'});
  const form=page.locator('#operations-toolbar form').filter({hasText:'Открыть инструкцию'});
  const [planned]=await Promise.all([page.waitForResponse(r=>r.url().endsWith('/projects/9000/setup/plans')&&r.request().method()==='POST'),form.locator('button').click()]);
  assert.equal(planned.status(),200);const plan=await planned.json();
  const waitMs=Math.max(0,Date.parse(plan.expires_at)-Date.now()+1500);console.log(JSON.stringify({phase:'waiting_real_expiry',waitMs,plan_id:plan.plan_id}));
  await new Promise(resolve=>setTimeout(resolve,waitMs));
  const [applied]=await Promise.all([page.waitForResponse(r=>r.url().endsWith('/'+plan.plan_id+'/apply')&&r.request().method()==='POST'),page.locator('#plan-apply-btn').click()]);
  const response=await applied.json();assert.equal(applied.status(),409);assert.equal(response.detail.error,'PLAN_STALE');
  const evidence={kind:'actual_installed_browser_real_wall_clock_expiry_before_fix',source_commit:'a1841e420a517dfdb9ed2c5a4b44eaeb6b3286ca',at:new Date().toISOString(),waitMs,plan_id:plan.plan_id,expires_at:plan.expires_at,http_status:applied.status(),response,error_text:await page.locator('#error').innerText(),modal_open:await page.locator('#plan-dialog').evaluate(n=>n.open),refresh_disabled:await page.locator('#refresh').isDisabled(),session_check_disabled:await page.locator('#session-check').isDisabled(),project_disabled:await page.locator('#project').isDisabled(),new_model_or_sprint_started:false};
  assert(evidence.error_text.includes('Ответ о применении не получен'));assert(evidence.modal_open&&evidence.refresh_disabled&&evidence.session_check_disabled&&evidence.project_disabled);
  fs.writeFileSync(root+'/repro-before.json',JSON.stringify(evidence,null,2));await page.screenshot({path:root+'/repro-before.png'});
  console.log(JSON.stringify({reproduced:true,http_status:409,controls_locked:true}));
 }finally{await context.close();await browser.close();}
})().catch(e=>{console.error(e.message);process.exitCode=1;});
