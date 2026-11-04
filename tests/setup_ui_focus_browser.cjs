/* Real Edge keyboard/focus behavior with all HTTP explicitly intercepted.
 * node tests/setup_ui_focus_browser.cjs <source-root> <output-directory> [--expect-defect]
 * This is an isolated browser fixture, never an installed-host qualification.
 */
const fs=require("fs"),path=require("path"),assert=require("assert/strict"),crypto=require("crypto");
const {chromium}=require(process.env.SDW_PLAYWRIGHT_MODULE||"C:/nqt37/qualification/browser-tests/node_modules/playwright");
const source=path.resolve(process.argv[2]||"."),out=path.resolve(process.argv[3]);
const expectDefect=process.argv.includes("--expect-defect");
const assets=path.join(source,"nginx_qa/static");
const {fixtureSnapshot,planFor,receiptFor}=require(path.join(source,"tests/setup_ui_dom.cjs"));
const storageKey="nginx-qa.setup.pending-apply.v1",reports=[],requests=[],errors=[];
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
const focusState=page=>page.evaluate(()=>({id:document.activeElement.id,tag:document.activeElement.tagName,
 documentFocused:document.hasFocus(),focusVisible:document.activeElement.matches(":focus-visible"),
 outline:getComputedStyle(document.activeElement).outlineStyle,inDialog:!!document.activeElement.closest("dialog[open]")}));
async function focusIs(page,locator){
 await page.waitForFunction(()=>document.activeElement!==document.body);
 assert.equal(await locator.evaluate(e=>document.activeElement===e),true,JSON.stringify(await focusState(page)));
 const state=await focusState(page);assert(state.documentFocused);assert(state.focusVisible);assert.notEqual(state.outline,"none");return state;
}
(async()=>{
 fs.mkdirSync(out,{recursive:true});
 const browser=await chromium.launch({executablePath:process.env.SDW_BROWSER_EXECUTABLE||"C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe",headless:true});
 let index=0;
 async function fixture(mode="network"){
  let authorized=true,applyCount=0,heldApply=null;
  const started=deferred(),plans=new Map(),records=[];
  const context=await browser.newContext({viewport:{width:390,height:844}}),page=await context.newPage();page.setDefaultTimeout(10000);
  page.on("pageerror",error=>errors.push(error.message));
  await context.route("**/*",async route=>{
   const request=route.request(),url=new URL(request.url()),p=url.pathname;assert.equal(url.origin,"https://focus-fixture.invalid");
   requests.push({path:p,method:request.method()});
   const json=(body,status=200)=>route.fulfill({status,contentType:"application/json",body:JSON.stringify(body)});
   if(p==="/setup")return route.fulfill({contentType:"text/html",body:fs.readFileSync(path.join(assets,"setup.html"))});
   if(["/setup/assets/setup.js","/setup/assets/setup.css"].includes(p))return route.fulfill({contentType:p.endsWith(".js")?"text/javascript":"text/css",body:fs.readFileSync(path.join(assets,path.basename(p)))});
   if(p==="/api/v1/operator/session")return json(authorized?{authenticated:true,actor_id:"operator-fixture",csrf_token:"fixture-only"}:{authenticated:false,pairing_code:"FIXTURE-PAIR"});
   if(p==="/api/v1/setup/projects")return json({projects:[{project_id:"9001",name:"Isolated focus fixture"}]});
   if(p.endsWith("/snapshot"))return json({...fixtureSnapshot("9001"),available_action_ids:["setup.open"]});
   if(p.endsWith("/operations"))return json({operations:records});
   if(p.endsWith("/advice"))return json({snapshot_id:fixtureSnapshot().snapshot_id,explanation:"Fixture only",claims:[],proposals:[{action_id:"setup.open",arguments:{topic:"git"},reason:"Fixture guidance"}]});
   if(p.endsWith("/plans")){const plan=planFor(request.postDataJSON(),++index,"operator-fixture","9001");plans.set(plan.plan_digest,plan);return json(plan);}
   if(p.endsWith("/inspect"))return json({kind:"setup_apply_inspection",schema_version:1,plan:plans.get(request.postDataJSON().plan_digest),receipt:null,retry_permitted:true});
   if(p.endsWith("/apply")){
    applyCount++;const plan=plans.get(request.postDataJSON().plan_digest);
    if(mode==="network")return route.abort("failed");
    if(mode==="unauth"){authorized=false;return json({detail:{error:"SESSION_EXPIRED"}},401);}
    const receipt=receiptFor(plan,{state:mode==="stale"?"BLOCKED":"SUCCEEDED",error_code:mode==="stale"?"PLAN_STALE":null});
    records.push(receipt);
    if(mode==="held"){heldApply=()=>json(receipt);started.resolve();return;}
    if(mode==="stale")return json({detail:{error:"PLAN_STALE",apply_outcome:"NOT_STARTED",plan_id:plan.plan_id,plan_digest:plan.plan_digest,receipt}},409);
    return json(receipt);
   }
   return route.abort("blockedbyclient");
  });
  await page.goto("https://focus-fixture.invalid/setup");await page.locator("#operations-toolbar button").waitFor();
  return {context,page,applyCount:()=>applyCount,authorize:()=>{authorized=true;},started,finish:()=>heldApply()};
 }
 async function open(page,opener=page.locator("#operations-toolbar button")){
  await opener.focus();await page.keyboard.press("Enter");await page.locator("#plan-dialog").waitFor({state:"visible"});return opener;
 }
 async function closed(page){await page.locator("#plan-dialog").waitFor({state:"hidden"});await page.evaluate(()=>new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r))));}
 try{
  const basic=await fixture(),page=basic.page,opener=await open(page);
  assert.equal(await opener.isDisabled(),true);
  await page.keyboard.press("Tab");await page.keyboard.press("Shift+Tab");await page.keyboard.press("Escape");await closed(page);
  if(expectDefect){
   const returned=await opener.evaluate(e=>document.activeElement===e),observed=await focusState(page);
   assert.equal(returned,false);assert.equal(observed.tag,"BODY");assert.equal(basic.applyCount(),0);
   reports.push({case:"original Escape defect",expected_defect_reproduced:true,observed});
   await page.screenshot({path:path.join(out,"before-focus.png")});
  }else{
   reports.push({case:"Escape returns visible keyboard focus to enabled Plan opener",observed:await focusIs(page,opener)});
   await page.screenshot({path:path.join(out,"escape-focus.png")});
   for(const close of ["plan-cancel-btn","plan-dialog-close"]){
    await open(page);await page.locator("#"+close).focus();await page.keyboard.press("Enter");await closed(page);
    await focusIs(page,opener);
   }
   reports.push({case:"Cancel and close control restore original Plan opener",apply_requests:basic.applyCount()});assert.equal(basic.applyCount(),0);
   await page.locator("#advisor-question").fill("Status?");await page.locator("#advisor-submit").focus();await page.keyboard.press("Enter");
   const proposed=page.locator("#advisor-proposals button");await proposed.waitFor();await open(page,proposed);
   await page.keyboard.press("Escape");await closed(page);await focusIs(page,proposed);reports.push({case:"advisor proposal opener is preserved"});
   for(const detached of [true,false]){
    await open(page);await opener.evaluate((e,remove)=>{if(remove)e.remove();else e.hidden=true;},detached);
    await page.keyboard.press("Escape");await closed(page);await focusIs(page,page.locator("#refresh"));
    await page.keyboard.press("Enter");await opener.waitFor();
   }
   reports.push({case:"detached or hidden opener falls back to current Refresh"});
   await open(page);
   await page.evaluate(()=>{const other=document.createElement("dialog");other.id="other-dialog";const button=document.createElement("button");button.id="other-focus";button.textContent="Other modal";other.append(button);document.body.append(other);other.showModal();document.getElementById("plan-cancel-btn").click();});
   await closed(page);await focusIs(page,page.locator("#other-focus"));
   await page.evaluate(()=>{document.getElementById("other-dialog").close();document.getElementById("other-dialog").remove();});
   reports.push({case:"closing original dialog never focuses behind another open modal"});

   // A lost Apply and recovery dialog exit retain the same identity without replay.
   await open(page);await page.locator("#plan-apply-btn").focus();await page.keyboard.press("Enter");await closed(page);
   await focusIs(page,page.locator("#apply-inspect"));const bookmark=await page.evaluate(k=>sessionStorage.getItem(k),storageKey);assert(bookmark);
   for(const close of ["Escape","plan-cancel-btn"]){
    await open(page,page.locator("#apply-inspect"));
    if(close!=="Escape")await page.locator("#"+close).focus();
    await page.keyboard.press(close==="Escape"?"Escape":"Enter");await closed(page);
    await focusIs(page,page.locator("#apply-inspect"));assert.equal(await page.evaluate(k=>sessionStorage.getItem(k),storageKey),bookmark);
   }
   await page.reload();await page.locator("#apply-inspect:not(:disabled)").waitFor();
   await open(page,page.locator("#apply-inspect"));await page.keyboard.press("Escape");await closed(page);await focusIs(page,page.locator("#apply-inspect"));
   assert.equal(await page.evaluate(k=>sessionStorage.getItem(k),storageKey),bookmark);assert.equal(basic.applyCount(),1);
   reports.push({case:"uncertain Apply, recovery Escape/Cancel and reload preserve key and focus",apply_requests:1});
   await basic.context.close();

   for(const mode of ["stale","success","unauth","held"]){
    let duringApply;
    const f=await fixture(mode),p=f.page;await open(p);await p.locator("#plan-apply-btn").focus();await p.keyboard.press("Enter");
    if(mode==="held"){
     await f.started.promise;await p.keyboard.press("Escape");assert.equal(await p.locator("#plan-dialog").evaluate(e=>e.open),true);
     duringApply=await focusState(p);assert.equal(duringApply.inDialog,true);assert.equal(duringApply.focusVisible,true);
     assert.notEqual(duringApply.outline,"none");assert(await p.evaluate(k=>sessionStorage.getItem(k),storageKey));await f.finish();
    }
    await closed(p);
    if(mode==="unauth"){
     await p.locator("#pairing").waitFor();await focusIs(p,p.locator("#session-check"));
     const bookmark=await p.evaluate(k=>sessionStorage.getItem(k),storageKey);assert(bookmark);f.authorize();await p.keyboard.press("Enter");
     await p.locator("#apply-inspect:not(:disabled)").waitFor();await open(p,p.locator("#apply-inspect"));
     await p.keyboard.press("Escape");await closed(p);await focusIs(p,p.locator("#apply-inspect"));
     assert.equal(await p.evaluate(k=>sessionStorage.getItem(k),storageKey),bookmark);
    }else{await focusIs(p,p.locator("#refresh"));assert.equal(await p.evaluate(k=>sessionStorage.getItem(k),storageKey),null);}
    assert.equal(f.applyCount(),1);reports.push({case:mode+" Apply close and recovery focus",apply_requests:1,...(duringApply?{duringApply}:{})});await f.context.close();
   }
  }
  assert.deepEqual(errors,[]);
  const report={kind:"REAL_EDGE_ALL_HTTP_INTERCEPTED_NO_INSTALLED_HOST_REQUESTS",expected_defect:expectDefect,
   viewport:{width:390,height:844},source_root:source,setup_js_sha256:crypto.createHash("sha256").update(fs.readFileSync(path.join(assets,"setup.js"))).digest("hex"),
   passed:true,scenario_groups:reports.length,reports,page_errors:errors,request_count:requests.length,
   explicit_apply_requests:requests.filter(r=>r.path.endsWith("/apply")).length,automatic_apply:0};
  fs.writeFileSync(path.join(out,"focus-browser-results.json"),JSON.stringify(report,null,2)+"\n");console.log(JSON.stringify(report));
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
