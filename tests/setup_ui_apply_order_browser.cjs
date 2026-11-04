/* Real Edge, reordered HTTP fixtures; no installed host or real Apply effect. */
const fs=require("fs"),path=require("path"),assert=require("assert/strict"),crypto=require("crypto");
const {chromium}=require(process.env.SDW_PLAYWRIGHT_MODULE||"C:/nqt37/qualification/browser-tests/node_modules/playwright");
const source=path.resolve(process.argv[2]||"."),out=path.resolve(process.argv[3]),expectedDefect=process.argv.includes("--expect-defect");
const {fixtureSnapshot,planFor,receiptFor}=require(path.join(source,"tests/setup_ui_dom.cjs"));
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
const settle=page=>page.evaluate(()=>new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r))));
(async()=>{
 fs.mkdirSync(out,{recursive:true});const results=[],errors=[];
 const browser=await chromium.launch({headless:true,executablePath:process.env.SDW_BROWSER_EXECUTABLE||"C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"});
 try{
  for(const scenario of ["new-session","new-work"]){
   const held=deferred(),release=deferred();let applied=false,holdUsed=false,index=0,actor="operator-original",csrf="csrf-original",applyCount=0;
   const plans=new Map();const context=await browser.newContext({viewport:{width:390,height:844}}),page=await context.newPage();page.setDefaultTimeout(10000);
   page.on("pageerror",error=>errors.push(error.message));
   await context.route("**/*",async route=>{
    const request=route.request(),url=new URL(request.url()),p=url.pathname;assert.equal(url.origin,"https://apply-order-fixture.invalid");
    const json=(body,status=200)=>route.fulfill({status,contentType:"application/json",body:JSON.stringify(body)});
    if(p==="/setup")return route.fulfill({contentType:"text/html",body:fs.readFileSync(path.join(source,"nginx_qa/static/setup.html"))});
    if(["/setup/assets/setup.js","/setup/assets/setup.css"].includes(p))return route.fulfill({contentType:p.endsWith(".js")?"text/javascript":"text/css",body:fs.readFileSync(path.join(source,"nginx_qa/static",path.basename(p)))});
    if(p==="/api/v1/operator/session")return json({authenticated:true,actor_id:actor,csrf_token:csrf});
    if(p==="/api/v1/setup/projects")return json({projects:[{project_id:"9001",name:"Ordering fixture"}]});
    if(p.endsWith("/snapshot")){
     if(applied&&!holdUsed&&scenario==="new-session"){holdUsed=true;held.resolve();await release.promise;}
     return json({...fixtureSnapshot("9001"),available_action_ids:["setup.open"]});
    }
    if(p.endsWith("/operations")){
     if(applied&&!holdUsed&&scenario==="new-work"){holdUsed=true;held.resolve();await release.promise;}
     return json({operations:[]});
    }
    if(p.endsWith("/plans")){const plan=planFor(request.postDataJSON(),++index,actor,"9001");plans.set(plan.plan_digest,plan);return json(plan);}
    if(p.endsWith("/apply")){
     applied=true;applyCount++;const plan=plans.get(request.postDataJSON().plan_digest),receipt=receiptFor(plan,{state:"BLOCKED",error_code:"PLAN_STALE"});
     return json({detail:{error:"PLAN_STALE",apply_outcome:"NOT_STARTED",plan_id:plan.plan_id,plan_digest:plan.plan_digest,receipt}},409);
    }
    return route.abort("blockedbyclient");
   });
   await page.goto("https://apply-order-fixture.invalid/setup");const planButton=page.locator("#operations-toolbar button");await planButton.waitFor();
   await planButton.focus();await page.keyboard.press("Enter");await page.locator("#plan-dialog").waitFor({state:"visible"});
   await page.locator("#plan-apply-btn").focus();await page.keyboard.press("Enter");await held.promise;await page.locator("#plan-dialog").waitFor({state:"hidden"});
   let desiredFocus;
   if(scenario==="new-session"){
    actor="operator-current";csrf="csrf-current";await page.locator("#session-check").focus();await page.keyboard.press("Enter");
    await page.locator("#refresh:not(:disabled)").waitFor();assert.equal(await page.locator("#error").isHidden(),true);desiredFocus="session-check";
   }else{
    // The new snapshot has rendered another plan button while old history is
    // still pending. Actual keyboard interaction creates/cancels a new plan.
    await planButton.focus();await page.keyboard.press("Enter");await page.locator("#plan-dialog").waitFor({state:"visible"});
    await page.keyboard.press("Escape");await page.locator("#plan-dialog").waitFor({state:"hidden"});
    assert.equal(await planButton.evaluate(e=>document.activeElement===e),true);desiredFocus="new-plan-button";
   }
   const before=await page.evaluate(()=>({error:document.getElementById("error").textContent,errorHidden:document.getElementById("error").hidden,focusId:document.activeElement.id,focusText:document.activeElement.textContent}));
   release.resolve();await settle(page);await page.waitForFunction(()=>!document.getElementById("refresh").disabled);await settle(page);
   const after=await page.evaluate(()=>({error:document.getElementById("error").textContent,errorHidden:document.getElementById("error").hidden,focusId:document.activeElement.id,focusText:document.activeElement.textContent}));
   const focusRetained=scenario==="new-session"?after.focusId===desiredFocus:await planButton.evaluate(e=>document.activeElement===e);
   if(expectedDefect){
    if(scenario==="new-session")assert.equal(after.errorHidden,false);else assert.equal(focusRetained,false);
   }else{assert.deepEqual(after,before);assert.equal(focusRetained,true);}
   assert.equal(applyCount,1);results.push({scenario,before,after,focusRetained,explicit_apply_requests:applyCount});await context.close();
  }
  assert.deepEqual(errors,[]);
  const report={kind:"REAL_EDGE_ALL_HTTP_INTERCEPTED",expected_defect:expectedDefect,passed:true,source_root:source,
   setup_js_sha256:crypto.createHash("sha256").update(fs.readFileSync(path.join(source,"nginx_qa/static/setup.js"))).digest("hex"),
   results,page_errors:errors,automatic_apply:0};
  fs.writeFileSync(path.join(out,"apply-order-results.json"),JSON.stringify(report,null,2)+"\n");console.log(JSON.stringify(report));
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
