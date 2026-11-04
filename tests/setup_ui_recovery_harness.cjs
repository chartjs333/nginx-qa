/* Defect probes: real production JS; explicitly synthetic HTTP/DOM fault cases. */
const assert=require("assert/strict");
const {harness,pause,reply,memoryStorage,receiptFor,fixtureSnapshot}=require("./setup_ui_dom.cjs");
const storageKey="nginx-qa.setup.pending-apply.v1";
const count=(app,suffix)=>app.calls.filter(c=>c.path.endsWith(suffix)).length;
async function plan(app){await app.elements["operations-toolbar"].childNodes[0].dispatch("submit");await pause();assert(app.elements["plan-dialog"].open);}
async function apply(app){await app.elements["plan-apply-btn"].dispatch("click");await pause();}
async function inspect(app){await app.elements["apply-inspect"].dispatch("click");await pause();}
const reports=[];
(async()=>{
 // A bound durable stale receipt permits a fresh plan; a status code alone does not.
 const stale=await harness({route:({path,plan,server})=>{
  if(path.endsWith("/apply")){
   const receipt=receiptFor(plan,{state:"BLOCKED",error_code:"PLAN_STALE"});
   server.records.unshift(receipt);
   return reply({detail:{error:"PLAN_STALE",apply_outcome:"NOT_STARTED",plan_id:plan.plan_id,plan_digest:plan.plan_digest,receipt}},409);
  }
 }});
 await plan(stale);await apply(stale);
 assert.equal(stale.storage.getItem(storageKey),null);assert.equal(stale.elements["plan-dialog"].open,false);
 assert.equal(stale.elements.refresh.disabled,false);assert.equal(stale.elements["session-check"].disabled,false);
 assert(stale.elements.error.textContent.includes("не выполнялся"));
 await plan(stale);assert.equal(count(stale,"/plans"),2);assert.equal(count(stale,"/apply"),1);
 reports.push("bound durable stale receipt restores fresh-plan flow");

 // Lost original is still before its intent: an absent inspection is not cancellation.
 let lateOriginal;
 const uncertain=await harness({route:({path,body,plan,server})=>{
  if(path.endsWith("/apply")){
   server.applies++;
   const previous=server.records.find(r=>r.plan_digest===body.plan_digest);
   if(previous)return reply({...previous,deduplicated:true});
   lateOriginal=()=>{server.effects++;server.records.push(receiptFor(plan,{state:"SUCCEEDED"}));};
   throw new Error("lost before intent");
  }
 }});
 await plan(uncertain);await apply(uncertain);
 const original=uncertain.storage.getItem(storageKey);assert(original);
 assert.equal(uncertain.elements.refresh.disabled,false);
 await inspect(uncertain);assert.equal(uncertain.elements["plan-dialog"].open,true);
 assert.equal(uncertain.storage.getItem(storageKey),original);
 await uncertain.elements["plan-dialog"].dispatch("cancel");await pause();
 assert.equal(uncertain.storage.getItem(storageKey),original);
 assert.equal(uncertain.elements["plan-dialog"].open,false);
 const reloaded=await harness({storage:uncertain.storage,server:uncertain.server});
 assert.equal(count(reloaded,"/apply"),0);assert.equal(count(reloaded,"/plans"),0);
 assert.equal(reloaded.elements["apply-recovery"].hidden,false);
 lateOriginal();
 await inspect(reloaded);assert.equal(reloaded.storage.getItem(storageKey),null);
 assert.equal(uncertain.server.effects,1);assert.equal(count(reloaded,"/apply"),0);
 reports.push("lost-before-intent absence, Escape and reload retain key; delayed effect reconciles once");

 // A first-response403 can conceal an already-committed effect.
 for(const failure of ["403","malformed200"]){
  const app=await harness({route:({path,plan,server})=>{
   if(path.endsWith("/apply")){server.effects++;server.records.push(receiptFor(plan,{state:"SUCCEEDED"}));
    return failure==="403"?reply({detail:{error:"SETUP_ACCESS_DENIED"}},403):reply(null);}
  }});
  await plan(app);await apply(app);assert(app.storage.getItem(storageKey));assert.equal(app.server.effects,1);
  assert.equal(app.elements["session-check"].disabled,false);
  await inspect(app);assert.equal(app.storage.getItem(storageKey),null);assert.equal(app.server.effects,1);assert.equal(count(app,"/apply"),1);
 }
 reports.push("post-effect first403 and malformed success preserve attempt and recover without another effect");

 // Unbound/non-durable error bodies never authorize dropping the original key.
 for(const error of [
  reply({detail:{error:"PLAN_STALE"}},409),
  reply({detail:{error:"SETUP_CONTEXT_STALE"}},409),
  reply({detail:{error:"DIAGNOSTIC_CAPTURE_UNAVAILABLE"}},429),
  reply({detail:{error:"OPERATOR_SESSION_REQUIRED"}},401),
  reply({detail:{error:"PLAN_STALE",apply_outcome:"NOT_STARTED",plan_id:"other",plan_digest:"f".repeat(64)}},409),
  reply({error:"UNAVAILABLE"},503)
 ]){
  const app=await harness({route:({path})=>path.endsWith("/apply")?error:undefined});
  await plan(app);await apply(app);assert(app.storage.getItem(storageKey));
  assert.equal(app.elements["plan-dialog"].open,false);assert.equal(app.elements["session-check"].disabled,false);
  await app.elements.refresh.dispatch("click");await pause();assert(app.storage.getItem(storageKey));
  assert.equal(count(app,"/apply"),1);assert.equal(app.elements["operations-toolbar"].childNodes[0].childNodes.at(-1).disabled,true);
 }
 reports.push("unbound stale/context/busy/auth/server errors retain identity while read-only recovery stays available");

 // Current actor and action permission are checked independently from persisted identity.
 let allowRetry=false;
 const revoked=await harness({route:({path,plan})=>{
  if(path.endsWith("/apply"))return reply({detail:{error:"SETUP_ACCESS_DENIED"}},403);
  if(path.endsWith("/inspect"))return reply({kind:"setup_apply_inspection",schema_version:1,plan,receipt:null,retry_permitted:allowRetry});
 }});
 await plan(revoked);await apply(revoked);const saved=revoked.storage.getItem(storageKey);
 revoked.authorize(false);await revoked.elements["session-check"].dispatch("click");await pause();
 assert.equal(revoked.elements.pairing.hidden,false);assert.equal(revoked.elements["apply-inspect"].disabled,true);
 revoked.authorize(true,"operator-other");await revoked.elements["session-check"].dispatch("click");await pause();
 assert.equal(revoked.elements["apply-inspect"].disabled,true);assert.equal(revoked.storage.getItem(storageKey),saved);
 revoked.authorize(true,"operator-fixture");await revoked.elements["session-check"].dispatch("click");await pause();
 await inspect(revoked);assert.equal(revoked.elements["plan-apply-btn"].disabled,true);
 await revoked.elements["plan-cancel-btn"].dispatch("click");await pause();assert.equal(revoked.storage.getItem(storageKey),saved);
 allowRetry=true;await inspect(revoked);assert.equal(revoked.elements["plan-apply-btn"].disabled,false);
 reports.push("reauth same actor and current retry grant; denied grant never clears bookmark");

 // Read-only navigation may proceed, but its stale inspection reply cannot reopen a modal.
 let resolveInspection;
 const navigating=await harness({route:({path,plan})=>{
  if(path.endsWith("/apply"))throw new Error("lost");
  if(path.endsWith("/inspect"))return new Promise(resolve=>{resolveInspection=()=>resolve(reply({kind:"setup_apply_inspection",schema_version:1,plan,receipt:null,retry_permitted:true}));});
 }});
 await plan(navigating);await apply(navigating);const pending=navigating.elements["apply-inspect"].dispatch("click");await pause();
 navigating.elements.project.value="9002";await navigating.elements.project.dispatch("change");await pause();
 resolveInspection();await pending;await pause();
 assert.equal(navigating.elements["plan-dialog"].open,false);assert(navigating.storage.getItem(storageKey));assert.equal(count(navigating,"/apply"),1);
 reports.push("late inspection cannot replace navigated project context");

 // Escape during in-flight Apply cannot discard or replace the durable bookmark.
 let finishApply;
 const running=await harness({route:({path,plan})=>path.endsWith("/apply")?new Promise(resolve=>{finishApply=()=>resolve(reply(receiptFor(plan,{state:"SUCCEEDED"})));}):undefined});
 await plan(running);const applying=running.elements["plan-apply-btn"].dispatch("click");await pause();
 const runningKey=running.storage.getItem(storageKey);assert(runningKey);
 await running.elements["plan-dialog"].dispatch("cancel");await pause();
 assert.equal(running.elements["plan-dialog"].open,true);assert.equal(running.storage.getItem(storageKey),runningKey);
 finishApply();await applying;await pause();assert.equal(running.storage.getItem(storageKey),null);
 reports.push("Escape during in-flight Apply does not abandon request");

 // Durable nonterminal receipts still forbid a fresh-key effect. Only the
 // current status of the exact operation bound by Apply/inspection can resolve it.
 for(const initialState of ["APPLYING","UNKNOWN","PLANNED"]){
  const app=await harness({route:({path,plan,server})=>{
   if(path.endsWith("/apply")){server.records.push(receiptFor(plan,{state:initialState}));return reply(server.records[0]);}
  }});
  await plan(app);await apply(app);const saved=app.storage.getItem(storageKey);assert(saved);
  await inspect(app);assert.equal(app.storage.getItem(storageKey),saved);assert.equal(app.elements["plan-dialog"].open,false);
  assert.equal(app.elements["operations-toolbar"].childNodes[0].childNodes.at(-1).disabled,true);
  await app.elements["receipts-list"].childNodes[0].childNodes.at(-1).dispatch("click");await pause();
  assert.equal(app.storage.getItem(storageKey),null);assert.equal(count(app,"/apply"),1);
 }
 reports.push("nonterminal receipts retain original key until exact bound current status is terminal");

 // Native APPLYING is an acknowledged running job, so its owned Stop remains
 // available even when current policy no longer grants Start.
 let allowed=["selftest.start","selftest.stop","recovery.open"];
 const native=await harness({route:({path,plan,server,project})=>{
  if(path.endsWith("/setup/snapshot"))return reply({...fixtureSnapshot(project),available_action_ids:allowed});
  if(path.endsWith("/apply")){
   server.effects++;
   const receipt=receiptFor(plan,{operation_id:"operation-"+server.effects,state:plan.action_id==="selftest.start"?"APPLYING":"SUCCEEDED",
    native_operation_id:"native-owned",native_sprint_id:"sprint-owned"});
   server.records.unshift(receipt);allowed=["selftest.stop","recovery.open"];return reply(receipt);
  }
 }});
 let form=native.elements["operations-toolbar"].childNodes[0];form.childNodes[1].childNodes[0].value="small-docs-selftest";
 await form.dispatch("submit");await pause();await apply(native);
 assert.equal(native.storage.getItem(storageKey),null);
 form=native.elements["operations-toolbar"].childNodes[0];assert.equal(form.childNodes[0].textContent,"Остановить самопроверку");
 assert.equal(form.childNodes.at(-1).disabled,false);form.childNodes[1].childNodes[0].value="native-owned";
 await form.dispatch("submit");await pause();await apply(native);
 const nativePlans=native.calls.filter(c=>c.path.endsWith("/plans"));
 assert.deepEqual(nativePlans.map(c=>c.body.action_id),["selftest.start","selftest.stop"]);
 assert.deepEqual(nativePlans[1].body.arguments,{self_test_id:"native-owned"});
 assert.equal(native.server.effects,2);assert.equal(native.storage.getItem(storageKey),null);
 reports.push("acknowledged native APPLYING allows owned Stop under revoked Start permission without duplicate Start");

 // The original plan and receipt must come from the authenticated inspection;
 // a foreign plan, malformed receipt, or delayed previous session cannot unlock.
 for(const bad of ["plan","receipt","session"]){
  let resolveReply;
  const app=await harness({route:({path,plan})=>{
   if(path.endsWith("/apply"))throw new Error("lost");
   if(path.endsWith("/inspect")){
    const result={kind:"setup_apply_inspection",schema_version:1,plan,receipt:receiptFor(plan,{state:"SUCCEEDED"}),retry_permitted:true};
    if(bad==="plan")result.plan={...plan,actor_id:"operator-other"};
    if(bad==="receipt")result.receipt={...result.receipt,plan_digest:"e".repeat(64)};
    if(bad==="session")return new Promise(resolve=>{resolveReply=()=>resolve(reply(result));});
    return reply(result);
   }
  }});
  await plan(app);await apply(app);const saved=app.storage.getItem(storageKey);
  const pending=app.elements["apply-inspect"].dispatch("click");await pause();
  if(bad==="session"){
   app.authorize(true,"operator-other");await app.elements["session-check"].dispatch("click");await pause();resolveReply();
  }
  await pending;await pause();assert.equal(app.storage.getItem(storageKey),saved);
  assert.equal(app.elements["plan-dialog"].open,false);assert.equal(count(app,"/apply"),1);
 }
 reports.push("foreign plan/receipt and late previous-session inspection cannot resolve an attempt");

 // The durable-before-send boundary fails closed; corrupt state is not silently erased.
 const brokenStorage=memoryStorage();brokenStorage.setItem=()=>{throw new Error("denied");};
 const noStorage=await harness({storage:brokenStorage});await plan(noStorage);await apply(noStorage);assert.equal(count(noStorage,"/apply"),0);
 const corrupt=memoryStorage();corrupt.setItem(storageKey,'{"invalid":true}');
 const invalid=await harness({storage:corrupt});assert.equal(invalid.elements["apply-recovery"].hidden,false);
 assert.equal(invalid.elements["operations-toolbar"].childNodes[0].childNodes.at(-1).disabled,true);
 assert.equal(count(invalid,"/apply"),0);assert.equal(corrupt.getItem(storageKey),'{"invalid":true}');
 reports.push("storage failure before send and corrupt stored context fail closed");

 process.stdout.write(JSON.stringify({passed:true,scenarios:reports.length,coverage:reports}));
})().catch(error=>{console.error(error);process.exitCode=1;});
