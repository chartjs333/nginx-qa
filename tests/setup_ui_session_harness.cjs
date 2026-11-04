/* Session ordering regressions against unmodified production JS, synthetic HTTP/DOM. */
const assert=require("assert/strict");
const {harness,pause,reply,fixtureSnapshot,planFor,digest}=require("./setup_ui_dom.cjs");
const key="nginx-qa.setup.pending-apply.v1";
const session=(actor,csrf)=>({authenticated:true,actor_id:actor,csrf_token:csrf});
const original=session("operator-original","csrf-original");
const current=session("operator-current","csrf-current");
const projects=project=>({projects:[{project_id:project,name:project}]});
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
const inspectCalls=h=>h.calls.filter(c=>c.path.endsWith("/inspect"));
const sessionCalls=h=>h.calls.filter(c=>c.path==="/api/v1/operator/session");
function view(h){
 return Object.fromEntries(["auth","error","sync","pairing","pairing-code","project","refresh","session-check","apply-inspect","apply-recovery-message"]
  .map(id=>{const e=h.elements[id];return [id,{text:e.textContent,hidden:e.hidden,disabled:e.disabled,value:e.value}];}));
}
async function uncertain(h){
 await h.elements["operations-toolbar"].childNodes[0].dispatch("submit");await pause();
 await h.elements["plan-apply-btn"].dispatch("click");await pause();
 assert(h.storage.getItem(key));
}
const reports=[];
(async()=>{
 // Original reviewer sequence: A is held, B accepts another actor, A arrives late.
 // Also cover an unauthenticated/latest failed session and a stale failure.
 for(const [name,newResponse,oldResponse] of [
  ["new actor",reply(current),reply(original)],
  ["new unauthenticated",reply({authenticated:false,pairing_code:"CURRENT-PAIR"}),reply(original)],
  ["new failure",reply({error:"CURRENT_SESSION_FAILURE"},503),reply(original)],
  ["old unauthenticated",reply(current),reply({authenticated:false,pairing_code:"OLD-PAIR"})],
  ["old failure",reply(current),reply({error:"STALE_SESSION_FAILURE"},503)]
 ]){
  let armed=false,sequence=0;const older=deferred();
  const h=await harness({actor:original.actor_id,route:({path})=>{
   if(armed&&path==="/api/v1/operator/session")return ++sequence===1?older.promise:newResponse;
  }});
  await uncertain(h);const bookmark=h.storage.getItem(key);armed=true;
  const old=h.elements["session-check"].dispatch("click");await pause();
  await h.elements["session-check"].dispatch("click");await pause();const accepted=view(h),calls=h.calls.length;
  assert.equal(h.elements["apply-inspect"].disabled,true);
  older.resolve(oldResponse);await old;await pause();
  assert.deepEqual(view(h),accepted,name);assert.equal(h.calls.length,calls,name);
  await h.elements["apply-inspect"].dispatch("click");await pause();
  assert.equal(inspectCalls(h).length,0);assert.equal(h.storage.getItem(key),bookmark);
  if(name==="new actor"){
   await h.elements.refresh.dispatch("click");await pause();
   assert.equal(h.calls.at(-1).headers["X-Nginx-QA-CSRF"],"csrf-current");
  }
 }
 reports.push("late session success or failure cannot overwrite latest actor, unauthenticated state, error or CSRF");

 // A's session is already accepted, but its project list is delayed past B.
 for(const failure of [false,true]){
  let active=false,next=original;const older=deferred();
  const h=await harness({actor:original.actor_id,route:({path,opts})=>{
   if(!active)return;
   if(path==="/api/v1/operator/session")return reply(next);
   if(path==="/api/v1/setup/projects")return opts.headers["X-Nginx-QA-CSRF"]==="csrf-original"?older.promise:reply(projects("9002"));
  }});
  await uncertain(h);const bookmark=h.storage.getItem(key);active=true;
  const old=h.elements["session-check"].dispatch("click");await pause();next=current;
  await h.elements["session-check"].dispatch("click");await pause();const accepted=view(h),calls=h.calls.length;
  older.resolve(failure?reply({error:"STALE_PROJECT_FAILURE"},403):reply(projects("9001")));await old;await pause();
  assert.deepEqual(view(h),accepted);assert.equal(h.elements.project.value,"9002");assert.equal(h.calls.length,calls);
  assert.equal(h.storage.getItem(key),bookmark);assert.equal(h.elements["apply-inspect"].disabled,true);
 }
 reports.push("delayed project list success and403 cannot change latest project, auth or diagnostics");

 // Detach old in-flight snapshot/history work; its finally must not unlock B's
 // still-pending refresh. A failed old read must not hide B's current data/error.
 for(const stage of ["snapshot","operations"]){
  for(const failure of [false,true]){
   let active=false,next=original;const older=deferred(),newer=deferred();
   const h=await harness({actor:original.actor_id,route:({path,opts,project})=>{
    if(!active)return;
    const csrf=opts.headers["X-Nginx-QA-CSRF"];
    if(path==="/api/v1/operator/session")return reply(next);
    if(path==="/api/v1/setup/projects")return reply(projects(csrf==="csrf-original"?"9001":"9002"));
    if(path.endsWith("/"+stage)&&csrf==="csrf-original")return older.promise;
    if(path.endsWith("/snapshot")&&csrf==="csrf-current")return newer.promise;
   }});
   active=true;const old=h.elements["session-check"].dispatch("click");await pause();next=current;
   const latest=h.elements["session-check"].dispatch("click");await pause();
   assert(h.calls.some(c=>c.path.endsWith("/9002/setup/snapshot")),"new session must not wait for old busy request");
   const waiting=view(h);assert.equal(h.elements.refresh.disabled,true);
   older.resolve(failure?reply({error:"STALE_READ_FAILURE"},503):reply(stage==="snapshot"?fixtureSnapshot("9001"):{operations:[]}));
   await old;await pause();assert.deepEqual(view(h),waiting);
   newer.resolve(reply(fixtureSnapshot("9002")));await latest;await pause();
   assert.equal(h.elements.project.value,"9002");assert.equal(h.elements.refresh.disabled,false);
   assert.equal(h.elements.error.hidden,true);assert.equal(h.elements.sync.textContent,"Диагностика обновлена");
  }
 }
 reports.push("old snapshot/history success, error and finally cannot block or unlock a newer refresh");

 // An old inspection401 must not start another session check after B is accepted.
 // An old advice or plan result must likewise not write into B's context.
 for(const stage of ["inspect","advice","plans"]){
  let active=false;const older=deferred();
  const h=await harness({actor:original.actor_id,route:({path})=>{
   if(active&&path.endsWith("/"+stage))return older.promise;
  }});
  if(stage==="inspect")await uncertain(h);
  const bookmark=h.storage.getItem(key);active=true;
  let old;
  if(stage==="inspect")old=h.elements["apply-inspect"].dispatch("click");
  else if(stage==="advice"){h.elements["advisor-question"].value="Status?";old=h.elements["advisor-form"].dispatch("submit");}
  else old=h.elements["operations-toolbar"].childNodes[0].dispatch("submit");
  await pause();h.authorize(true,current.actor_id);
  await h.elements["session-check"].dispatch("click");await pause();const accepted=view(h),calls=sessionCalls(h).length;
  older.resolve(reply({error:"STALE_ACTION_FAILURE"},401));await old;await pause();
  assert.deepEqual(view(h),accepted);assert.equal(sessionCalls(h).length,calls);
  assert.equal(h.storage.getItem(key),bookmark);assert.equal(h.elements["plan-dialog"].open,false);
 }
 reports.push("superseded inspection/advice/plan failure cannot reset or reauthenticate the latest session");

 // Even the same actor and snapshot digest do not make an older generation's
 // successful advice/plan current. Its pending flag cannot unlock a newer one.
 for(const stage of ["advice","plans"]){
  let active=false,number=0,oldSuccess;const older=deferred(),newer=deferred();
  const h=await harness({actor:original.actor_id,route:({path,body,project})=>{
   if(active&&path.endsWith("/"+stage)){
    if(++number===1){
     oldSuccess=stage==="plans"?planFor(body,100,original.actor_id,project):
      {snapshot_id:digest,explanation:"STALE_ADVICE",claims:[],proposals:[]};
     return older.promise;
    }
    return newer.promise;
   }
  }});
  active=true;
  const begin=()=>{if(stage==="plans")return h.elements["operations-toolbar"].childNodes[0].dispatch("submit");
   h.elements["advisor-question"].value="Status?";return h.elements["advisor-form"].dispatch("submit");};
  const button=()=>stage==="plans"?h.elements["operations-toolbar"].childNodes[0].childNodes.at(-1):h.elements["advisor-submit"];
  const old=begin();await pause();await h.elements["session-check"].dispatch("click");await pause();
  const latest=begin();await pause();const accepted=view(h);assert.equal(button().disabled,true);
  older.resolve(reply(oldSuccess));await old;await pause();
  assert.deepEqual(view(h),accepted);assert.equal(button().disabled,true);
  assert.equal(h.elements["plan-dialog"].open,false);assert.equal(h.elements["advisor-result"].hidden,true);
  newer.resolve(reply({error:"CURRENT_ACTION_FAILURE"},503));await latest;await pause();assert.equal(button().disabled,false);
 }
 reports.push("same-actor stale plan/advice success and finally cannot replace or unlock newer work");
 process.stdout.write(JSON.stringify({passed:true,fixture_only:true,scenarios:reports.length,coverage:reports}));
})().catch(error=>{console.error(error);process.exitCode=1;});
