/* Executes production setup.js unchanged with deterministic HTTP and DOM fixtures. */
const fs = require("fs"), vm = require("vm"), assert = require("assert");
class Element {
  constructor(tag) { this.tagName=tag; this.childNodes=[]; this.events={}; this.textContent=""; this.value=""; this.hidden=false; this.disabled=false; this.dataset={}; this.style={}; this.open=false; }
  append(...nodes) { this.childNodes.push(...nodes); if(this.tagName==="select" && !this.value && nodes.length) this.value=nodes[0].value; }
  replaceChildren(...nodes) { this.childNodes=[]; if(this.tagName==="select")this.value=""; this.append(...nodes); }
  addEventListener(name, callback) { (this.events[name] ||= []).push(callback); }
  dispatch(name) { if(this.disabled && name==="click")return; return Promise.all((this.events[name]||[]).map(f=>f({preventDefault(){}}))); }
  showModal(){this.open=true;}
  close(){this.open=false;}
}
const production = fs.readFileSync("nginx_qa/static/setup.js","utf8");
const html = fs.readFileSync("nginx_qa/static/setup.html","utf8");
const pause = async()=>{for(let i=0;i<8;i++)await new Promise(r=>setImmediate(r));};
const digest="c".repeat(64);
function fixtureSnapshot() { return {snapshot_id:digest, authority_fingerprint:"d".repeat(64), generated_at:new Date().toISOString(), checks:[], available_action_ids:["diagnostics.refresh","worker.probe"]}; }
async function harness({authorized=true, projectsError=false, recordedOperations=[]}={}) {
  const elements={};
  for(const match of html.matchAll(/<([a-z]+)[^>]*\bid="([^"]+)"/g))elements[match[2]]=new Element(match[1]);
  const doc={readyState:"complete",getElementById:id=>{assert(elements[id],id);return elements[id];},createElement:tag=>new Element(tag),
    querySelectorAll:selector=>{
      const found=[],seen=new Set();
      function visit(n){if(seen.has(n))return;seen.add(n); if((n.className||"").split(" ").includes(selector.slice(1)))found.push(n);n.childNodes.forEach(visit);}
      Object.values(elements).forEach(visit);return found;
    }};
  const calls=[];let applies=0, failSnapshot=false, failAdvice=false;
  const records=recordedOperations.map(r=>({...r}));
  const fetch=async(path,opts={})=>{
    const body=opts.body?JSON.parse(opts.body):null;
    calls.push({path,method:opts.method||"GET",headers:opts.headers,body});
    const reply=(data,status=200)=>({ok:status===200,status,json:async()=>data});
    if(path==="/api/v1/operator/session")return reply({authenticated:authorized,csrf_token:authorized?"fixture-csrf":null,pairing_code:authorized?null:"PAIR-1234"});
    if(path==="/api/v1/setup/projects")return projectsError?reply({error:"NO_GRANT"},403):reply({projects:[{project_id:"9001",name:"test"}]});
    if(path.endsWith("/setup/snapshot"))return failSnapshot?reply({error:"UNAVAILABLE"},503):reply(fixtureSnapshot());
    if(path.endsWith("/setup/operations"))return reply({schema_version:1,kind:"setup_operation_list",operations:records,projection:"recorded"});
    if(path.endsWith("/setup/plans"))return reply({plan_id:"plan-1",plan_digest:"e".repeat(64),project_id:"9001",snapshot_id:digest,action_id:body.action_id,arguments:body.arguments,resources:[],preconditions:[],effects:[],expires_at:new Date().toISOString(),confirmation_required:true});
    if(path.endsWith("/apply")){
      if(++applies===1)throw new Error("network outcome uncertain");
      const receipt={operation_id:"operation-1",state:"APPLYING",action_id:"diagnostics.refresh"};
      records.unshift(receipt);
      return reply(receipt);
    }
    if(path.includes("/setup/operations/"))return reply({operation_id:"operation-1",state:"BLOCKED",action_id:"diagnostics.refresh"});
    if(path.endsWith("/setup/advice")){
      if(failAdvice)throw new Error("network outcome uncertain");
      return reply({snapshot_id:digest,explanation:"Детерминированный диагностический отчёт",claims:[],proposals:[]});
    }
    throw new Error("unexpected request "+path);
  };
  vm.runInNewContext(production,{document:doc,fetch,window:{crypto:require("crypto").webcrypto},TextEncoder,console,Date,Math});
  await pause();
  return {elements,calls,doc,failSnapshot:()=>{failSnapshot=true;},failAdvice:v=>{failAdvice=v;}};
}
(async()=>{
  const anon=await harness({authorized:false});
  assert.equal(anon.calls.length,1);
  assert.equal(anon.calls[0].method,"GET");
  assert.equal(anon.elements["pairing-code"].textContent,"PAIR-1234");
  assert.equal(anon.elements["advisor-submit"].disabled,true);
  const denied=await harness({projectsError:true});
  assert.equal(denied.calls.some(c=>c.path.includes("/projects/9000/")),false);
  assert.equal(denied.calls.some(c=>c.path.endsWith("/snapshot")),false);

  // A fresh page retrieves durable receipts without replaying an action.
  const reloaded=await harness({recordedOperations:[{operation_id:"operation-1",state:"APPLYING",action_id:"selftest.start"}]});
  assert.equal(reloaded.elements["receipts-list"].childNodes.length,1);
  const savedCard=reloaded.elements["receipts-list"].childNodes[0];
  assert(savedCard.childNodes[0].textContent.includes("APPLYING"));
  assert(savedCard.childNodes.some(n=>n.textContent.includes("Сохранённое состояние")));
  assert(reloaded.calls.some(c=>c.path.endsWith("/setup/operations")&&c.method==="GET"));
  assert(reloaded.calls.every(c=>c.method==="GET"));
  await savedCard.childNodes.at(-1).dispatch("click");await pause();
  assert(reloaded.elements["receipts-list"].childNodes[0].childNodes[0].textContent.includes("BLOCKED"));
  assert(reloaded.calls.every(c=>c.method==="GET"));

  const app=await harness();
  assert.equal(app.calls[0].method,"GET");
  assert.equal(app.calls[1].headers["X-Nginx-QA-CSRF"],"fixture-csrf");
  assert.equal(app.elements.project.value,"9001");
  assert.equal(app.elements["operations-toolbar"].childNodes.length,2);
  await app.elements["operations-toolbar"].childNodes[0].dispatch("submit"); await pause();
  const planCall=app.calls.find(c=>c.path.endsWith("/setup/plans"));
  assert.deepEqual(Object.keys(planCall.body).sort(),["schema_version","snapshot_id","action_id","arguments"].sort());
  assert.equal(planCall.body.snapshot_id,digest);
  assert.equal(app.elements["plan-dialog"].open,true);
  await app.elements["plan-apply-btn"].dispatch("click");await pause();
  assert.equal(app.elements["plan-dialog"].open,true);
  await app.elements["plan-apply-btn"].dispatch("click");await pause();
  const applied=app.calls.filter(c=>c.path.endsWith("/apply"));
  assert.equal(applied.length,2);
  assert.deepEqual(applied[0].body,applied[1].body);
  assert.deepEqual(Object.keys(applied[0].body).sort(),["schema_version","plan_digest","confirmed","idempotency_key"].sort());
  assert.equal(applied[0].headers["X-Nginx-QA-CSRF"],"fixture-csrf");
  assert.equal(applied[0].body.confirmed,true);
  assert.equal(app.elements["plan-dialog"].open,false);
  const receiptCard=app.elements["receipts-list"].childNodes[0];
  assert(receiptCard.childNodes[0].textContent.includes("APPLYING"));
  assert(!receiptCard.childNodes[0].textContent.includes("успеш"));
  await receiptCard.childNodes.at(-1).dispatch("click");await pause();
  assert(app.elements["receipts-list"].childNodes[0].childNodes[0].textContent.includes("BLOCKED"));

  app.elements["advisor-question"].value="Status?";
  app.failAdvice(true);
  await app.elements["advisor-form"].dispatch("submit");await pause();
  app.failAdvice(false);
  await app.elements["advisor-form"].dispatch("submit");await pause();
  const advice=app.calls.filter(c=>c.path.endsWith("/advice"));
  assert.equal(advice.length,2);
  assert.deepEqual(advice[0].body,advice[1].body);
  app.failSnapshot();
  await app.elements.refresh.dispatch("click");await pause();
  assert.equal(app.elements["advisor-submit"].disabled,true);
  assert.equal(app.elements["advisor-result"].hidden,true);
  assert.equal(app.elements["operations-toolbar"].childNodes.length,0);
  process.stdout.write(JSON.stringify({passed:true,scenarios:9,plan_request:planCall.body,apply_request:applied[0].body}));
})().catch(err=>{console.error(err);process.exitCode=1;});
