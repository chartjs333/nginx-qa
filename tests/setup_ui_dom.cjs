/* Runs the shipped UI without rewriting it; HTTP and DOM are explicit fixtures. */
const fs = require("fs"), vm = require("vm"), assert = require("assert");
const crypto = require("crypto");
class Element {
  constructor(tag) { this.tagName=tag; this.childNodes=[]; this.events={}; this.textContent=""; this.value=""; this.hidden=false; this.disabled=false; this.dataset={}; this.style={}; this.open=false; }
  append(...nodes) { this.childNodes.push(...nodes); if(this.tagName==="select" && !this.value && nodes.length)this.value=nodes[0].value; }
  replaceChildren(...nodes) { this.childNodes=[]; if(this.tagName==="select")this.value=""; this.append(...nodes); }
  addEventListener(name, callback) { (this.events[name] ||= []).push(callback); }
  dispatch(name) { if(this.disabled && name==="click")return; return Promise.all((this.events[name]||[]).map(f=>f({preventDefault(){}}))); }
  showModal(){this.open=true;} close(){this.open=false;}
}
const pause = async()=>{for(let i=0;i<12;i++)await new Promise(r=>setImmediate(r));};
const digest="c".repeat(64);
const reply=(data,status=200)=>({ok:status>=200&&status<300,status,json:async()=>data});
function fixtureSnapshot(project="9001") { return {snapshot_id:digest,project_id:project,authority_fingerprint:"d".repeat(64),generated_at:new Date().toISOString(),checks:[],available_action_ids:["diagnostics.refresh","worker.probe"]}; }
function memoryStorage() { const map=new Map(); return {map,getItem:k=>map.has(k)?map.get(k):null,setItem:(k,v)=>map.set(k,v),removeItem:k=>map.delete(k)}; }
function planFor(body,index,actor,project) {
  return {kind:"setup_action_plan",schema_version:1,plan_id:"plan-"+index,plan_digest:crypto.createHash("sha256").update("plan-"+index).digest("hex"),
    actor_id:actor,project_id:project,target_project_id:null,snapshot_id:body.snapshot_id,action_id:body.action_id,arguments:body.arguments,
    resources:[],preconditions:[],required_permissions:["read"],effects:["Isolated fixture action"],authority_fingerprint:"d".repeat(64),
    created_at:new Date().toISOString(),expires_at:new Date(Date.now()+120000).toISOString(),confirmation_required:true};
}
function receiptFor(plan,overrides={}) {
  return {kind:"setup_operation_receipt",schema_version:1,operation_id:"operation-1",state:"APPLYING",action_id:plan.action_id,
    plan_id:plan.plan_id,plan_digest:plan.plan_digest,project_id:plan.project_id,created_at:new Date().toISOString(),updated_at:new Date().toISOString(),
    native_operation_id:null,native_sprint_id:null,evidence_ids:[],error_code:null,retryable:false,deduplicated:false,...overrides};
}
async function harness({authorized=true,actor="operator-fixture",projectsError=false,recordedOperations=[],storage=memoryStorage(),
                        route,server:sharedServer,source}={}) {
  const elements={};
  const html=fs.readFileSync("nginx_qa/static/setup.html","utf8");
  for(const match of html.matchAll(/<([a-z]+)[^>]*\bid="([^"]+)"/g))elements[match[2]]=new Element(match[1]);
  const doc={readyState:"complete",getElementById:id=>{assert(elements[id],id);return elements[id];},createElement:tag=>new Element(tag),
    querySelectorAll:selector=>{
      const found=[],seen=new Set();
      function visit(n){if(seen.has(n))return;seen.add(n);if((n.className||"").split(" ").includes(selector.slice(1)))found.push(n);n.childNodes.forEach(visit);}
      Object.values(elements).forEach(visit);return found;
    }};
  const server=sharedServer||{plans:new Map(),records:recordedOperations.map(r=>({...r})),applies:0,effects:0,nextPlan:0};
  const calls=[];let failSnapshot=false,failAdvice=false;
  const fetch=async(path,opts={})=>{
    const body=opts.body?JSON.parse(opts.body):null;
    calls.push({path,method:opts.method||"GET",headers:opts.headers,body});
    const project=(path.match(/\/projects\/([0-9]+)\//)||[])[1]||"9001";
    const plan=body&&server.plans.get(body.plan_digest);
    if(route) {const value=await route({path,opts,body,project,plan,server,reply,receiptFor});if(value!==undefined)return value;}
    if(path==="/api/v1/operator/session")return reply({authenticated:authorized,actor_id:authorized?actor:null,csrf_token:authorized?"fixture-csrf":null,pairing_code:authorized?null:"PAIR-1234"});
    if(path==="/api/v1/setup/projects")return projectsError?reply({error:"NO_GRANT"},403):reply({projects:[{project_id:"9001",name:"test"},{project_id:"9002",name:"other"}]});
    if(path.endsWith("/setup/snapshot"))return failSnapshot?reply({error:"UNAVAILABLE"},503):reply(fixtureSnapshot(project));
    if(path.endsWith("/setup/operations"))return reply({schema_version:1,kind:"setup_operation_list",operations:server.records,projection:"recorded"});
    if(path.endsWith("/setup/plans")){const p=planFor(body,++server.nextPlan,actor,project);server.plans.set(p.plan_digest,p);return reply(p);}
    if(path.endsWith("/inspect"))return reply({kind:"setup_apply_inspection",schema_version:1,plan,receipt:server.records.find(r=>r.plan_digest===body.plan_digest)||null,retry_permitted:true});
    if(path.endsWith("/apply")){
      if(++server.applies===1)throw new Error("network outcome uncertain");
      const receipt=receiptFor(plan);server.records.unshift(receipt);return reply(receipt);
    }
    if(path.includes("/setup/operations/"))return reply({...server.records[0],state:"BLOCKED"});
    if(path.endsWith("/setup/advice")){if(failAdvice)throw new Error("network outcome uncertain");return reply({snapshot_id:digest,explanation:"Детерминированный диагностический отчёт",claims:[],proposals:[]});}
    throw new Error("unexpected request "+path);
  };
  vm.runInNewContext(source||fs.readFileSync("nginx_qa/static/setup.js","utf8"),{document:doc,fetch,window:{crypto:crypto.webcrypto,sessionStorage:storage},TextEncoder,console,Date,Math});
  await pause();
  return {elements,calls,doc,server,storage,failSnapshot:()=>{failSnapshot=true;},failAdvice:v=>{failAdvice=v;},
    authorize:(value,nextActor=actor)=>{authorized=value;actor=nextActor;}};
}
module.exports={Element,pause,digest,reply,fixtureSnapshot,memoryStorage,planFor,receiptFor,harness};
