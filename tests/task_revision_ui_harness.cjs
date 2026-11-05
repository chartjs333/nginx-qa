/* Production task UI with explicit DOM/HTTP fixtures; no service or model. */
const fs=require('fs'),vm=require('vm'),assert=require('assert');
const {Element,pause,reply}=require('./setup_ui_dom.cjs');
const elements={},calls=[],windows={};
for(const m of fs.readFileSync('nginx_qa/static/tasks.html','utf8').matchAll(/<([a-z]+)[^>]*\bid="([^"]+)"/g))elements[m[2]]=new Element(m[1]);
for(const e of Object.values(elements)){e.files=[];e.focus=()=>{};}
const keep={disabled:true};elements['attachment-action'].querySelector=()=>keep;
elements['attachment-action'].value='replace';elements['source-kind'].value='branch';
const oldId='task-'+'a'.repeat(32),newId='task-'+'b'.repeat(32),commit='c'.repeat(40);
const old={task_id:oldId,project_id:'9001',repository_url:'https://github.com/acme/repo.git',source_commit:commit,
  task:'Original request from history',status:'Completed',operation_id:'old-op',plan_digest:'old-plan',attempt:1,
  attachment:{name:'old.xlsx',sha256:'d'.repeat(64),rows:3},assignments:[],reviews:[],effects:[]};
let draft=null,confirmed=true,confirms=0;
const fetch=async(path,opts={})=>{
  const body=opts.body ? JSON.parse(opts.body):null;calls.push({path,body,method:opts.method||'GET'});
  if(path==='/api/v1/operator/session')return reply({authenticated:true,actor_id:'alice',csrf_token:'fixture'});
  if(path==='/api/v1/tasks/projects')return reply({projects:[{project_id:'9001',name:'Test',repository_url:old.repository_url,source_ref:'refs/heads/main'}]});
  if(path==='/api/v1/tasks')return reply({tasks:[old,...(draft?[draft]:[])]});
  if(path.endsWith('/sources'))return reply({branches:[{ref:'refs/heads/main',commit}]});
  if(path===`/api/v1/tasks/${oldId}/revision`)return reply({parent_task_id:oldId,root_task_id:oldId,attempt:2,project_id:'9001',repository_url:old.repository_url,task:old.task,attachment:old.attachment,source:{kind:'commit',value:commit}});
  if(path==='/api/v1/tasks/preview'){
    draft={...old,...body,task_id:newId,parent_task_id:oldId,attempt:2,status:'Draft',operation_id:null,plan_digest:'new-plan'};
    return reply(draft);
  }
  if(path===`/api/v1/tasks/${newId}/start`)return reply({...draft,status:'Running',operation_id:'new-op'});
  if(path===`/api/v1/tasks/${oldId}`)return reply(old);
  if(path===`/api/v1/tasks/${newId}`)return reply(draft);
  throw new Error('Unexpected request: '+path);
};
vm.runInNewContext(fs.readFileSync('nginx_qa/static/tasks.js','utf8'),{
  document:{getElementById:id=>{assert(elements[id],id);return elements[id];},createElement:tag=>new Element(tag)},
  window:{addEventListener:(event,fn)=>windows[event]=fn},location:{hash:'#'+oldId},fetch,
  confirm:()=>{confirms++;return confirmed;},setInterval:()=>{},Uint8Array,btoa:value=>Buffer.from(value,'binary').toString('base64'),console
});
async function event(id,name='click'){await elements[id].dispatch(name);await pause();}
(async()=>{
  await pause();assert(elements.start.disabled);
  elements.task.value='User already typed corrected requirements';
  const selected={name:'updated.xlsx',size:3,arrayBuffer:async()=>new Uint8Array([1,2,3]).buffer};
  elements.attachment.files=[selected];await event('task-form','input');
  await event('refresh');assert.equal(elements.task.value,'User already typed corrected requirements');
  assert.equal(elements.attachment.files[0],selected);
  confirmed=false;await event('revise');assert.equal(calls.filter(c=>c.path.endsWith('/revision')).length,0);
  confirmed=true;await event('revise');assert.equal(confirms,2);
  assert.equal(elements.task.value,'User already typed corrected requirements');assert.equal(elements.attachment.files[0],selected);
  assert(elements.project.disabled);assert(elements.start.disabled);assert.equal(calls.filter(c=>c.path.endsWith('/start')).length,0);
  await event('task-form','submit');
  const preview=calls.find(c=>c.path.endsWith('/preview')).body;
  assert.equal(preview.parent_task_id,oldId);assert.equal(preview.attachment_action,'replace');assert.equal(preview.attachment_name,'updated.xlsx');
  assert.deepEqual(preview.source,{kind:'branch',value:'refs/heads/main'});assert(!elements.start.disabled);
  assert.equal(calls.filter(c=>c.path.endsWith('/start')).length,0);
  elements.task.value='Further unsaved requirements';await event('task-form','input');assert(elements.start.disabled);
  await event('refresh');assert.equal(elements.task.value,'Further unsaved requirements');assert(elements.start.disabled);
  await elements.history.childNodes[0].dispatch('click');await pause();assert.equal(elements.task.value,'Further unsaved requirements');
  // Explicit new form is the only action that clears unsaved editing.
  await event('new-task');assert.equal(elements.task.value,'');assert(!elements.project.disabled);
  await elements.history.childNodes[0].dispatch('click');await pause();await event('revise');
  assert.equal(elements.task.value,old.task);assert.equal(elements['attachment-action'].value,'keep');
  assert.equal(elements['source-kind'].value,'commit');assert.equal(elements['source-value'].value,commit);
  await event('task-form','submit');assert.equal(calls.filter(c=>c.path.endsWith('/preview')).at(-1).body.attachment_action,'keep');
  await event('start');assert.equal(calls.filter(c=>c.path.endsWith('/start')).length,1);
  assert.deepEqual(calls.find(c=>c.path.endsWith('/start')).body,{plan_digest:'new-plan'});
  assert.equal(calls.filter(c=>/admit|prepare|model/.test(c.path)).length,0);
  process.stdout.write(JSON.stringify({passed:true}));
})().catch(e=>{console.error(e);process.exitCode=1;});
