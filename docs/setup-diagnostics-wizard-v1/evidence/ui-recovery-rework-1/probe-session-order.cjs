const fs=require('fs'), path=require('path'), assert=require('assert');
const root=__dirname;
process.chdir(path.join(root,'export'));
const {harness,reply,pause}=require(path.join(root,'export/tests/setup_ui_dom.cjs'));
const key='nginx-qa.setup.pending-apply.v1';
(async()=>{
  let hold=false, olderResolve, sessionNumber=0;
  const h=await harness({actor:'operator-original',route:({path})=>{
    if(path==='/api/v1/operator/session' && hold){
      sessionNumber++;
      if(sessionNumber===1) return new Promise(r=>olderResolve=r);
      return reply({authenticated:true,actor_id:'operator-current',csrf_token:'csrf-current'});
    }
  }});
  await h.elements['operations-toolbar'].childNodes[0].dispatch('submit'); await pause();
  await h.elements['plan-apply-btn'].dispatch('click'); await pause();
  assert(h.storage.getItem(key), 'ambiguous first Apply must retain the original key');
  const original=h.storage.getItem(key);
  hold=true;
  const old=h.elements['session-check'].dispatch('click'); await pause();
  h.authorize(true,'operator-current');
  await h.elements['session-check'].dispatch('click'); await pause();
  const current={inspect_disabled:h.elements['apply-inspect'].disabled,message:h.elements['apply-recovery-message'].textContent};
  assert.equal(current.inspect_disabled,true,'latest actor must not inspect original actor attempt');
  olderResolve(reply({authenticated:true,actor_id:'operator-original',csrf_token:'csrf-original'}));
  await old; await pause();
  const stale={inspect_disabled:h.elements['apply-inspect'].disabled,message:h.elements['apply-recovery-message'].textContent};
  // Observe which stale credential the production code uses after the old response.
  await h.elements['apply-inspect'].dispatch('click'); await pause();
  const inspect=h.calls.filter(c=>c.path.endsWith('/inspect')).at(-1);
  const finding={tested_sha:'7a39bd90eae245f68da902d3d6974a8b76100b14',fixture_only:true,
    latest_authoritative_actor:'operator-current',before_late_response:current,after_late_response:stale,
    inspection_sent:!!inspect,inspection_csrf:inspect?.headers?.['X-Nginx-QA-CSRF'],
    original_key_preserved:h.storage.getItem(key)===original,
    finding:'An older session GET overwrites the newer operator and re-enables the original operator attempt. Backend authorization still guards actual requests; this is a client session ordering defect.'};
  assert.equal(stale.inspect_disabled,false,'reproducer expects stale response to re-enable old actor inspection');
  assert.equal(finding.inspection_csrf,'csrf-original');
  fs.writeFileSync(path.join(root,'probe-session-order.json'),JSON.stringify(finding,null,2)+'\n');
  console.log(JSON.stringify(finding));
})().catch(e=>{console.error(e);process.exit(1);});
