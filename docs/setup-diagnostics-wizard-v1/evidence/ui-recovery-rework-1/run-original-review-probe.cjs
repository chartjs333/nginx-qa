// Executes the reviewer's exact probe and frozen export. Only its JSON output
// destination is redirected so the genuine review artifacts remain unchanged.
const fs=require("fs"),path=require("path"),vm=require("vm");
const reviewDir="C:/nqt35/client/sdw-ui-recovery-20261104/reviews/runtime-ui-7a39bd";
const output=path.join(__dirname,"original-probe-rerun.json");
const original=fs.readFileSync(path.join(reviewDir,"probe-session-order.cjs"),"utf8");
vm.runInNewContext(original,{
 __dirname:reviewDir,process,console,
 require(name){
  if(name!=="fs")return require(name);
  return {...fs,writeFileSync(target,data){
   if(path.resolve(target)!==path.resolve(reviewDir,"probe-session-order.json"))throw new Error("Unexpected probe output");
   fs.writeFileSync(output,data);
  }};
 }
},{filename:"exact-reviewer-probe.cjs"});
