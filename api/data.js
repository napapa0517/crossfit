// Vercel 서버리스 함수: 공유 데이터 저장소 (Upstash Redis REST API)
// 환경변수: KV_REST_API_URL / KV_REST_API_TOKEN (또는 UPSTASH_REDIS_REST_URL / _TOKEN), COACH_PASSWORD
//   GET                      → 설정·프로그램·운동 기록 전체 (누구나)
//   POST ?action=log         → 운동 기록 저장 (누구나: 타임별 인원만 / 코치: 메모 포함)
//   POST ?action=login       → 코치 비밀번호 확인
//   POST ?action=config      → 설정·동작 목록 저장 (코치)
//   POST ?action=program     → 하루 프로그램 저장·삭제 (코치)
//   POST ?action=import      → 전체 덮어쓰기: 백업 불러오기·초기화 (코치)
const crypto = require('crypto');

const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const COACH_PASSWORD = process.env.COACH_PASSWORD || '';
const K = {config:'wod:config', programs:'wod:programs', logs:'wod:logs'};
const DEFAULT_SLOTS = ['06:30','17:30','18:40','19:50','21:00'];
const DATE = /^\d{4}-\d{2}-\d{2}$/;

async function redis(cmds){
  const r = await fetch(REDIS_URL.replace(/\/$/,'')+'/pipeline', {
    method:'POST',
    headers:{Authorization:'Bearer '+REDIS_TOKEN, 'Content-Type':'application/json'},
    body:JSON.stringify(cmds)
  });
  if(!r.ok) throw new Error('저장소 오류 '+r.status);
  const out = await r.json();
  for(const x of out) if(x.error) throw new Error(x.error);
  return out.map(x=>x.result);
}
function hashToObj(arr){
  const o = {};
  for(let i=0;i<(arr||[]).length;i+=2){ try{ o[arr[i]] = JSON.parse(arr[i+1]); }catch(e){} }
  return o;
}
function isCoach(req){
  if(!COACH_PASSWORD) return false;
  const a = Buffer.from(String(req.headers['x-coach-password']||''));
  const b = Buffer.from(COACH_PASSWORD);
  return a.length===b.length && crypto.timingSafeEqual(a,b);
}
function hsetCmd(key,obj){
  const args = [];
  for(const [k,v] of Object.entries(obj||{})) if(DATE.test(k) && v) args.push(k, JSON.stringify(v));
  return args.length ? [['HSET',key,...args]] : [];
}

module.exports = async (req,res)=>{
  res.setHeader('Cache-Control','no-store');
  const send = (code,obj)=>res.status(code).json(obj);
  if(!REDIS_URL || !REDIS_TOKEN) return send(503,{ok:false,error:'storage-not-configured'});
  try{
    if(req.method==='GET'){
      const [cfg,progs,logs] = await redis([['GET',K.config],['HGETALL',K.programs],['HGETALL',K.logs]]);
      return send(200,{ok:true, config:cfg?JSON.parse(cfg):null, programs:hashToObj(progs), logs:hashToObj(logs), coachEnabled:!!COACH_PASSWORD});
    }
    if(req.method!=='POST') return send(405,{ok:false,error:'method not allowed'});
    const action = req.query.action;
    const body = typeof req.body==='string' ? JSON.parse(req.body||'{}') : (req.body||{});

    if(action==='login'){
      if(!COACH_PASSWORD) return send(503,{ok:false,error:'COACH_PASSWORD 환경변수가 설정되지 않았습니다'});
      return isCoach(req) ? send(200,{ok:true}) : send(401,{ok:false,error:'비밀번호가 틀렸습니다'});
    }

    if(action==='log'){
      const {date,log} = body;
      if(!DATE.test(date||'')) return send(400,{ok:false,error:'날짜 형식 오류'});
      const coach = isCoach(req);
      const [prevRaw,cfgRaw] = await redis([['HGET',K.logs,date],['GET',K.config]]);
      const prev = prevRaw ? JSON.parse(prevRaw) : {counts:{},memo:''};
      const slots = (cfgRaw && JSON.parse(cfgRaw).settings?.slots) || DEFAULT_SLOTS;
      const counts = {};
      for(const s of slots){
        const v = log?.counts?.[s];
        if(v===''||v==null){ counts[s] = ''; continue; }
        const n = Number(v);
        if(!Number.isInteger(n) || n<0 || n>200) return send(400,{ok:false,error:`${s} 인원 값이 올바르지 않습니다`});
        counts[s] = String(n);
      }
      // 회원은 인원수만 바꿀 수 있고 메모는 기존 값을 유지
      const memo = coach ? String(log?.memo||'').slice(0,1000) : (prev.memo||'');
      const empty = Object.values(counts).every(v=>v==='') && !memo.trim();
      const next = {counts,memo};
      await redis([empty ? ['HDEL',K.logs,date] : ['HSET',K.logs,date,JSON.stringify(next)]]);
      return send(200,{ok:true, log:empty?null:next});
    }

    if(!isCoach(req)) return send(401,{ok:false,error:'코치 로그인이 필요합니다'});

    if(action==='config'){
      if(!body.config || !Array.isArray(body.config.movements)) return send(400,{ok:false,error:'설정 형식 오류'});
      await redis([['SET',K.config,JSON.stringify(body.config)]]);
      return send(200,{ok:true});
    }
    if(action==='program'){
      if(!DATE.test(body.date||'')) return send(400,{ok:false,error:'날짜 형식 오류'});
      await redis([body.program ? ['HSET',K.programs,body.date,JSON.stringify(body.program)] : ['HDEL',K.programs,body.date]]);
      return send(200,{ok:true});
    }
    if(action==='import'){
      if(!body.config || !Array.isArray(body.config.movements)) return send(400,{ok:false,error:'백업 형식 오류'});
      await redis([
        ['DEL',K.programs],['DEL',K.logs],['SET',K.config,JSON.stringify(body.config)],
        ...hsetCmd(K.programs,body.programs), ...hsetCmd(K.logs,body.logs)
      ]);
      return send(200,{ok:true});
    }
    return send(400,{ok:false,error:'알 수 없는 요청'});
  }catch(e){
    return send(500,{ok:false,error:e.message});
  }
};
