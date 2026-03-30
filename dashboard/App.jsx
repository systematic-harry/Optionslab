import { useState, useCallback, useMemo, useEffect } from "react";
import { LineChart, Line, AreaChart, Area, BarChart, Bar, XAxis, YAxis, Tooltip,
         ResponsiveContainer, ReferenceLine } from "recharts";

const API = "http://localhost:8000";

// ── CONSTANTS ────────────────────────────────────────────────
const STRIKES = [21500,21600,21700,21800,21900,22000,22100,22200,22300,22400,22500];
const SPOT = 22148;
const C = {
  bg:"#0a0e1a", surf:"#0f1424", card:"#131929", border:"#1e2a3a",
  accent:"#00d4aa", warn:"#f59e0b", text:"#e2e8f0", muted:"#64748b",
  green:"#22c55e", red:"#ef4444", call:"#38bdf8", put:"#f472b6"
};
const tt = {
  contentStyle:{background:"#0f1424",border:"1px solid #1e2a3a",borderRadius:6,fontSize:11},
  labelStyle:{color:"#64748b"}
};
const fmt=(n,d=0)=>n==null?"—":Number(n).toLocaleString("en-IN",{minimumFractionDigits:d,maximumFractionDigits:d});
const pnlCol=v=>v>0?C.green:v<0?C.red:C.muted;
const sigCol=s=>s==="STRONG"?C.green:s==="WATCH"?C.warn:C.muted;
const sigBg=s=>s==="STRONG"?"#22c55e22":s==="WATCH"?"#f59e0b22":"#ffffff0a";

// ── MATH ─────────────────────────────────────────────────────
function cdf(x){const a1=0.254829592,a2=-0.284496736,a3=1.421413741,a4=-1.453152027,a5=1.061405429;const sign=x<0?-1:1,t=1/(1+0.3275911*Math.abs(x));const y=(((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*Math.exp(-x*x/2);return 0.5*(1+sign*(1-y));}
function pdf(x){return Math.exp(-x*x/2)/Math.sqrt(2*Math.PI);}
function bsPrice(S,K,T,r,sig,type){if(T<=0)return type==="call"?Math.max(S-K,0):Math.max(K-S,0);const d1=(Math.log(S/K)+(r+sig*sig/2)*T)/(sig*Math.sqrt(T)),d2=d1-sig*Math.sqrt(T);if(type==="call")return S*cdf(d1)-K*Math.exp(-r*T)*cdf(d2);return K*Math.exp(-r*T)*cdf(-d2)-S*cdf(-d1);}
function calcGreeks(S,K,T,r,sig,type){if(T<=0)return{delta:type==="call"?1:0,gamma:0,theta:0,vega:0};const d1=(Math.log(S/K)+(r+sig*sig/2)*T)/(sig*Math.sqrt(T)),d2=d1-sig*Math.sqrt(T);const delta=type==="call"?cdf(d1):cdf(d1)-1;const gamma=pdf(d1)/(S*sig*Math.sqrt(T));const theta=type==="call"?(-(S*pdf(d1)*sig)/(2*Math.sqrt(T))-r*K*Math.exp(-r*T)*cdf(d2))/365:(-(S*pdf(d1)*sig)/(2*Math.sqrt(T))+r*K*Math.exp(-r*T)*cdf(-d2))/365;const vega=S*pdf(d1)*Math.sqrt(T)/100;return{delta:delta.toFixed(3),gamma:gamma.toFixed(5),theta:theta.toFixed(2),vega:vega.toFixed(2)};}
function generateChain(){const T=7/365,r=0.067;return STRIKES.map(K=>{const cIV=0.12+0.04*Math.pow((K-SPOT)/SPOT,2)*10+(K<SPOT?0.02:0);const pIV=cIV+0.015;const cG=calcGreeks(SPOT,K,T,r,cIV,"call"),pG=calcGreeks(SPOT,K,T,r,pIV,"put");const cOI=Math.round((K<SPOT?800000:1200000)*(0.9+Math.random()*0.3));const pOI=Math.round((K>SPOT?900000:600000)*(0.9+Math.random()*0.3));return{strike:K,atm:Math.abs(K-SPOT)<75,call:{ltp:+bsPrice(SPOT,K,T,r,cIV,"call").toFixed(2),iv:(cIV*100).toFixed(1),oi:cOI,vol:Math.round(cOI*0.08),...cG},put:{ltp:+bsPrice(SPOT,K,T,r,pIV,"put").toFixed(2),iv:(pIV*100).toFixed(1),oi:pOI,vol:Math.round(pOI*0.08),...pG}};});}
function generateOI(){return Array.from({length:30},(_,i)=>{const d=new Date(2025,0,2+i*3);const cOI=12e6+Math.sin(i*0.4)*3e6+Math.random()*1e6;const pOI=10e6+Math.sin(i*0.4+1)*2.5e6+Math.random()*1e6;return{date:d.toLocaleDateString("en-IN",{day:"2-digit",month:"short"}),callOI:+(cOI/1e5).toFixed(1),putOI:+(pOI/1e5).toFixed(1),pcr:+(pOI/cOI).toFixed(3)};});}
function generateBacktest(strat){let equity=100000,trades=[],curve=[{date:"Start",val:100000}];const months=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];let wins=0,losses=0,totalPnl=0;const wp={straddle:0.46,strangle:0.44,iron_condor:0.62,bull_spread:0.52};const pr={straddle:[500,4500],strangle:[400,3800],iron_condor:[200,2200],bull_spread:[500,5500]};for(let i=0;i<48;i++){const[lo,hi]=pr[strat]||[400,4000];const won=Math.random()<(wp[strat]||0.48);const pnl=Math.round((won?1:-1)*(lo+Math.random()*(hi-lo)));equity+=pnl;totalPnl+=pnl;if(pnl>0)wins++;else losses++;trades.push({id:i+1,date:`${months[Math.floor(i/4)%12]} W${i%4+1}`,pnl,win:pnl>0});if(i%2===0)curve.push({date:`${months[Math.floor(i/4)%12]}`,val:equity});}return{trades:trades.slice(-10),curve,wins,losses,totalPnl,maxDD:Math.round(Math.min(...curve.map(c=>c.val))-100000),sharpe:+((totalPnl/100000*Math.sqrt(48))/0.15).toFixed(2)};}

function Card({children,style}){return <div style={{background:C.card,border:`1px solid ${C.border}`,borderRadius:8,padding:16,...style}}>{children}</div>;}
function Label({children}){return <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:8}}>{children}</div>;}
function StatCard({label,value,color,sub}){return(<div style={{background:C.card,border:`1px solid ${C.border}`,borderRadius:8,padding:"12px 14px"}}><div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:6}}>{label}</div><div style={{fontSize:18,fontWeight:700,color:color||C.text,fontFamily:"monospace"}}>{value}</div>{sub&&<div style={{fontSize:10,color:C.muted,marginTop:4}}>{sub}</div>}</div>);}

// ── SCREENER TAB ─────────────────────────────────────────────
function ScreenerTab({connected}){
  const [screeners,setScreeners]=useState([]);
  const [selectedScreener,setSelectedScreener]=useState("");
  const [frequency,setFrequency]=useState("1 day");
  const [periods,setPeriods]=useState(14);
  const [save,setSave]=useState(false);
  const [running,setRunning]=useState(false);
  const [results,setResults]=useState(null);
  const [hovered,setHovered]=useState(null);
  const [filter,setFilter]=useState("ALL");
  const [vix,setVix]=useState(null);
  const [error,setError]=useState(null);

  // Load screeners list and VIX on mount
  useEffect(()=>{
    fetch(`${API}/screeners`)
      .then(r=>r.json())
      .then(d=>{
        setScreeners(d.screeners||[]);
        if(d.screeners?.length>0) setSelectedScreener(d.screeners[0].filename);
      })
      .catch(()=>setError("Could not reach server. Is server.py running?"));

    fetch(`${API}/vix`)
      .then(r=>r.json())
      .then(d=>setVix(d.vix))
      .catch(()=>{});
  },[]);

  const runScan=async()=>{
    if(!connected){setError("Connect to TWS first!");return;}
    if(!selectedScreener){setError("Select a screener first!");return;}
    setRunning(true);
    setError(null);
    setResults(null);
    try{
      const res=await fetch(`${API}/scan`,{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({screener:selectedScreener,frequency,periods,save})
      });
      const data=await res.json();
      if(data.status==="skipped"){
        setError(`Scan skipped: ${data.reason}`);
      } else {
        setResults(data);
        if(data.results?.length>0) setHovered(data.results[0]);
      }
    }catch(e){
      setError("Scan failed. Check server logs.");
    }
    setRunning(false);
  };

  const rows=results?.results?.filter(r=>filter==="ALL"||r.signal===filter)||[];

  return(
    <div style={{display:"flex",flexDirection:"column",gap:14}}>
      {/* Controls */}
      <Card>
        <div style={{display:"flex",gap:12,alignItems:"flex-end",flexWrap:"wrap"}}>
          <div style={{display:"flex",flexDirection:"column",gap:4}}>
            <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>Screener</div>
            <select value={selectedScreener} onChange={e=>setSelectedScreener(e.target.value)}
              style={{padding:"6px 10px",borderRadius:6,border:`1px solid ${C.border}`,background:C.surf,color:C.text,fontSize:12,cursor:"pointer",minWidth:180}}>
              {screeners.map(s=><option key={s.filename} value={s.filename}>{s.name}</option>)}
              {screeners.length===0&&<option>No screeners found</option>}
            </select>
          </div>
          <div style={{display:"flex",flexDirection:"column",gap:4}}>
            <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>Frequency</div>
            <select value={frequency} onChange={e=>setFrequency(e.target.value)}
              style={{padding:"6px 10px",borderRadius:6,border:`1px solid ${C.border}`,background:C.surf,color:C.text,fontSize:12,cursor:"pointer"}}>
              {["1 min","5 mins","15 mins","30 mins","1 hour","4 hours","1 day","1 week","1 month"].map(f=><option key={f}>{f}</option>)}
            </select>
          </div>
          <div style={{display:"flex",flexDirection:"column",gap:4}}>
            <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>Periods</div>
            <input type="number" value={periods} onChange={e=>setPeriods(+e.target.value)}
              min={1} max={500} style={{padding:"6px 10px",borderRadius:6,border:`1px solid ${C.border}`,background:C.surf,color:C.text,fontSize:12,width:80}}/>
          </div>
          <div style={{display:"flex",flexDirection:"column",gap:4}}>
            <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>Save to GCS</div>
            <label style={{display:"flex",alignItems:"center",gap:6,cursor:"pointer",height:34}}>
              <input type="checkbox" checked={save} onChange={e=>setSave(e.target.checked)} style={{accentColor:C.accent,width:14,height:14}}/>
              <span style={{fontSize:12,color:C.muted}}>For backtesting</span>
            </label>
          </div>
          <div style={{marginLeft:"auto",display:"flex",flexDirection:"column",gap:4,alignItems:"flex-end"}}>
            {vix&&<div style={{fontSize:10,color:C.muted}}>VIX: <span style={{color:vix<15?C.green:C.red,fontFamily:"monospace",fontWeight:600}}>{vix} {vix<15?"✓ OK":"✗ High"}</span></div>}
            <button onClick={runScan} disabled={running||!connected}
              style={{padding:"7px 24px",borderRadius:6,border:`1px solid ${running||!connected?C.border:C.accent}`,background:running?"#00d4aa11":!connected?"transparent":"#00d4aa22",color:running||!connected?C.muted:C.accent,fontSize:12,fontWeight:700,cursor:running||!connected?"not-allowed":"pointer"}}>
              {running?"Scanning...":"▶ Run Scan"}
            </button>
          </div>
        </div>
        {error&&<div style={{marginTop:10,padding:"8px 12px",borderRadius:6,background:"#ef444422",border:`1px solid ${C.red}`,color:C.red,fontSize:11}}>{error}</div>}
      </Card>

      {/* Results */}
      {results&&<>
        <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:10}}>
          <StatCard label="Total Scanned" value={results.total} color={C.muted}/>
          <StatCard label="Strong Signal" value={results.strong} color={C.green} sub="Ready to trade"/>
          <StatCard label="Watch List"    value={results.watch}  color={C.warn}  sub="Monitor"/>
          <StatCard label="Skip"          value={results.skip}   color={C.muted} sub="Not suitable"/>
        </div>

        <div style={{display:"flex",gap:8,alignItems:"center"}}>
          {["ALL","STRONG","WATCH","SKIP"].map(f=>(
            <button key={f} onClick={()=>setFilter(f)} style={{padding:"4px 12px",borderRadius:6,border:`1px solid ${filter===f?sigCol(f):C.border}`,background:filter===f?sigBg(f):"transparent",color:filter===f?sigCol(f):C.muted,fontSize:11,cursor:"pointer",textTransform:"uppercase"}}>
              {f}
            </button>
          ))}
        </div>

        <Card style={{padding:0,overflow:"hidden"}}>
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11,minWidth:800}}>
              <thead><tr style={{background:C.surf,borderBottom:`1px solid ${C.border}`}}>
                {["Stock","Price","Chg%","Signal","RSI","ADX","MACD","BB Width","Reasons"].map(h=>(
                  <th key={h} style={{padding:"8px 10px",color:C.muted,textAlign:"left",fontSize:10,fontWeight:400,letterSpacing:"0.05em"}}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {rows.map(row=>{
                  const hov=hovered?.symbol===row.symbol;
                  return(
                    <tr key={row.symbol} onMouseEnter={()=>setHovered(row)}
                      style={{background:hov?"#1e2a3a88":"transparent",borderBottom:`1px solid ${C.border}22`,cursor:"pointer"}}>
                      <td style={{padding:"8px 10px",fontWeight:600,color:C.text}}>{row.symbol}</td>
                      <td style={{padding:"8px 10px",fontFamily:"monospace"}}>₹{fmt(row.price,2)}</td>
                      <td style={{padding:"8px 10px",fontFamily:"monospace",color:row.change>0?C.green:C.red}}>{row.change>0?"+":""}{row.change}%</td>
                      <td style={{padding:"8px 10px"}}>
                        <span style={{padding:"3px 10px",borderRadius:4,fontSize:10,fontWeight:600,background:sigBg(row.signal),color:sigCol(row.signal)}}>{row.signal}</span>
                      </td>
                      <td style={{padding:"8px 10px",fontFamily:"monospace",color:row.rsi>70?C.red:row.rsi<30?C.green:C.text}}>{row.rsi??"—"}</td>
                      <td style={{padding:"8px 10px",fontFamily:"monospace",color:row.adx<20?C.green:row.adx>25?C.red:C.warn}}>{row.adx??"—"}</td>
                      <td style={{padding:"8px 10px",fontFamily:"monospace",color:row.macd>0?C.green:C.red}}>{row.macd??"—"}</td>
                      <td style={{padding:"8px 10px",fontFamily:"monospace",color:C.muted}}>{row.bb_width??"—"}</td>
                      <td style={{padding:"8px 10px",maxWidth:200}}>
                        {row.reasons?.slice(0,2).map((r,i)=>(
                          <div key={i} style={{fontSize:10,color:C.muted,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis"}}>{r}</div>
                        ))}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Card>

        {/* Chart for hovered stock */}
        {hovered&&hovered.chart?.length>0&&(
          <Card>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12,flexWrap:"wrap",gap:8}}>
              <div style={{display:"flex",alignItems:"baseline",gap:10}}>
                <span style={{fontWeight:700,fontSize:14,color:C.text}}>{hovered.symbol}</span>
                <span style={{fontFamily:"monospace",fontWeight:700}}>₹{fmt(hovered.price,2)}</span>
                <span style={{fontSize:12,color:hovered.change>0?C.green:C.red,fontFamily:"monospace"}}>{hovered.change>0?"+":""}{hovered.change}%</span>
              </div>
              <div style={{display:"flex",gap:10,fontSize:11}}>
                {[{l:"RSI",v:hovered.rsi,c:hovered.rsi>70?C.red:hovered.rsi<30?C.green:C.text},
                  {l:"ADX",v:hovered.adx,c:hovered.adx<20?C.green:hovered.adx>25?C.red:C.warn},
                  {l:"MACD",v:hovered.macd,c:hovered.macd>0?C.green:C.red}].map(i=>(
                  <span key={i.l} style={{background:C.surf,padding:"3px 10px",borderRadius:6,border:`1px solid ${C.border}`}}>
                    <span style={{color:C.muted}}>{i.l}: </span>
                    <span style={{color:i.c,fontFamily:"monospace",fontWeight:600}}>{i.v??"—"}</span>
                  </span>
                ))}
              </div>
            </div>
            <ResponsiveContainer width="100%" height={160}>
              <AreaChart data={hovered.chart}>
                <defs><linearGradient id="cg" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor={C.accent} stopOpacity={0.3}/><stop offset="95%" stopColor={C.accent} stopOpacity={0}/></linearGradient></defs>
                <XAxis dataKey="date" tick={{fontSize:9,fill:C.muted}} interval={14}/>
                <YAxis tick={{fontSize:9,fill:C.muted}} width={65} domain={["auto","auto"]} tickFormatter={v=>v>=1000?"₹"+(v/1000).toFixed(1)+"k":"₹"+v}/>
                <Tooltip {...tt} formatter={v=>["₹"+fmt(v,2),"Close"]}/>
                <Area type="monotone" dataKey="close" stroke={C.accent} strokeWidth={2} fill="url(#cg)"/>
              </AreaChart>
            </ResponsiveContainer>
            <div style={{display:"flex",flexWrap:"wrap",gap:6,marginTop:10}}>
              {hovered.reasons?.map((r,i)=>(
                <span key={i} style={{fontSize:10,padding:"3px 10px",borderRadius:4,
                  background:r.includes("✓")?"#22c55e11":r.includes("✗")?"#ef444411":"#ffffff0a",
                  border:`1px solid ${r.includes("✓")?"#22c55e33":r.includes("✗")?"#ef444433":C.border}`,
                  color:r.includes("✓")?C.green:r.includes("✗")?C.red:C.muted}}>{r}</span>
              ))}
            </div>
          </Card>
        )}
      </>}

      {!results&&!running&&(
        <div style={{textAlign:"center",padding:"40px",color:C.muted,fontSize:12,border:`1px dashed ${C.border}`,borderRadius:8}}>
          {connected?"Select a screener and click Run Scan":"Connect to TWS first, then run a scan"}
        </div>
      )}
    </div>
  );
}

// ── OPTION CHAIN ─────────────────────────────────────────────
function OptionChain({chain,selected,onToggle}){
  const [filter,setFilter]=useState("all");
  const rows=chain.filter(r=>filter==="all"||(filter==="atm"&&r.atm)||(filter==="itm"&&r.strike<SPOT)||(filter==="otm"&&r.strike>SPOT));
  return(
    <div style={{display:"flex",flexDirection:"column",gap:12}}>
      <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
        {["all","itm","atm","otm"].map(f=>(
          <button key={f} onClick={()=>setFilter(f)} style={{padding:"4px 12px",borderRadius:6,border:`1px solid ${filter===f?C.accent:C.border}`,background:filter===f?"#00d4aa22":"transparent",color:filter===f?C.accent:C.muted,fontSize:11,cursor:"pointer",textTransform:"uppercase"}}>{f}</button>
        ))}
        <div style={{marginLeft:"auto",fontSize:11,color:C.muted}}>SPOT <span style={{color:C.accent,fontWeight:700,fontFamily:"monospace"}}>₹{fmt(SPOT)}</span><span style={{marginLeft:10,color:C.warn}}>7d expiry · Lot 50</span></div>
      </div>
      <div style={{overflowX:"auto"}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:11,minWidth:820}}>
          <thead><tr style={{borderBottom:`1px solid ${C.border}`}}>
            {["OI","Vol","IV","LTP","Δ","Θ","Ν"].map(h=><th key={h} style={{padding:"6px 8px",color:C.call,textAlign:"right",fontWeight:500,fontSize:10}}>{h}</th>)}
            <th style={{padding:"6px 12px",color:C.muted,textAlign:"center",minWidth:80,fontSize:10}}>STRIKE</th>
            {["LTP","IV","Vol","OI","Δ","Θ","Ν"].map(h=><th key={h+"p"} style={{padding:"6px 8px",color:C.put,textAlign:"left",fontWeight:500,fontSize:10}}>{h}</th>)}
            <th style={{width:32}}/>
          </tr></thead>
          <tbody>
            {rows.map(row=>{
              const sel=selected.includes(row.strike);
              return(<tr key={row.strike} style={{background:row.atm?`${C.accent}09`:sel?`${C.warn}09`:"transparent",borderBottom:`1px solid ${C.border}22`}}>
                {[fmt(row.call.oi),fmt(row.call.vol),row.call.iv+"%","₹"+fmt(row.call.ltp,2),row.call.delta,row.call.theta,row.call.vega].map((v,i)=>(
                  <td key={i} style={{padding:"5px 8px",textAlign:"right",color:i===3?C.call:C.text,fontFamily:"monospace"}}>{v}</td>
                ))}
                <td onClick={()=>onToggle(row.strike)} style={{padding:"5px 12px",textAlign:"center",cursor:"pointer",fontWeight:700,fontSize:12,color:row.atm?C.accent:C.muted,background:sel?`${C.warn}22`:row.atm?`${C.accent}15`:"transparent",borderLeft:`1px solid ${C.border}33`,borderRight:`1px solid ${C.border}33`}}>
                  {row.strike}{row.atm&&<div style={{fontSize:8,color:C.accent}}>ATM</div>}
                </td>
                {["₹"+fmt(row.put.ltp,2),row.put.iv+"%",fmt(row.put.vol),fmt(row.put.oi),row.put.delta,row.put.theta,row.put.vega].map((v,i)=>(
                  <td key={i} style={{padding:"5px 8px",textAlign:"left",color:i===0?C.put:C.text,fontFamily:"monospace"}}>{v}</td>
                ))}
                <td style={{padding:"5px 4px"}}><button onClick={()=>onToggle(row.strike)} style={{width:20,height:20,borderRadius:4,border:`1px solid ${sel?C.warn:C.border}`,background:sel?`${C.warn}22`:"transparent",color:sel?C.warn:C.muted,cursor:"pointer",fontSize:13,display:"flex",alignItems:"center",justifyContent:"center"}}>{sel?"−":"+"}</button></td>
              </tr>);
            })}
          </tbody>
        </table>
      </div>
      <div style={{fontSize:10,color:C.muted}}>Click strike or + to add to Strategy Builder · Δ=Delta · Θ=Theta · Ν=Vega</div>
    </div>
  );
}

// ── OI PANEL ─────────────────────────────────────────────────
function OIPanel({data}){
  const latest=data[data.length-1];const avg=+(data.reduce((s,d)=>s+d.pcr,0)/data.length).toFixed(3);const pcrCol=latest.pcr>1.2?C.green:latest.pcr<0.8?C.red:C.warn;
  return(<div style={{display:"flex",flexDirection:"column",gap:16}}>
    <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:10}}>
      <StatCard label="Current PCR" value={latest.pcr.toFixed(3)} color={pcrCol} sub={latest.pcr>1?"↑ Bullish":"↓ Bearish"}/>
      <StatCard label="30d Avg PCR" value={avg} color={C.muted}/>
      <StatCard label="Total Call OI" value={latest.callOI.toFixed(1)+"L"} color={C.call} sub="Lakh contracts"/>
      <StatCard label="Total Put OI" value={latest.putOI.toFixed(1)+"L"} color={C.put} sub="Lakh contracts"/>
    </div>
    <Card><Label>Put-Call Ratio — 30 days</Label>
      <ResponsiveContainer width="100%" height={130}><LineChart data={data}>
        <XAxis dataKey="date" tick={{fontSize:9,fill:C.muted}} interval={4}/>
        <YAxis tick={{fontSize:9,fill:C.muted}} domain={[0.5,1.9]} width={35}/>
        <Tooltip {...tt}/><ReferenceLine y={1} stroke={C.muted} strokeDasharray="4 4" strokeOpacity={0.5}/>
        <ReferenceLine y={1.2} stroke={C.green} strokeDasharray="3 3" strokeOpacity={0.5}/>
        <ReferenceLine y={0.8} stroke={C.red} strokeDasharray="3 3" strokeOpacity={0.5}/>
        <Line type="monotone" dataKey="pcr" stroke={C.accent} strokeWidth={2} dot={false}/>
      </LineChart></ResponsiveContainer>
    </Card>
    <Card><Label>Call vs Put OI (Lakh contracts)</Label>
      <ResponsiveContainer width="100%" height={150}><BarChart data={data} barSize={8} barGap={1}>
        <XAxis dataKey="date" tick={{fontSize:9,fill:C.muted}} interval={4}/>
        <YAxis tick={{fontSize:9,fill:C.muted}} width={35}/><Tooltip {...tt}/>
        <Bar dataKey="callOI" fill={C.call} name="Call OI" opacity={0.85} radius={[2,2,0,0]}/>
        <Bar dataKey="putOI" fill={C.put} name="Put OI" opacity={0.85} radius={[2,2,0,0]}/>
      </BarChart></ResponsiveContainer>
      <div style={{display:"flex",gap:16,fontSize:10,marginTop:6}}><span style={{color:C.call}}>■ Call OI</span><span style={{color:C.put}}>■ Put OI</span></div>
    </Card>
  </div>);
}


function MetricCard({label,value,color,sub}){
  return(
    <div style={{background:C.card,border:`1px solid ${C.border}`,borderRadius:8,padding:"10px 12px"}}>
      <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.05em",marginBottom:4}}>{label}</div>
      <div style={{fontSize:16,fontWeight:700,color:color||C.text,fontFamily:"monospace"}}>{value}</div>
      {sub&&<div style={{fontSize:10,color:C.muted,marginTop:2}}>{sub}</div>}
    </div>
  );
}

// ============================================================
//  BacktestTab_v2.jsx
//  Replace BacktestPanel function in App.jsx with this
//  Rename: export default function BacktestTab → function BacktestPanel
// ============================================================

function BacktestPanel({connected, screeners}){
  const [strategy,setStrategy]     = useState("");
  const [frequency,setFrequency]   = useState("1 day");
  const [startDate,setStartDate]   = useState("2024-01-01");
  const [endDate,setEndDate]       = useState(new Date().toISOString().slice(0,10));
  const [capital,setCapital]       = useState(100000);
  const [useGcs,setUseGcs]         = useState(true);
  const [singleSymbol,setSingle]   = useState("RELIANCE");
  const [sizingType,setSizingType] = useState("pct_capital");
  const [sizingValue,setSizingVal] = useState(10);
  const [running,setRunning]       = useState(false);
  const [results,setResults]       = useState(null);
  const [error,setError]           = useState(null);
  const [section,setSection]       = useState("summary");

  useEffect(()=>{
    if(screeners?.length>0 && !strategy) setStrategy(screeners[0].filename);
  },[screeners]);

  const sizingLabel = {
    fixed_amount:  "Amount (₹)",
    fixed_qty:     "Shares",
    pct_capital:   "% of Capital",
    full_capital:  "N/A"
  }[sizingType];

  const runBacktest = async()=>{
    if(!connected){setError("Connect to TWS first!");return;}
    if(!strategy){setError("Select a strategy!");return;}
    setRunning(true);setError(null);setResults(null);
    try{
      const res = await fetch(`${API}/backtest`,{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({
          screener:     strategy,
          symbols:      useGcs ? [] : [singleSymbol.trim().toUpperCase()],
          use_gcs:      useGcs,
          frequency,
          start_date:   startDate,
          end_date:     endDate,
          capital:      Number(capital),
          sizing_type:  sizingType,
          sizing_value: Number(sizingValue),
        })
      });
      const data = await res.json();
      if(data.error) setError(data.error);
      else setResults(data);
    }catch(e){
      setError("Backtest failed. Check server logs.");
    }
    setRunning(false);
  };

  // PDF Export using jsPDF
  const exportPDF = ()=>{
    if(!results) return;
    const script = document.createElement("script");
    script.src = "https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js";
    script.onload = ()=>{
      const { jsPDF } = window.jspdf;
      const doc = new jsPDF();
      let y = 15;

      const line = (text, size=10, bold=false)=>{
        doc.setFontSize(size);
        doc.setFont("helvetica", bold?"bold":"normal");
        doc.text(text, 14, y);
        y += size * 0.5 + 2;
        if(y > 270){ doc.addPage(); y = 15; }
      };

      const divider = ()=>{ doc.setDrawColor(200); doc.line(14,y,196,y); y+=4; };

      // Header
      line("OptionLab — Backtest Report", 16, true);
      divider();
      line(`Strategy: ${results.screener?.replace(".py","")}  |  Frequency: ${results.frequency}`);
      line(`Period: ${results.start_date} to ${results.end_date}  |  Capital: ₹${fmt(results.capital)}`);
      line(`Sizing: ${results.sizing_type} = ${results.sizing_value}  |  Generated: ${new Date().toLocaleString("en-IN")}`);
      y += 4;

      // Profitability
      line("PROFITABILITY", 12, true); divider();
      line(`Net Profit: ₹${fmt(results.net_profit)}   Total Return: ${results.total_return_pct}%   Buy & Hold: ${results.buy_hold_return_pct}%`);
      line(`Gross Profit: ₹${fmt(results.gross_profit)}   Gross Loss: ₹${fmt(results.gross_loss)}   Profit Factor: ${results.profit_factor}`);
      line(`Monthly Avg: ${results.monthly_avg_return}%   Std Dev: ${results.monthly_std}%`);
      y += 4;

      // Trades
      line("TRADES", 12, true); divider();
      line(`Total: ${results.total_trades}   Wins: ${results.winning_trades}   Losses: ${results.losing_trades}   Win Rate: ${results.win_rate}%`);
      line(`Avg P&L/Trade: ₹${fmt(results.avg_pnl_per_trade,2)}   Max Profit: ₹${fmt(results.max_profit)}   Max Loss: ₹${fmt(results.max_loss)}`);
      line(`Largest Winner: ${results.largest_winner_pct}% of gross   Largest Loser: ${results.largest_loser_pct}% of gross`);
      line(`Max Consec Losses: ${results.max_consec_losses}   Avg Win Days: ${results.avg_win_days}   Avg Loss Days: ${results.avg_loss_days}`);
      line(`Long Trades: ${results.long_trades}   Short Trades: ${results.short_trades}`);
      y += 4;

      // Risk
      line("RISK", 12, true); divider();
      line(`Max Drawdown: ₹${fmt(results.max_drawdown)} (${results.max_drawdown_pct}%)   Sharpe: ${results.sharpe_ratio}   MAR: ${results.mar_ratio}`);
      line(`Best Stock: ${results.best_stock}   Worst Stock: ${results.worst_stock}`);
      y += 4;

      // Per stock
      if(results.stock_summary?.length > 0){
        line("PER STOCK SUMMARY", 12, true); divider();
        results.stock_summary.forEach(s=>{
          line(`${s.symbol.padEnd(15)} P&L: ₹${fmt(s.total_pnl).padStart(10)}  Win%: ${s.win_rate}%  Trades: ${s.total_trades}  L:${s.long_trades}/S:${s.short_trades}`);
        });
        y += 4;
      }

      // Data quality
      if(results.data_quality?.length > 0){
        line("DATA QUALITY", 12, true); divider();
        results.data_quality.forEach(q=>{
          if(q.fetched > 0){
            line(`${q.symbol}: ${q.fetched} fetched, ${q.dropped} dropped, ${q.filled} filled → ${q.final} bars used`);
            q.warnings?.forEach(w=> line(`  ⚠ ${w}`));
          }
        });
        y += 4;
      }

      // Trade log
      if(results.trade_log?.length > 0){
        doc.addPage(); y = 15;
        line("TRADE LOG", 12, true); divider();
        line("#    Symbol    Dir    Entry Date   Entry₹    Exit Date    Exit₹     P&L₹     Return%  Days");
        doc.setDrawColor(200); doc.line(14,y,196,y); y+=3;
        results.trade_log.forEach((t,i)=>{
          const row = `${String(i+1).padStart(3)}  ${t.symbol.padEnd(10)} ${t.direction.padEnd(5)}  ${t.entry_date}  ₹${fmt(t.entry_price,2).padStart(8)}  ${t.exit_date}  ₹${fmt(t.exit_price,2).padStart(8)}  ₹${fmt(t.pnl,2).padStart(8)}  ${t.pnl_pct}%  ${t.hold_days}d`;
          line(row, 8);
        });
      }

      doc.save(`backtest_${results.screener?.replace(".py","")}_${results.start_date}_${results.end_date}.pdf`);
    };
    document.head.appendChild(script);
  };

  return(
    <div style={{display:"flex",flexDirection:"column",gap:14}}>

      {/* Controls */}
      <Card>
        <div style={{display:"flex",gap:12,alignItems:"flex-start",flexWrap:"wrap"}}>

          {/* Strategy */}
          <div style={{display:"flex",flexDirection:"column",gap:4}}>
            <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>Strategy</div>
            <select value={strategy} onChange={e=>setStrategy(e.target.value)}
              style={{padding:"6px 10px",borderRadius:6,border:`1px solid ${C.border}`,background:C.surf,color:C.text,fontSize:12,cursor:"pointer",minWidth:180}}>
              {screeners?.map(s=><option key={s.filename} value={s.filename}>{s.name}</option>)}
            </select>
          </div>

          {/* Frequency */}
          <div style={{display:"flex",flexDirection:"column",gap:4}}>
            <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>Frequency</div>
            <select value={frequency} onChange={e=>setFrequency(e.target.value)}
              style={{padding:"6px 10px",borderRadius:6,border:`1px solid ${C.border}`,background:C.surf,color:C.text,fontSize:12,cursor:"pointer"}}>
              {["1 day","1 week","1 month"].map(f=><option key={f}>{f}</option>)}
            </select>
          </div>

          {/* Data Source */}
          <div style={{display:"flex",flexDirection:"column",gap:4}}>
            <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>Data Source</div>
            <div style={{display:"flex",flexDirection:"column",gap:6,padding:"6px 10px",borderRadius:6,border:`1px solid ${C.border}`,background:C.surf}}>
              <label style={{display:"flex",alignItems:"center",gap:6,cursor:"pointer",fontSize:12}}>
                <input type="radio" checked={useGcs} onChange={()=>setUseGcs(true)} style={{accentColor:C.accent}}/>
                <span style={{color:useGcs?C.accent:C.muted}}>GCS Universe (all 50)</span>
              </label>
              <label style={{display:"flex",alignItems:"center",gap:6,cursor:"pointer",fontSize:12}}>
                <input type="radio" checked={!useGcs} onChange={()=>setUseGcs(false)} style={{accentColor:C.accent}}/>
                <span style={{color:!useGcs?C.accent:C.muted}}>Single Stock</span>
              </label>
              {!useGcs&&(
                <input value={singleSymbol} onChange={e=>setSingle(e.target.value.toUpperCase())}
                  placeholder="e.g. RELIANCE"
                  style={{padding:"4px 8px",borderRadius:4,border:`1px solid ${C.accent}`,background:C.card,color:C.text,fontSize:12,width:120,marginTop:2}}/>
              )}
            </div>
          </div>

          {/* Position Sizing */}
          <div style={{display:"flex",flexDirection:"column",gap:4}}>
            <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>Position Sizing</div>
            <div style={{display:"flex",flexDirection:"column",gap:6,padding:"6px 10px",borderRadius:6,border:`1px solid ${C.border}`,background:C.surf}}>
              <select value={sizingType} onChange={e=>setSizingType(e.target.value)}
                style={{padding:"4px 8px",borderRadius:4,border:`1px solid ${C.border}`,background:C.card,color:C.text,fontSize:11,cursor:"pointer"}}>
                <option value="pct_capital">% of Capital</option>
                <option value="fixed_amount">Fixed Amount (₹)</option>
                <option value="fixed_qty">Fixed Qty (shares)</option>
                <option value="full_capital">Full Capital</option>
              </select>
              {sizingType !== "full_capital" && (
                <div style={{display:"flex",alignItems:"center",gap:6}}>
                  <span style={{fontSize:10,color:C.muted}}>{sizingLabel}:</span>
                  <input type="number" value={sizingValue} onChange={e=>setSizingVal(+e.target.value)}
                    style={{padding:"3px 6px",borderRadius:4,border:`1px solid ${C.border}`,background:C.card,color:C.text,fontSize:12,width:80}}/>
                </div>
              )}
            </div>
          </div>

          {/* Dates + Capital */}
          <div style={{display:"flex",flexDirection:"column",gap:6}}>
            <div style={{display:"flex",gap:8}}>
              <div style={{display:"flex",flexDirection:"column",gap:4}}>
                <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>From</div>
                <input type="date" value={startDate} onChange={e=>setStartDate(e.target.value)}
                  style={{padding:"6px 10px",borderRadius:6,border:`1px solid ${C.border}`,background:C.surf,color:C.text,fontSize:12}}/>
              </div>
              <div style={{display:"flex",flexDirection:"column",gap:4}}>
                <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>To</div>
                <input type="date" value={endDate} onChange={e=>setEndDate(e.target.value)}
                  style={{padding:"6px 10px",borderRadius:6,border:`1px solid ${C.border}`,background:C.surf,color:C.text,fontSize:12}}/>
              </div>
            </div>
            <div style={{display:"flex",flexDirection:"column",gap:4}}>
              <div style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>Capital (₹)</div>
              <input type="number" value={capital} onChange={e=>setCapital(e.target.value)}
                style={{padding:"6px 10px",borderRadius:6,border:`1px solid ${C.border}`,background:C.surf,color:C.text,fontSize:12,width:130}}/>
            </div>
          </div>

          {/* Buttons */}
          <div style={{marginLeft:"auto",display:"flex",gap:8,alignItems:"flex-end",paddingBottom:2}}>
            {results&&(
              <button onClick={exportPDF}
                style={{padding:"7px 16px",borderRadius:6,border:`1px solid ${C.warn}`,background:"#f59e0b22",color:C.warn,fontSize:12,fontWeight:600,cursor:"pointer"}}>
                📄 Export PDF
              </button>
            )}
            <button onClick={runBacktest} disabled={running||!connected}
              style={{padding:"7px 20px",borderRadius:6,border:`1px solid ${running||!connected?C.border:C.accent}`,background:running?"#00d4aa11":!connected?"transparent":"#00d4aa22",color:running||!connected?C.muted:C.accent,fontSize:12,fontWeight:700,cursor:running||!connected?"not-allowed":"pointer"}}>
              {running?"Running...":"▶ Run Backtest"}
            </button>
          </div>
        </div>
        {error&&<div style={{marginTop:10,padding:"8px 12px",borderRadius:6,background:"#ef444422",border:`1px solid ${C.red}`,color:C.red,fontSize:11}}>{error}</div>}
      </Card>

      {results&&<>
        {/* Section tabs */}
        <div style={{display:"flex",gap:0,borderBottom:`1px solid ${C.border}`}}>
          {["summary","charts","trades","quality"].map(s=>(
            <button key={s} onClick={()=>setSection(s)}
              style={{padding:"8px 20px",background:"transparent",border:"none",borderBottom:`2px solid ${section===s?C.accent:"transparent"}`,color:section===s?C.accent:C.muted,cursor:"pointer",fontSize:12,fontWeight:section===s?600:400,textTransform:"capitalize"}}>
              {s==="quality"?"Data Quality":s.charAt(0).toUpperCase()+s.slice(1)}
            </button>
          ))}
        </div>

        {/* SUMMARY */}
        {section==="summary"&&<>
          <div style={{fontSize:11,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em"}}>Profitability</div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:8}}>
            <MetricCard label="Net Profit"       value={`₹${fmt(results.net_profit)}`}          color={pnlCol(results.net_profit)}/>
            <MetricCard label="Gross Profit"      value={`₹${fmt(results.gross_profit)}`}         color={C.green}/>
            <MetricCard label="Gross Loss"        value={`₹${fmt(results.gross_loss)}`}           color={C.red}/>
            <MetricCard label="Profit Factor"     value={results.profit_factor}                    color={results.profit_factor>1?C.green:C.red}/>
            <MetricCard label="Total Return"      value={`${results.total_return_pct}%`}           color={pnlCol(results.total_return_pct)}/>
            <MetricCard label="Buy & Hold"        value={`${results.buy_hold_return_pct}%`}        color={C.muted} sub="benchmark"/>
            <MetricCard label="Monthly Avg"       value={`${results.monthly_avg_return}%`}         color={pnlCol(results.monthly_avg_return)}/>
            <MetricCard label="Monthly Std Dev"   value={`${results.monthly_std}%`}                color={C.muted}/>
          </div>
          <div style={{fontSize:11,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em",marginTop:4}}>Trades</div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:8}}>
            <MetricCard label="Total Trades"      value={results.total_trades}                     color={C.muted}/>
            <MetricCard label="Winning"           value={results.winning_trades}                   color={C.green}/>
            <MetricCard label="Losing"            value={results.losing_trades}                    color={C.red}/>
            <MetricCard label="Win Rate"          value={`${results.win_rate}%`}                   color={results.win_rate>50?C.green:C.red}/>
            <MetricCard label="Avg P&L/Trade"     value={`₹${fmt(results.avg_pnl_per_trade,2)}`}  color={pnlCol(results.avg_pnl_per_trade)}/>
            <MetricCard label="Largest Winner"    value={`₹${fmt(results.max_profit)}`}            color={C.green} sub={`${results.largest_winner_pct}% of gross`}/>
            <MetricCard label="Largest Loser"     value={`₹${fmt(results.max_loss)}`}              color={C.red}   sub={`${results.largest_loser_pct}% of gross`}/>
            <MetricCard label="Max Consec Losses" value={results.max_consec_losses}                color={C.red}/>
            <MetricCard label="Avg Win Days"      value={results.avg_win_days}                     color={C.green}/>
            <MetricCard label="Avg Loss Days"     value={results.avg_loss_days}                    color={C.red}/>
            <MetricCard label="Long Trades"       value={results.long_trades}                      color={C.green}/>
            <MetricCard label="Short Trades"      value={results.short_trades}                     color={C.red}/>
          </div>
          <div style={{fontSize:11,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em",marginTop:4}}>Risk</div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:8}}>
            <MetricCard label="Max Drawdown"      value={`₹${fmt(results.max_drawdown)}`}         color={C.red}/>
            <MetricCard label="Max Drawdown %"    value={`${results.max_drawdown_pct}%`}           color={C.red}/>
            <MetricCard label="Sharpe Ratio"      value={results.sharpe_ratio}                     color={results.sharpe_ratio>1?C.green:results.sharpe_ratio>0?C.warn:C.red}/>
            <MetricCard label="MAR Ratio"         value={results.mar_ratio}                        color={results.mar_ratio>1?C.green:C.warn} sub="net gain% / max DD%"/>
          </div>
          {results.stock_summary?.length>0&&<>
            <div style={{fontSize:11,color:C.muted,textTransform:"uppercase",letterSpacing:"0.06em",marginTop:4}}>Per Stock</div>
            <Card style={{padding:0,overflow:"hidden"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                <thead><tr style={{background:C.surf,borderBottom:`1px solid ${C.border}`}}>
                  {["Symbol","Total P&L","Win Rate","Trades","Best","Worst","Long","Short"].map(h=>(
                    <th key={h} style={{padding:"8px 10px",color:C.muted,textAlign:"left",fontSize:10,fontWeight:400}}>{h}</th>
                  ))}
                </tr></thead>
                <tbody>
                  {results.stock_summary.map(s=>(
                    <tr key={s.symbol} style={{borderBottom:`1px solid ${C.border}22`}}>
                      <td style={{padding:"7px 10px",fontWeight:600,color:C.text}}>{s.symbol}</td>
                      <td style={{padding:"7px 10px",fontFamily:"monospace",color:pnlCol(s.total_pnl)}}>₹{fmt(s.total_pnl)}</td>
                      <td style={{padding:"7px 10px",fontFamily:"monospace",color:s.win_rate>50?C.green:C.red}}>{s.win_rate}%</td>
                      <td style={{padding:"7px 10px",fontFamily:"monospace",color:C.muted}}>{s.total_trades}</td>
                      <td style={{padding:"7px 10px",fontFamily:"monospace",color:C.green}}>₹{fmt(s.best_trade)}</td>
                      <td style={{padding:"7px 10px",fontFamily:"monospace",color:C.red}}>₹{fmt(s.worst_trade)}</td>
                      <td style={{padding:"7px 10px",fontFamily:"monospace",color:C.green}}>{s.long_trades}</td>
                      <td style={{padding:"7px 10px",fontFamily:"monospace",color:C.red}}>{s.short_trades}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Card>
          </>}
        </>}

        {/* CHARTS */}
        {section==="charts"&&<>
          <Card>
            <Label>Equity Curve</Label>
            <ResponsiveContainer width="100%" height={200}>
              <AreaChart data={results.equity_curve}>
                <defs><linearGradient id="eq" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor={C.accent} stopOpacity={0.3}/><stop offset="95%" stopColor={C.accent} stopOpacity={0}/></linearGradient></defs>
                <XAxis dataKey="date" tick={{fontSize:9,fill:C.muted}} interval={Math.floor((results.equity_curve?.length||1)/6)}/>
                <YAxis tick={{fontSize:9,fill:C.muted}} width={70} tickFormatter={v=>"₹"+(v/1000).toFixed(0)+"k"}/>
                <Tooltip {...tt} formatter={v=>["₹"+fmt(v),"Equity"]}/>
                <ReferenceLine y={results.capital} stroke={C.muted} strokeDasharray="4 4" strokeOpacity={0.4}/>
                <Area type="monotone" dataKey="value" stroke={C.accent} strokeWidth={2} fill="url(#eq)"/>
              </AreaChart>
            </ResponsiveContainer>
          </Card>
          <Card>
            <Label>Underwater Curve (Drawdown %)</Label>
            <ResponsiveContainer width="100%" height={160}>
              <AreaChart data={results.underwater_curve}>
                <defs><linearGradient id="uw" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor={C.red} stopOpacity={0}/><stop offset="95%" stopColor={C.red} stopOpacity={0.4}/></linearGradient></defs>
                <XAxis dataKey="date" tick={{fontSize:9,fill:C.muted}} interval={Math.floor((results.underwater_curve?.length||1)/6)}/>
                <YAxis tick={{fontSize:9,fill:C.muted}} width={45} tickFormatter={v=>v+"%"}/>
                <Tooltip {...tt} formatter={v=>[v+"%","Drawdown"]}/>
                <ReferenceLine y={0} stroke={C.muted} strokeDasharray="4 4" strokeOpacity={0.4}/>
                <Area type="monotone" dataKey="drawdown" stroke={C.red} strokeWidth={1.5} fill="url(#uw)"/>
              </AreaChart>
            </ResponsiveContainer>
          </Card>
        </>}

        {/* TRADE LOG */}
        {section==="trades"&&(
          <Card style={{padding:0,overflow:"hidden"}}>
            <div style={{overflowX:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:11,minWidth:800}}>
                <thead><tr style={{background:C.surf,borderBottom:`1px solid ${C.border}`}}>
                  {["#","Symbol","Dir","Entry Date","Entry ₹","Exit Date","Exit ₹","Shares","P&L","Return%","Days","Result"].map(h=>(
                    <th key={h} style={{padding:"8px 10px",color:C.muted,textAlign:"left",fontSize:10,fontWeight:400}}>{h}</th>
                  ))}
                </tr></thead>
                <tbody>
                  {results.trade_log?.map((t,i)=>(
                    <tr key={i} style={{borderBottom:`1px solid ${C.border}22`}}>
                      <td style={{padding:"6px 10px",color:C.muted}}>{i+1}</td>
                      <td style={{padding:"6px 10px",fontWeight:600}}>{t.symbol}</td>
                      <td style={{padding:"6px 10px",color:t.direction==="LONG"?C.green:C.red,fontWeight:600}}>{t.direction}</td>
                      <td style={{padding:"6px 10px",color:C.muted,fontFamily:"monospace"}}>{t.entry_date}</td>
                      <td style={{padding:"6px 10px",fontFamily:"monospace"}}>₹{fmt(t.entry_price,2)}</td>
                      <td style={{padding:"6px 10px",color:C.muted,fontFamily:"monospace"}}>{t.exit_date}</td>
                      <td style={{padding:"6px 10px",fontFamily:"monospace"}}>₹{fmt(t.exit_price,2)}</td>
                      <td style={{padding:"6px 10px",fontFamily:"monospace",color:C.muted}}>{t.shares}</td>
                      <td style={{padding:"6px 10px",fontFamily:"monospace",color:pnlCol(t.pnl)}}>₹{fmt(t.pnl,2)}</td>
                      <td style={{padding:"6px 10px",fontFamily:"monospace",color:pnlCol(t.pnl_pct)}}>{t.pnl_pct}%</td>
                      <td style={{padding:"6px 10px",color:C.muted}}>{t.hold_days}d</td>
                      <td style={{padding:"6px 10px"}}>
                        <span style={{padding:"2px 8px",borderRadius:4,fontSize:10,background:t.win?"#22c55e22":"#ef444422",color:t.win?C.green:C.red}}>
                          {t.win?"WIN":"LOSS"}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        )}

        {/* DATA QUALITY */}
        {section==="quality"&&(
          <Card>
            <Label>Data Quality Report</Label>
            <div style={{display:"flex",flexDirection:"column",gap:8}}>
              {results.data_quality?.filter(q=>q.fetched>0).map((q,i)=>(
                <div key={i} style={{padding:"10px 12px",borderRadius:6,background:C.surf,border:`1px solid ${C.border}`}}>
                  <div style={{display:"flex",gap:16,alignItems:"center",flexWrap:"wrap"}}>
                    <span style={{fontWeight:600,fontSize:12,color:C.text,minWidth:100}}>{q.symbol}</span>
                    <span style={{fontSize:11,color:C.muted}}>Fetched: <span style={{color:C.text,fontFamily:"monospace"}}>{q.fetched}</span></span>
                    <span style={{fontSize:11,color:C.muted}}>Dropped: <span style={{color:C.red,fontFamily:"monospace"}}>{q.dropped}</span></span>
                    <span style={{fontSize:11,color:C.muted}}>Filled: <span style={{color:q.filled>0?C.warn:C.green,fontFamily:"monospace"}}>{q.filled}</span></span>
                    <span style={{fontSize:11,color:C.muted}}>Final: <span style={{color:C.accent,fontFamily:"monospace"}}>{q.final}</span></span>
                    {q.warnings?.length===0&&<span style={{fontSize:10,color:C.green}}>✓ Clean</span>}
                  </div>
                  {q.warnings?.map((w,j)=>(
                    <div key={j} style={{fontSize:10,color:C.warn,marginTop:4}}>⚠ {w}</div>
                  ))}
                </div>
              ))}
            </div>
          </Card>
        )}
      </>}

      {!results&&!running&&(
        <div style={{textAlign:"center",padding:"40px",color:C.muted,fontSize:12,border:`1px dashed ${C.border}`,borderRadius:8}}>
          {connected?"Select strategy, configure options and click Run Backtest":"Connect to TWS first"}
        </div>
      )}
    </div>
  );
}


// ── STRATEGY BUILDER ─────────────────────────────────────────
function StrategyBuilder({selected,chain}){
  const [legs,setLegs]=useState([{id:1,type:"call",strike:22300,action:"sell",qty:1},{id:2,type:"put",strike:21900,action:"sell",qty:1}]);
  const [tpl,setTpl]=useState("strangle");
  const TMPLS={straddle:[{type:"call",strike:22100,action:"sell",qty:1},{type:"put",strike:22100,action:"sell",qty:1}],strangle:[{type:"call",strike:22300,action:"sell",qty:1},{type:"put",strike:21900,action:"sell",qty:1}],iron_condor:[{type:"call",strike:22400,action:"buy",qty:1},{type:"call",strike:22200,action:"sell",qty:1},{type:"put",strike:21800,action:"sell",qty:1},{type:"put",strike:21600,action:"buy",qty:1}],bull_spread:[{type:"call",strike:22100,action:"buy",qty:1},{type:"call",strike:22300,action:"sell",qty:1}]};
  const apply=t=>{if(TMPLS[t]){setLegs(TMPLS[t].map((l,i)=>({...l,id:i+1})));setTpl(t);}};
  const update=(id,k,v)=>setLegs(l=>l.map(x=>x.id===id?{...x,[k]:v}:x));
  const payoff=useMemo(()=>Array.from({length:41},(_,i)=>{const S=SPOT-2000+i*100;let pnl=0;legs.forEach(leg=>{const row=chain.find(r=>r.strike===leg.strike);const prem=row?row[leg.type].ltp:0;const intr=leg.type==="call"?Math.max(S-leg.strike,0):Math.max(leg.strike-S,0);pnl+=(leg.action==="sell"?-1:1)*(intr-prem)*leg.qty*50;});return{spot:S,pnl:Math.round(pnl)};}),[legs,chain]);
  const maxP=Math.max(...payoff.map(p=>p.pnl));const maxL=Math.min(...payoff.map(p=>p.pnl));
  const bes=payoff.filter((p,i)=>i>0&&((payoff[i-1].pnl<0&&p.pnl>=0)||(payoff[i-1].pnl>=0&&p.pnl<0)));
  const netPrem=legs.reduce((s,leg)=>{const row=chain.find(r=>r.strike===leg.strike);const ltp=row?row[leg.type].ltp:0;return s+(leg.action==="sell"?ltp:-ltp)*leg.qty*50;},0);
  return(<div style={{display:"flex",flexDirection:"column",gap:16}}>
    <div style={{display:"flex",gap:8,flexWrap:"wrap",alignItems:"center"}}>
      <span style={{fontSize:11,color:C.muted}}>Templates:</span>
      {Object.keys(TMPLS).map(t=>(<button key={t} onClick={()=>apply(t)} style={{padding:"4px 10px",borderRadius:6,border:`1px solid ${tpl===t?C.accent:C.border}`,background:tpl===t?"#00d4aa22":"transparent",color:tpl===t?C.accent:C.muted,fontSize:11,cursor:"pointer"}}>{t.replace("_"," ")}</button>))}
      <button onClick={()=>setLegs(l=>[...l,{id:Date.now(),type:"call",strike:22000,action:"buy",qty:1}])} style={{marginLeft:"auto",padding:"4px 12px",borderRadius:6,border:`1px solid ${C.warn}`,background:"#f59e0b11",color:C.warn,fontSize:11,cursor:"pointer"}}>+ Add leg</button>
    </div>
    <Card style={{padding:0,overflow:"auto"}}>
      <table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:520}}>
        <thead><tr style={{background:C.surf,borderBottom:`1px solid ${C.border}`}}>{["Action","Type","Strike","Qty","Premium","Δ Delta","Θ Theta",""].map(h=><th key={h} style={{padding:"8px 10px",color:C.muted,textAlign:"left",fontSize:10,fontWeight:400,letterSpacing:"0.05em"}}>{h}</th>)}</tr></thead>
        <tbody>{legs.map(leg=>{const row=chain.find(r=>r.strike===leg.strike);const g=row?row[leg.type]:null;return(<tr key={leg.id} style={{borderBottom:`1px solid ${C.border}33`}}>
          <td style={{padding:"7px 10px"}}><select value={leg.action} onChange={e=>update(leg.id,"action",e.target.value)} style={{border:`1px solid ${leg.action==="sell"?C.red:C.green}`,color:leg.action==="sell"?C.red:C.green,background:C.card,borderRadius:4,padding:"3px 6px"}}><option value="buy">BUY</option><option value="sell">SELL</option></select></td>
          <td style={{padding:"7px 10px"}}><select value={leg.type} onChange={e=>update(leg.id,"type",e.target.value)} style={{color:leg.type==="call"?C.call:C.put,background:C.card,border:`1px solid ${C.border}`,borderRadius:4,padding:"3px 6px"}}><option value="call">CALL</option><option value="put">PUT</option></select></td>
          <td style={{padding:"7px 10px"}}><select value={leg.strike} onChange={e=>update(leg.id,"strike",+e.target.value)} style={{background:C.card,color:C.text,border:`1px solid ${C.border}`,borderRadius:4,padding:"3px 6px"}}>{STRIKES.map(s=><option key={s} value={s}>{s}</option>)}</select></td>
          <td style={{padding:"7px 10px"}}><input type="number" min={1} max={20} value={leg.qty} onChange={e=>update(leg.id,"qty",+e.target.value)} style={{width:48,background:C.card,color:C.text,border:`1px solid ${C.border}`,borderRadius:4,padding:"3px 6px"}}/></td>
          <td style={{padding:"7px 10px",color:leg.action==="sell"?C.green:C.red,fontFamily:"monospace"}}>{g?"₹"+fmt(g.ltp,2):"—"}</td>
          <td style={{padding:"7px 10px",fontFamily:"monospace",color:C.text}}>{g?g.delta:"—"}</td>
          <td style={{padding:"7px 10px",fontFamily:"monospace",color:C.warn}}>{g?g.theta:"—"}</td>
          <td style={{padding:"7px 10px"}}><button onClick={()=>setLegs(l=>l.filter(x=>x.id!==leg.id))} style={{background:"transparent",border:"none",color:C.muted,fontSize:16,cursor:"pointer"}}>×</button></td>
        </tr>);})}</tbody>
      </table>
    </Card>
    <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:10}}>
      <StatCard label="Net Premium" value={"₹"+Math.round(netPrem)} color={netPrem>0?C.green:C.red}/>
      <StatCard label="Max Profit" value={maxP>50000?"Unlimited":"₹"+fmt(maxP)} color={C.green}/>
      <StatCard label="Max Loss" value={maxL<-50000?"Unlimited":"₹"+fmt(maxL)} color={C.red}/>
      <StatCard label="Breakeven(s)" value={bes.length?bes.map(b=>b.spot).join(" / "):"—"} color={C.warn}/>
    </div>
    <Card><Label>Payoff at Expiry (1 lot = 50 units)</Label>
      <ResponsiveContainer width="100%" height={170}><AreaChart data={payoff}>
        <defs>
          <linearGradient id="gp" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor={C.green} stopOpacity={0.4}/><stop offset="95%" stopColor={C.green} stopOpacity={0}/></linearGradient>
          <linearGradient id="gl" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor={C.red} stopOpacity={0}/><stop offset="95%" stopColor={C.red} stopOpacity={0.4}/></linearGradient>
        </defs>
        <XAxis dataKey="spot" tick={{fontSize:9,fill:C.muted}} interval={5}/>
        <YAxis tick={{fontSize:9,fill:C.muted}} width={65} tickFormatter={v=>"₹"+(v/1000).toFixed(1)+"k"}/>
        <Tooltip {...tt} formatter={v=>["₹"+fmt(v),"P&L"]} labelFormatter={v=>"Spot: ₹"+fmt(v)}/>
        <ReferenceLine y={0} stroke={C.muted} strokeDasharray="4 4" strokeOpacity={0.5}/>
        <ReferenceLine x={SPOT} stroke={C.accent} strokeDasharray="4 4" strokeOpacity={0.7}/>
        <Area type="monotone" dataKey="pnl" stroke={C.accent} strokeWidth={2.5} fill={payoff[20]?.pnl>=0?"url(#gp)":"url(#gl)"} name="P&L"/>
      </AreaChart></ResponsiveContainer>
    </Card>
  </div>);
}

// ── MAIN APP ──────────────────────────────────────────────────
export default function App(){
  const [tab,setTab]=useState("screener");
  const chain=useMemo(generateChain,[]);
  const oiData=useMemo(generateOI,[]);
  const [selected,setSelected]=useState([]);
  const [connected,setConnected]=useState(false);
  const [screeners,setScreeners]=useState([]);
  const [connecting,setConnecting]=useState(false);
  const toggleStrike=useCallback(s=>setSelected(p=>p.includes(s)?p.filter(x=>x!==s):[...p,s]),[]);

  // Check connection status on load
  useEffect(()=>{
    fetch(`${API}/status`)
      .then(r=>r.json())
      .then(d=>setConnected(d.tws_connected))
      .catch(()=>{});
  },[]);

  useEffect(()=>{
    fetch(`${API}/screeners`)
      .then(r=>r.json())
      .then(d=>setScreeners(d.screeners||[]))
      .catch(()=>{});
  },[]);

  const connectTWS=async()=>{
    setConnecting(true);
    try{
      const res=await fetch(`${API}/connect`,{method:"POST"});
      const data=await res.json();
      setConnected(data.status==="connected"||data.status==="already_connected");
    }catch(e){
      alert("Could not connect. Is server.py running?");
    }
    setConnecting(false);
  };

  const TABS=[
    {id:"screener", label:"Screener"},
    {id:"chain",    label:"Option Chain"},
    {id:"oi",       label:"PCR & OI"},
    {id:"backtest", label:"Backtest"},
    {id:"strategy", label:"Strategy Builder", badge:selected.length||null},
  ];

  return(
    <div style={{minHeight:"100vh",background:C.bg,color:C.text,fontFamily:"system-ui,sans-serif"}}>
      <div style={{borderBottom:`1px solid ${C.border}`,padding:"0 20px",display:"flex",alignItems:"center",gap:20,height:50,position:"sticky",top:0,background:C.bg,zIndex:100}}>
        <span style={{fontSize:13,fontWeight:700,color:C.accent,letterSpacing:"0.12em",fontFamily:"monospace"}}>OPTIONLAB</span>
        <span style={{fontSize:9,color:C.muted,letterSpacing:"0.08em"}}>INDIA</span>
        <div style={{display:"flex",alignItems:"baseline",gap:8,marginLeft:8}}>
          <span style={{fontSize:11,color:C.muted}}>NIFTY 50</span>
          <span style={{fontSize:15,fontWeight:700,fontFamily:"monospace"}}>₹{fmt(SPOT)}</span>
          <span style={{fontSize:12,color:C.green,fontFamily:"monospace"}}>+0.43%</span>
        </div>
        <div style={{marginLeft:"auto",display:"flex",alignItems:"center",gap:12}}>
          {connected&&<span style={{fontSize:11,color:C.green}}>● TWS Connected</span>}
          <button onClick={connectTWS} disabled={connecting||connected}
            style={{padding:"6px 16px",borderRadius:6,border:`1px solid ${connected?C.green:C.accent}`,background:connected?"#22c55e22":"#00d4aa22",color:connected?C.green:C.accent,fontSize:11,fontWeight:600,cursor:connected?"default":"pointer"}}>
            {connecting?"Connecting...":connected?"✓ Connected":"Connect to TWS"}
          </button>
        </div>
      </div>
      <div style={{borderBottom:`1px solid ${C.border}`,padding:"0 20px",display:"flex"}}>
        {TABS.map(t=>(
          <button key={t.id} onClick={()=>setTab(t.id)} style={{padding:"11px 18px",background:"transparent",border:"none",borderBottom:`2px solid ${tab===t.id?C.accent:"transparent"}`,color:tab===t.id?C.accent:C.muted,cursor:"pointer",fontSize:12,fontWeight:tab===t.id?600:400,display:"flex",alignItems:"center",gap:6}}>
            {t.label}
            {t.badge&&<span style={{background:C.warn,color:"#000",borderRadius:10,padding:"1px 6px",fontSize:10,fontWeight:700}}>{t.badge}</span>}
          </button>
        ))}
      </div>
      <div style={{padding:"18px 20px",maxWidth:1400,margin:"0 auto",paddingBottom:24}}>
        {tab==="screener"&&<ScreenerTab connected={connected}/>}
        {tab==="chain"&&<OptionChain chain={chain} selected={selected} onToggle={toggleStrike}/>}
        {tab==="oi"&&<OIPanel data={oiData}/>}
        {tab==="backtest"&&<BacktestPanel connected={connected} screeners={screeners}/>}
        {tab==="strategy"&&<StrategyBuilder selected={selected} chain={chain}/>}
      </div>
    </div>
  );
}
