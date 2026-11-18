import { ZoneEditor } from './b6_zone_editor.js';
const editor = new ZoneEditor('editor');
const status = document.getElementById('status');
function send(type, payload) { window.parent.postMessage({isStreamlitMessage:true,type,...payload}, '*'); }
function reportHeight() { send('streamlit:setFrameHeight',{height:document.body.scrollHeight+10}); }
window.addEventListener('message',e=>{if(e.data?.type==='streamlit:render'){reportHeight();}});
document.getElementById('draw').onclick=()=>{editor.startDrawingMode();status.textContent='Modo desenho ativo.'};
document.getElementById('edit').onclick=()=>{editor.stopDrawingMode();status.textContent='Modo edição ativo.'};
document.getElementById('clear').onclick=()=>{editor.clearAll();status.textContent='Editor limpo. Para apagar zonas salvas, confirme o editor vazio.'};
document.getElementById('export').onclick=()=>{
  const zones=editor.exportAllZones(document.getElementById('name').value.trim()||'Zona',document.getElementById('type').value.trim()||'DANGER');
  send('streamlit:setComponentValue',{value:{zones,submitted_at:Date.now()}});
  status.textContent=`${zones.length} zona(s) enviada(s) ao backend A3.`;
};
send('streamlit:componentReady',{apiVersion:1});reportHeight();
