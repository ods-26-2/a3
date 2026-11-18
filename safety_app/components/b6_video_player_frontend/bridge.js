// A3-only bridge using the original B6.1 compiled ES module, unchanged.
import { OverlayPlayer } from './b6_overlay_player.js';
const send = (type, fields = {}) => window.parent.postMessage({isStreamlitMessage:true, type, ...fields}, '*');
const player = new OverlayPlayer({containerId:'a3-b6-player', autoPlay:false});
let lastUrl = null;
window.addEventListener('message', event => {
  if (event.source !== window.parent || event.data?.type !== 'streamlit:render') return;
  const args = event.data.args || {};
  if (typeof args.video_url === 'string' && args.video_url !== lastUrl) {
    lastUrl = args.video_url;
    player.loadVideo(lastUrl);
  }
  if (Array.isArray(args.timeline)) player.loadMetadataTimeline(args.timeline);
  requestAnimationFrame(() => send('streamlit:setFrameHeight', {height:Math.max(350, document.documentElement.scrollHeight + 12)}));
});
send('streamlit:componentReady', {apiVersion:1});
