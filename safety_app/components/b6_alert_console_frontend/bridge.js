// A3-only Streamlit adapter: public B6.3 API, no alert UI or business logic replicated.
import { AlertConsole } from './b6_alert_console.js';

const send = (type, fields = {}) => window.parent.postMessage({isStreamlitMessage: true, type, ...fields}, '*');
const reportHeight = () => send('streamlit:setFrameHeight', {height: Math.max(260, document.documentElement.scrollHeight + 16)});
const action = (kind, alertId = null, timestamp = null) => {
  send('streamlit:setComponentValue', {value: {
    kind, alert_id: alertId, timestamp,
    request_id: (globalThis.crypto && crypto.randomUUID) ? crypto.randomUUID() : String(Date.now()) + '-' + String(Math.random())
  }});
};
const consoleView = new AlertConsole({
  containerId: 'a3-b6-alerts',
  onAcknowledge: id => action('ACKNOWLEDGE', id),
  onMute: id => action('MUTE', id),
  onClose: id => action('CLOSE', id),
  onFocusTimeline: (id, timestamp) => action('FOCUS', id, timestamp),
  onAcknowledgeAll: () => action('ACKNOWLEDGE_ALL'),
});
window.addEventListener('message', event => {
  if (event.source !== window.parent || event.data?.type !== 'streamlit:render') return;
  const data = event.data.args || {};
  const alerts = Array.isArray(data.alerts) ? data.alerts : [];
  // B6 owns the alert presentation, quick filters, buttons and focus behavior.
  consoleView.loadAlerts(alerts);
  requestAnimationFrame(reportHeight);
});
send('streamlit:componentReady', {apiVersion:1});
reportHeight();
