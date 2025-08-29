#include "opencv2/stream/webpage.hpp"
#include "common.hpp"

namespace cv {
namespace stream {
namespace webpage {

// Public MIME strings
const char* const kContentTypeHtml = "text/html; charset=utf-8";
const char* const kContentTypeJs   = "application/javascript; charset=utf-8";
const char* const kContentTypeCss  = "text/css; charset=utf-8";

namespace internal {

// --- escaping helpers --------------------------------------------------------
std::string html_escape(const std::string& s) {
    std::string out; out.reserve(s.size()+16);
    for (char c : s) {
        switch (c) {
            case '&':  out += "&amp;";  break;
            case '<':  out += "&lt;";   break;
            case '>':  out += "&gt;";   break;
            case '"':  out += "&quot;"; break;
            case '\'': out += "&#39;";  break;
            default:   out += c;        break;
        }
    }
    return out;
}
std::string js_escape(const std::string& s) {
    std::string out; out.reserve(s.size()+16);
    for (char c : s) {
        switch (c) {
            case '\\': out += "\\\\"; break;
            case '"':  out += "\\\""; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:   out += c;      break;
        }
    }
    return out;
}

// --- CSS + HTML shells -------------------------------------------------------
static std::string css_default_text() {
    return
R"(html,body{margin:0;padding:0;background:#0b0f14;color:#e6edf3;font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,Noto Sans,sans-serif;}
a{color:#61dafb;text-decoration:none}
.cv-wrap{display:flex;flex-direction:row;gap:16px;padding:16px}
.cv-col{flex:1;min-width:0}
.cv-video, .cv-canvas{width:100%;max-width:100%;background:#000;display:block;border-radius:12px}
.cv-img{max-width:100%;border-radius:12px;display:block}
.cv-side{width:320px;flex:0 0 320px}
.cv-card{background:#11161d;border:1px solid #233041;border-radius:12px;margin-bottom:12px;padding:12px}
.cv-card-title{font-weight:700;margin-bottom:8px;color:#a6b2bf}
.cv-row{display:flex;flex-direction:column;margin:8px 0}
.cv-lab{font-size:13px;margin-bottom:4px;color:#9fb0c0}
.cv-ctrl{padding:8px;border-radius:8px;border:1px solid #2a3a4d;background:#0e141b;color:#e6edf3}
.cv-hint{font-size:12px;color:#7f8fa4;margin-top:4px}
.cv-tabs{display:flex;gap:8px;margin-bottom:8px}
.cv-tab{background:#11161d;color:#a6b2bf;border:1px solid #233041;border-radius:999px;padding:6px 12px;cursor:pointer}
.cv-tab.active{background:#1a2430;color:#fff}
.cv-muted{opacity:.7}
)";
}

std::string default_css() { return css_default_text(); }

std::string shell_head(const std::string& title) {
    std::string safe = html_escape(title.empty() ? "OpenCV Stream" : title);
    std::string css  = default_css();
    std::string out;
    out.reserve(safe.size() + css.size() + 256);
    out += "<!doctype html><html><head><meta charset='utf-8'>"
           "<meta name='viewport' content='width=device-width, initial-scale=1'>"
           "<title>"; out += safe; out += "</title>"
           "<style>"; out += css; out += "</style>"
           "</head><body>";
    return out;
}
std::string shell_tail() { return "</body></html>"; }

// --- JS builders (verbatim from original, plus heartbeat keep-alive) --------
std::string build_js_webrtc(const std::string& signaling_path) {
    std::ostringstream js;
    js <<
R"((() => {
  const q = new URL(location.href).searchParams;
  const h = new URL(location.href).hash.replace(/^#/, '');
  const token = q.get('t') || (h.startsWith('t=') ? h.slice(2) : '');

  function log(...args){ console.log('[webrtc]', ...args); }

  async function startWebRTC(rootId, signalingUrl) {
    const v = document.getElementById(rootId + '-video');
    if (!v) { console.error('video element missing'); return; }

    const pc = new RTCPeerConnection({});
    const pendingCandidates = [];
    let remoteSet = false;

    async function safeAddCandidate(candInit) {
      if (!remoteSet) { pendingCandidates.push(candInit); return; }
      if (!candInit || !candInit.candidate) {
        try { await pc.addIceCandidate(null); } catch {}
        return;
      }
      try { await pc.addIceCandidate(candInit); }
      catch (e) { console.warn('[webrtc] addIceCandidate failed (post-remote):', e, candInit); }
    }
    async function drainCandidates() {
      while (pendingCandidates.length) {
        const c = pendingCandidates.shift();
        await safeAddCandidate(c);
      }
    }

    pc.ontrack = e => { if (e.streams && e.streams[0]) v.srcObject = e.streams[0]; };
    pc.onconnectionstatechange = () => log('pc state =', pc.connectionState);

    const ws = new WebSocket(signalingUrl.replace(/^http/, 'ws'));

    // --- heartbeat to prevent server idle-timeout ---
    const KA_MS = 10000; // 10s
    let kaTimer = null;

    ws.onopen = () => {
      log('ws open');
      if (token) ws.send('T ' + token);
      kaTimer = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          try { ws.send('ka'); } catch {}
        }
      }, KA_MS);
    };
    ws.onclose = ev => {
      if (kaTimer) { clearInterval(kaTimer); kaTimer = null; }
      log('ws close', ev.code, ev.reason);
    };
    ws.onerror = err => {
      if (kaTimer) { clearInterval(kaTimer); kaTimer = null; }
      console.error('ws error', err);
    };

    ws.onmessage = async (ev) => {
      if (typeof ev.data !== 'string') return;
      let msg;
      try { msg = JSON.parse(ev.data); }
      catch { return; }

      if (msg.type && msg.sdp) {
        const desc = new RTCSessionDescription(msg);
        try {
          await pc.setRemoteDescription(desc);
        } catch (e) {
          console.error('[webrtc] setRemoteDescription failed:', e);
          return;
        }
        remoteSet = true;
        await drainCandidates();

        if (desc.type === 'offer') {
          const answer = await pc.createAnswer();
          await pc.setLocalDescription(answer);
          ws.send(JSON.stringify(answer));
        }
        return;
      }

      if (Object.prototype.hasOwnProperty.call(msg, 'candidate')) {
        const ice = { ...msg };
        const sdp = (pc.remoteDescription && pc.remoteDescription.sdp) || '';
        const hasAnyMid = /(^|\r\n)a=mid:/m.test(sdp);
        if (!hasAnyMid && 'sdpMid' in ice) {
          delete ice.sdpMid;
        }
        await safeAddCandidate(ice);
        return;
      }

      if (msg.error) {
        console.error('webrtc error', msg);
        return;
      }
    };

    pc.onicecandidate = ev => {
      ws.send(JSON.stringify(ev.candidate || { candidate: '' }));
    };
  }

  window.__cv_webrtc_start = (rootId, url) => startWebRTC(rootId, url);
  document.addEventListener('DOMContentLoaded', () => {
    const root = document.getElementById('cv-webrtc-root');
    if (root) startWebRTC('cv-webrtc', ')" << js_escape(signaling_path) << R"(');
  });
})();)";
    return js.str();
}

std::string build_js_fmp4(const std::string& ws_media_path) {
    std::ostringstream js;
    js <<
R"((() => {
  const q = new URL(location.href).searchParams;
  const h = new URL(location.href).hash.replace(/^#/, '');
  const token = q.get('t') || (h.startsWith('t=') ? h.slice(2) : '');

  function log(...args){ console.log('[fmp4]', ...args); }

  function u32(b, o){ return (b[o]<<24)|(b[o+1]<<16)|(b[o+2]<<8)|b[o+3]; }

  function sniffCodecFromInit(buf) {
    const bytes = new Uint8Array(buf);
    const posAvcC = (() => {
      for (let p=0; p+8<=bytes.length; ) {
        const size = u32(bytes, p);
        if (size < 8) break;
        const typ = String.fromCharCode(bytes[p+4],bytes[p+5],bytes[p+6],bytes[p+7]);
        if (typ === 'avcC') return p;
        p += size;
      }
      return -1;
    })();
    if (posAvcC >= 0) {
      const off = posAvcC + 8;
      if (off+4 <= bytes.length) {
        const profile = bytes[off+1];
        const compat  = bytes[off+2];
        const level   = bytes[off+3];
        const hex = (v)=>('00'+v.toString(16)).slice(-2).toUpperCase();
        return 'video/mp4; codecs=\"avc1.'+hex(profile)+hex(compat)+hex(level)+'\"';
      }
    }
    const posAv1C = (() => {
      for (let p=0; p+8<=bytes.length; ) {
        const size = u32(bytes, p);
        if (size < 8) break;
        const typ = String.fromCharCode(bytes[p+4],bytes[p+5],bytes[p+6],bytes[p+7]);
        if (typ === 'av1C') return p;
        p += size;
      }
      return -1;
    })();
    if (posAv1C >= 0) {
      return 'video/mp4; codecs=\"av01.0.08M.08\"';
    }
    return 'video/mp4; codecs=\"avc1.42E01E\"';
  }

  function startMSE(rootId, wsUrl) {
    const v = document.getElementById(rootId + '-video');
    if (!v) { console.error('video element missing'); return; }
    const ms = new MediaSource();
    v.src = URL.createObjectURL(ms);

    let sb = null;
    let queue = [];

    function appendNext() {
      if (!sb || sb.updating || queue.length === 0) return;
      const seg = queue.shift();
      try { sb.appendBuffer(seg); } catch(e) { console.error('appendBuffer failed', e); }
    }

    ms.addEventListener('sourceopen', () => {});

    const ws = new WebSocket(wsUrl.replace(/^http/, 'ws'));
    ws.binaryType = 'arraybuffer';
    ws.onopen = () => { if (token) ws.send('T ' + token); };
    ws.onmessage = (ev) => {
      if (!(ev.data instanceof ArrayBuffer)) return;
      if (!sb) {
        const mime = sniffCodecFromInit(ev.data);
        try { sb = ms.addSourceBuffer(mime); }
        catch(e) { console.error('addSourceBuffer failed for', mime, e); return; }
        sb.mode = 'segments';
        sb.addEventListener('updateend', appendNext);
      }
      queue.push(ev.data);
      appendNext();
    };
    ws.onclose = ()=>log('ws closed');
    ws.onerror = (e)=>console.error('ws error', e);
  }

  window.__cv_fmp4_start = (rootId, url) => startMSE(rootId, url);
  document.addEventListener('DOMContentLoaded', () => {
    const root = document.getElementById('cv-fmp4-root');
    if (root) startMSE('cv-fmp4', ')" << js_escape(ws_media_path) << R"(');
  });
})();)";
    return js.str();
}

std::string build_js_raw(const std::string& ws_media_path) {
    std::ostringstream js;
    js <<
R"( (() => {
  const url = new URL(location.href);
  const q = url.searchParams;
  const h = url.hash.replace(/^#/, '');
  const token = q.get('t') || (h.startsWith('t=') ? h.slice(2) : '');
  const VERBOSE = (q.get('debug') === '1') || h.includes('debug=1');
  const AS_RGB  = (q.get('rgb') === '1')   || h.includes('rgb=1');

  function info(...a){ console.info('[raw]', ...a); }
  function log (...a){ if (VERBOSE) console.log('[raw]', ...a); }
  function warn(...a){ console.warn('[raw]', ...a); }
  function err (...a){ console.error('[raw]', ...a); }

  function asciiPreview(u8, n=64){
    const len = Math.min(n, u8.length);
    let s = '';
    for (let i=0;i<len;++i){ const c=u8[i]; s += (c>=32 && c<127)?String.fromCharCode(c):'.'; }
    return s;
  }

  function parseHeader(u8) {
    let i=0; while (i<u8.length && u8[i]!==10 && u8[i]!==13) i++;
    if (i>=u8.length) return null;
    let j = i+1; if (j<u8.length && u8[i]===13 && u8[j]===10) j++;
    const head = new TextDecoder().decode(u8.subarray(0,i));
    const m = head.match(/^RAW\s+(\d+)x(\d+)(?:\s.*)?$/);
    if (!m) return null;
    return { w:parseInt(m[1],10), h:parseInt(m[2],10), off:j };
  }

  function packToRgba(src, w, h, asRgb){
    const rgba = new Uint8ClampedArray(w*h*4);
    let si=0, di=0;
    if (asRgb) {
      for (let y=0;y<h;++y) for (let x=0;x<w;++x) {
        const r=src[si++], g=src[si++], b=src[si++];
        rgba[di++]=r; rgba[di++]=g; rgba[di++]=b; rgba[di++]=255;
      }
    } else {
      for (let y=0;y<h;++y) for (let x=0;x<w;++x) {
        const b=src[si++], g=src[si++], r=src[si++];
        rgba[di++]=r; rgba[di++]=g; rgba[di++]=b; rgba[di++]=255;
      }
    }
    return rgba;
  }

  function startRaw(rootId, wsUrl) {
    const c = document.getElementById(rootId + '-canvas');
    if (!c) { err('canvas not found:', rootId + '-canvas'); return; }
    const ctx = c.getContext('2d', {alpha:false, desynchronized:true});

    const u = new URL(wsUrl, location.href);
    u.protocol = (location.protocol === 'https:') ? 'wss:' : 'ws:';
    info('connecting', u.toString(), 'token?', !!token, 'rgb?', AS_RGB);

    let acc = new Uint8Array(0);
    let needBytes = 0;
    let w = 0, h = 0;
    let gotFirst = false;

    const ws = new WebSocket(u.toString());
    ws.binaryType = 'arraybuffer';

    const watchdog = setTimeout(() => warn('no data after 3000ms (check WS path, server, token)'), 3000);

    ws.onopen = () => {
      info('ws open');
      if (token) { const msg = 'T ' + token; log('send token', msg); ws.send(msg); }
    };
    ws.onerror = (e) => err('ws error', e);
    ws.onclose = (e) => { warn('ws close', e.code, e.reason); clearTimeout(watchdog); };

    let frames = 0, t0 = performance.now();

    function concat(a,b){ const out = new Uint8Array(a.length + b.length); out.set(a,0); out.set(b,a.length); return out; }

    function tryConsume() {
      while (true) {
        if (needBytes === 0) {
          const hdr = parseHeader(acc);
          if (!hdr) return;
          w = hdr.w; h = hdr.h;
          needBytes = w*h*3;
          acc = acc.subarray(hdr.off);
          if (c.width !== w || c.height !== h) { c.width=w; c.height=h; info('resize canvas', w, h); }
        }
        if (acc.length < needBytes) return;
        const frame = acc.subarray(0, needBytes);
        acc = acc.subarray(needBytes);
        needBytes = 0;

        const t1 = performance.now();
        const rgba = packToRgba(frame, w, h, AS_RGB);
        const img  = new ImageData(rgba, w, h);
        ctx.putImageData(img, 0, 0);
        const t2 = performance.now();

        if (!gotFirst) { clearTimeout(watchdog); info('first frame rendered'); gotFirst = true; }
        frames++;
        const now = performance.now();
        if (now - t0 > 2000) {
          const fps = (frames * 1000) / (now - t0);
          frames = 0; t0 = now;
          console.log('[raw] fps ~', fps.toFixed(1), 'backlog', acc.length);
        }
      }
    }

    ws.onmessage = ev => {
      if (typeof ev.data === 'string') return;
      if (!(ev.data instanceof ArrayBuffer)) return;
      const u8 = new Uint8Array(ev.data);
      if (needBytes === 0 && !parseHeader(u8)) {
        console.warn('[raw] no header in first chunk; preview:', u8.slice(0,64));
      }
      acc = concat(acc, u8);
      tryConsume();
    };
  }

  window.__cv_raw_start = (rootId, url) => startRaw(rootId, url);
  document.addEventListener('DOMContentLoaded', () => {
    const root = document.getElementById('cv-raw-root');
    if (root) startRaw('cv-raw', ')" << js_escape(ws_media_path) << R"(');
  });
})(); )";
    return js.str();
}

std::string build_js_img_live(const std::string& img_url, int refresh_ms) {
    std::ostringstream js;
    js <<
R"( (() => {
  const url = new URL(location.href);
  const VERBOSE = (url.searchParams.get('debug') === '1') || url.hash.includes('debug=1');
  function log(...a){ if(VERBOSE) console.log('[img]', ...a); }
  function warn(...a){ console.warn('[img]', ...a); }
  function err(...a){ console.error('[img]', ...a); }

  function startLive(imgId, baseUrl, ms) {
    const img = document.getElementById(imgId);
    if (!img) { err('img not found'); return; }

    function tick() {
      const u = new URL(baseUrl, location.href);
      u.searchParams.set('_', Date.now().toString());
      const tgt = u.toString();
      log('refresh ->', tgt);
      const t0 = performance.now();
      img.onload = () => { log('loaded in', (performance.now()-t0).toFixed(2), 'ms'); };
      img.onerror = (e) => { err('load error', e); };
      img.src = tgt;
      if (ms > 0) setTimeout(tick, ms);
    }
    tick();
  }
  window.__cv_img_live = (id, url, ms) => startLive(id, url, ms);
  document.addEventListener('DOMContentLoaded', () => {
    const el = document.getElementById('cv-img');
    if (el) startLive('cv-img', ')" << js_escape(img_url) << R"(', )" << refresh_ms << R"();
  });
})(); )";
    return js.str();
}

std::string build_js_controls_rt(const std::string& ws_ctrl_path) {
    if (ws_ctrl_path.empty()) return "(()=>{})();";
    std::ostringstream js;
    js <<
R"( (() => {
  function el(tag, cls){ const e=document.createElement(tag); if (cls) e.className=cls; return e; }

  function startControls(rootId, wsUrl) {
    const spec = (window.__CV_CTRL_SPEC || []);
    const root = document.getElementById(rootId);
    if (!root || spec.length === 0) return;

    const ws = wsUrl ? new WebSocket(wsUrl.replace(/^http/, 'ws')) : null;

    const groups = {};
    spec.forEach(s => {
      const gname = s.group || 'Controls';
      if (!groups[gname]) groups[gname] = [];
      groups[gname].push(s);
    });

    Object.keys(groups).forEach(g => {
      const card = el('div', 'cv-card');
      const title = el('div', 'cv-card-title'); title.textContent = g;
      card.appendChild(title);

      groups[g].forEach(s => {
        const row = el('div', 'cv-row');
        const lab = el('label', 'cv-lab'); lab.textContent = s.label || s.id;
        row.appendChild(lab);

        let input = null;
        if (s.type === 'Boolean') {
          input = el('input'); input.type='checkbox';
          input.checked = (s.defaultValue === 'true');
        } else if (s.type === 'Enum') {
          input = el('select');
          (s.options||[]).forEach(o => {
            const opt = el('option'); opt.value=o.value; opt.textContent=o.label||o.value;
            if (o.value === s.defaultValue) opt.selected = true;
            input.appendChild(opt);
          });
        } else {
          input = el('input'); input.type = 'text';
          input.value = s.defaultValue || '';
          if (s.type === 'Integer') input.inputMode='numeric';
        }
        if (s.readOnly) input.disabled = true;
        input.className = 'cv-ctrl';
        input.dataset.id = s.id;

        input.addEventListener('change', () => {
          const id = input.dataset.id;
          let val = '';
          if (s.type === 'Boolean') val = input.checked ? 'true' : 'false';
          else val = input.value;

          if (ws && ws.readyState === 1) {
            ws.send(JSON.stringify({id:id, value:val}));
          }
        });

        row.appendChild(input);
        if (s.help) {
          const hint = el('div', 'cv-hint'); hint.textContent = s.help;
          row.appendChild(hint);
        }
        card.appendChild(row);
      });

      root.appendChild(card);
    });

    if (ws) {
      ws.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data);
          if (msg && msg.id && typeof msg.value === 'string') {
            const input = root.querySelector('[data-id="'+msg.id+'"]');
            if (!input) return;
            if (input.type === 'checkbox') input.checked = (msg.value === 'true');
            else input.value = msg.value;
          }
        } catch(e){}
      };
    }
  }

  window.__cv_controls_start = (rootId, url) => startControls(rootId, url);
  document.addEventListener('DOMContentLoaded', () => {
    const root = document.getElementById('cv-controls');
    if (root) startControls('cv-controls', ')" << js_escape(ws_ctrl_path) << R"(');
  });
})(); )";
    return js.str();
}

} // namespace internal
} // namespace webpage
} // namespace stream
} // namespace cv
