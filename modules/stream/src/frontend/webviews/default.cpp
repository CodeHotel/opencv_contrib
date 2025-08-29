// webpage.cpp
#include <opencv2/stream/webpage.hpp>

#include <algorithm>
#include <sstream>
#include <string>
#include <vector>
#include <cstring>
#include <cstdio>

namespace cv {
namespace stream {
namespace webpage {

namespace {

// ---------------------------------------------------------------------
// tiny buffer helper
// ---------------------------------------------------------------------
static inline std::size_t copy_to(char* dst, std::size_t cap, const std::string& s) {
    if (cap > 0) {
        std::size_t n = std::min(cap - 1, s.size());
        if (n) std::memcpy(dst, s.data(), n);
        dst[n] = '\0';
    }
    return s.size();
}

static inline std::string html_escape(const std::string& s) {
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

static inline std::string js_escape(const std::string& s) {
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

// ============ Core JS runtimes (inline, no external fetch) ===================
//
// Notes:
// - WebRTC: native RTCPeerConnection; WS signaling; token via ?t= or #t= or provided path.
// - fMP4: MediaSource + SourceBuffer; we sniff codec from init segment (supports H.264 via avcC
//         and AV1 via av1C with a reasonable default). We avoid shipping any decoder.
// - Raw: WS frames with minimal "RAW WxH\n" header + BGR bytes; drawn to <canvas> as RGBA.
//
// ============================================================================

static std::string JS_WEBRTC_BASE(const std::string& signaling_path) {
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

    // Accept legacy SDP: don't force bundlePolicy/rtcpMuxPolicy
    const pc = new RTCPeerConnection({});

    // --- buffer ICE candidates until remoteDescription is set ---
    const pendingCandidates = [];
    let remoteSet = false;

    async function safeAddCandidate(candInit) {
      if (!remoteSet) { pendingCandidates.push(candInit); return; }
      // null/empty => end-of-candidates
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
    ws.onopen = () => {
      log('ws open');
      if (token) ws.send('T ' + token);
    };
    ws.onclose = ev => log('ws close', ev.code, ev.reason);
    ws.onerror = err => console.error('ws error', err);

    ws.onmessage = async (ev) => {
      if (typeof ev.data !== 'string') return;
      let msg;
      try { msg = JSON.parse(ev.data); }
      catch { return; } // e.g., "OK"

      // SDP
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

      // ICE
      if (Object.prototype.hasOwnProperty.call(msg, 'candidate')) {
        // Sanitize: legacy SDP may have no a=mid lines; if so, drop sdpMid so
        // the browser applies by m-line index instead of by mid string.
        const ice = { ...msg };
        const sdp = pc.remoteDescription && pc.remoteDescription.sdp || '';
        const hasAnyMid = /(^|\r\n)a=mid:/m.test(sdp);
        if (!hasAnyMid && 'sdpMid' in ice) {
          delete ice.sdpMid;
        }
        // Forward (still buffered if remoteSet=false)
        await safeAddCandidate(ice);
        return;
      }

      if (msg.error) {
        console.error('webrtc error', msg);
        return;
      }
    };

    pc.onicecandidate = ev => {
      // Forward local ICE; null => end-of-candidates
      ws.send(JSON.stringify(ev.candidate || { candidate: '' }));
    };
  }

  // bootstrap if a placeholder is present
  window.__cv_webrtc_start = (rootId, url) => startWebRTC(rootId, url);
  document.addEventListener('DOMContentLoaded', () => {
    const root = document.getElementById('cv-webrtc-root');
    if (root) startWebRTC('cv-webrtc', ')" << js_escape(signaling_path) << R"(');
  });
})();)";
    return js.str();
}



static std::string JS_MSE_FMP4(const std::string& ws_media_path) {
    std::ostringstream js;
    js <<
R"((() => {
  const q = new URL(location.href).searchParams;
  const h = new URL(location.href).hash.replace(/^#/, '');
  const token = q.get('t') || (h.startsWith('t=') ? h.slice(2) : '');

  function log(...args){ console.log('[fmp4]', ...args); }

  function u32(b, o){ return (b[o]<<24)|(b[o+1]<<16)|(b[o+2]<<8)|b[o+3]; }
  function findBox(b, name) {
    // naive scan; fine for init (small)
    const bytes = new Uint8Array(b);
    const len = bytes.length;
    for (let p=0; p+8<=len; ) {
      const size = u32(bytes, p);
      const type = String.fromCharCode(bytes[p+4],bytes[p+5],bytes[p+6],bytes[p+7]);
      if (type === name) return p;
      if (size < 8) break;
      p += size;
    }
    return -1;
  }

  function sniffCodecFromInit(buf) {
    const bytes = new Uint8Array(buf);
    // avcC => H.264
    const posAvcC = (() => {
      // avcC can be nested; do a broader scan
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
        return 'video/mp4; codecs="avc1.'+hex(profile)+hex(compat)+hex(level)+'"';
      }
    }
    // av1C => AV1 (use a safe default string if parsing is tedious)
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
      // Reasonable default that matches 8-bit Main (commonly produced):
      return 'video/mp4; codecs="av01.0.08M.08"';
    }
    // Last resort guesses (try H.264 baseline → high)
    return 'video/mp4; codecs="avc1.42E01E"';
  }

  function startMSE(rootId, wsUrl) {
    const v = document.getElementById(rootId + '-video');
    if (!v) { console.error('video element missing'); return; }
    const ms = new MediaSource();
    v.src = URL.createObjectURL(ms);

    let sb = null;
    let queue = [];
    let opened = false;

    function appendNext() {
      if (!sb || sb.updating || queue.length === 0) return;
      const seg = queue.shift();
      try {
        sb.appendBuffer(seg);
      } catch(e) {
        console.error('appendBuffer failed', e);
      }
    }

    ms.addEventListener('sourceopen', () => {
      opened = true;
    });

    const ws = new WebSocket(wsUrl.replace(/^http/, 'ws'));
    ws.binaryType = 'arraybuffer';
    ws.onopen = () => { if (token) ws.send('T ' + token); };
    ws.onmessage = (ev) => {
      if (!(ev.data instanceof ArrayBuffer)) {
        // likely "OK"
        return;
      }
      if (!sb) {
        const mime = sniffCodecFromInit(ev.data);
        try {
          sb = ms.addSourceBuffer(mime);
        } catch(e) {
          console.error('addSourceBuffer failed for', mime, e);
          return;
        }
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

static std::string JS_RAW(const std::string& ws_media_path) {
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

  // Accept "RAW 640x480" or "RAW 640x480 anything..." (CR/LF tolerant)
  function parseHeader(u8) {
    let i=0; while (i<u8.length && u8[i]!==10 && u8[i]!==13) i++; // \n or \r
    if (i>=u8.length) return null;
    let j = i+1; if (j<u8.length && u8[i]===13 && u8[j]===10) j++; // CRLF
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

    // Build absolute ws(s) URL from relative or http(s) path.
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

    function concat(a,b){
      const out = new Uint8Array(a.length + b.length);
      out.set(a,0); out.set(b,a.length); return out;
    }

    function tryConsume() {
      while (true) {
        if (needBytes === 0) {
          const hdr = parseHeader(acc);
          if (!hdr) return; // wait for more bytes
          w = hdr.w; h = hdr.h;
          needBytes = w*h*3;
          acc = acc.subarray(hdr.off);
          log('header', w, 'x', h, 'need', needBytes, 'remain', acc.length);
          if (c.width !== w || c.height !== h) { c.width=w; c.height=h; info('resize canvas', w, h); }
        }
        if (acc.length < needBytes) return; // wait more
        const frame = acc.subarray(0, needBytes);
        acc = acc.subarray(needBytes);
        needBytes = 0;

        const t1 = performance.now();
        const rgba = packToRgba(frame, w, h, AS_RGB);
        const img  = new ImageData(rgba, w, h);
        ctx.putImageData(img, 0, 0);
        const t2 = performance.now();

        if (!gotFirst) { clearTimeout(watchdog); info('first frame rendered'); gotFirst = true; }
        if (VERBOSE) log(`render ${w}x${h} in ${(t2-t1).toFixed(2)} ms`);

        frames++;
        const now = performance.now();
        if (now - t0 > 2000) {
          const fps = (frames * 1000) / (now - t0);
          log('fps ~', fps.toFixed(1), 'backlog', acc.length);
          frames = 0; t0 = now;
        }
      }
    }

    ws.onmessage = ev => {
      if (typeof ev.data === 'string') { log('text', ev.data.slice(0,128)); return; }
      if (!(ev.data instanceof ArrayBuffer)) { warn('non-ArrayBuffer message ignored'); return; }
      const u8 = new Uint8Array(ev.data);
      log('chunk', u8.length);

      // If header parse keeps failing, show a preview once.
      if (needBytes === 0 && !parseHeader(u8)) {
        warn('no header in chunk; first64="' + asciiPreview(u8) + '" bytes=', u8.length);
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


  static std::string JS_IMG_LIVE(const std::string& img_url, int refresh_ms) {
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

static std::string JS_CONTROLS_RT(const std::string& ws_ctrl_path) {
    std::ostringstream js;
    js <<
R"( (() => {
  // expects window.__CV_CTRL_SPEC = [{id,label,type,defaultValue,minValue,maxValue,step,options:[{value,label}],readOnly,help,group}, ...]
  // minimal UI rendering
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
    return ws_ctrl_path.empty() ? "(()=>{})();" : js.str();
}

static std::string CSS_DEFAULT() {
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

// --------------------- HTML shells ------------------------------------------

static std::string SHELL_HEAD(const std::string& title) {
    std::ostringstream os;
    os << "<!doctype html><html><head><meta charset='utf-8'>"
       << "<meta name='viewport' content='width=device-width, initial-scale=1'>"
       << "<title>" << html_escape(title.empty() ? "OpenCV Stream" : title) << "</title>"
       << "<style>" << CSS_DEFAULT() << "</style>"
       << "</head><body>";
    return os.str();
}

static std::string SHELL_TAIL() {
    return "</body></html>";
}

} // anon

// ============================================================================
// HTML PAGES
// ============================================================================

std::size_t root_page(char* dst, std::size_t cap,
                      const char* mount_path,
                      const char* title) {
    std::ostringstream os;
    os << SHELL_HEAD(title ? title : "OpenCV Stream")
       << "<div class='cv-wrap'><div class='cv-col'>"
       << "<h1 style='margin-top:0'>"
       << html_escape(title ? title : "OpenCV Stream") << "</h1>"
       << "<p class='cv-muted'>This is the stream root. Upstream code should register concrete pages and endpoints.</p>"
       << "<p>Mount path: <code>" << html_escape(mount_path ? mount_path : "/") << "</code></p>"
       << "</div></div>"
       << SHELL_TAIL();
    return copy_to(dst, cap, os.str());
}

// Embedded video (heuristic runtime)
std::size_t embedded_video_page(char* dst, std::size_t cap,
                                const char* stream_path,
                                const char* title) {
    // Heuristic selection
    std::string sp = stream_path ? stream_path : "";
    VideoClient client = VideoClient::Raw;
    if (sp.find("webrtc") != std::string::npos) client = VideoClient::WebRTC;
    else if (sp.find("fmp4") != std::string::npos) client = VideoClient::Fmp4;

    return embedded_video_page(dst, cap, stream_path, client, title);
}

std::size_t embedded_video_page(char* dst, std::size_t cap,
                                const char* stream_path,
                                VideoClient client,
                                const char* title) {
    const std::string sp = stream_path ? stream_path : "";
    std::ostringstream os;
    os << SHELL_HEAD(title ? title : "OpenCV Stream")
       << "<div class='cv-wrap'><div class='cv-col'>";
    if (client == VideoClient::WebRTC) {
        os << "<video id='cv-webrtc-video' class='cv-video' autoplay playsinline controls></video>";
        os << "<script>" << JS_WEBRTC_BASE(sp) << "</script>";
        os << "<script>__cv_webrtc_start('cv-webrtc','" << js_escape(sp) << "');</script>";
    } else if (client == VideoClient::Fmp4) {
        os << "<video id='cv-fmp4-video' class='cv-video' autoplay playsinline controls muted></video>";
        os << "<script>" << JS_MSE_FMP4(sp) << "</script>";
        os << "<script>__cv_fmp4_start('cv-fmp4','" << js_escape(sp) << "');</script>";
    } else { // Raw
        os << "<canvas id='cv-raw-canvas' class='cv-canvas'></canvas>";
        os << "<script>" << JS_RAW(sp) << "</script>";
        os << "<script>__cv_raw_start('cv-raw','" << js_escape(sp) << "');</script>";
    }
    os << "</div></div>" << SHELL_TAIL();
    return copy_to(dst, cap, os.str());
}

// Embedded image
std::size_t embedded_image_page(char* dst, std::size_t cap,
                                const char* image_url,
                                const char* title) {
    std::ostringstream os;
    os << SHELL_HEAD(title ? title : "OpenCV Stream")
       << "<div class='cv-wrap'><div class='cv-col'>"
       << "<img id='cv-img' class='cv-img' src='" << html_escape(image_url?image_url:"/image") << "'>"
       << "</div></div>" << SHELL_TAIL();
    return copy_to(dst, cap, os.str());
}

std::size_t embedded_image_page(char* dst, std::size_t cap,
                                const char* image_url,
                                int refresh_ms,
                                const char* title) {
    std::ostringstream os;
    os << SHELL_HEAD(title ? title : "OpenCV Stream")
       << "<div class='cv-wrap'><div class='cv-col'>"
       << "<img id='cv-img' class='cv-img'>"
       << "</div></div>"
       << "<script>" << JS_IMG_LIVE(image_url?image_url:"/image", refresh_ms) << "</script>"
       << SHELL_TAIL();
    return copy_to(dst, cap, os.str());
}

// Default webview (no controls)
std::size_t webview_default_page(char* dst, std::size_t cap,
                                 const char* mount_path,
                                 const char* ws_url,
                                 const char* title) {
    (void)mount_path;
    // heuristic
    std::string sp = ws_url ? ws_url : "";
    VideoClient client = VideoClient::Raw;
    if (sp.find("webrtc") != std::string::npos) client = VideoClient::WebRTC;
    else if (sp.find("fmp4") != std::string::npos) client = VideoClient::Fmp4;
    return embedded_video_page(dst, cap, ws_url, client, title);
}

// Default webview (with controls)
std::size_t webview_default_page(char* dst, std::size_t cap,
                                 const char* mount_path,
                                 const char* ws_url,
                                 const char* ws_ctrl_url,
                                 const ParamSpec* controls,
                                 std::size_t num_controls,
                                 const char* title) {
    (void)mount_path;
    std::string sp = ws_url ? ws_url : "";
    VideoClient client = VideoClient::Raw;
    if (sp.find("webrtc") != std::string::npos) client = VideoClient::WebRTC;
    else if (sp.find("fmp4") != std::string::npos) client = VideoClient::Fmp4;

    std::ostringstream spec;
    spec << "[";
    for (std::size_t i=0;i<num_controls;i++) {
        const ParamSpec& p = controls[i];
        spec << (i? ",":"") << "{";
        spec << "\"id\":\"" << js_escape(p.id?p.id:"") << "\",";
        spec << "\"label\":\"" << js_escape(p.label?p.label:p.id?p.id:"") << "\",";
        const char* t = "Text";
        switch (p.type) {
            case ParamType::Number:  t="Number"; break;
            case ParamType::Integer: t="Integer"; break;
            case ParamType::Boolean: t="Boolean"; break;
            case ParamType::Enum:    t="Enum"; break;
            case ParamType::Text:    t="Text"; break;
        }
        spec << "\"type\":\"" << t << "\",";
        spec << "\"defaultValue\":\"" << js_escape(p.defaultValue?p.defaultValue:"") << "\",";
        spec << "\"minValue\":" << p.minValue << ",";
        spec << "\"maxValue\":" << p.maxValue << ",";
        spec << "\"step\":" << p.step << ",";
        spec << "\"readOnly\":" << (p.readOnly?"true":"false") << ",";
        spec << "\"help\":\"" << js_escape(p.help?p.help:"") << "\",";
        spec << "\"group\":\"" << js_escape(p.group?p.group:"") << "\",";
        spec << "\"options\":[";
        for (std::size_t k=0;k<p.numOptions;k++) {
            const ParamOption& o = p.options[k];
            if (k) spec << ",";
            spec << "{\"value\":\"" << js_escape(o.value?o.value:"") << "\",\"label\":\"" << js_escape(o.label?o.label:"") << "\"}";
        }
        spec << "]}";
    }
    spec << "]";

    std::ostringstream os;
    os << SHELL_HEAD(title ? title : "OpenCV Stream")
       << "<div class='cv-wrap'>"
       << "  <div class='cv-col'>";

    if (client == VideoClient::WebRTC) {
        os << "<video id='cv-webrtc-video' class='cv-video' autoplay playsinline controls></video>";
        os << "<script>" << JS_WEBRTC_BASE(sp) << "</script>";
        os << "<script>__cv_webrtc_start('cv-webrtc','" << js_escape(sp) << "');</script>";
    } else if (client == VideoClient::Fmp4) {
        os << "<video id='cv-fmp4-video' class='cv-video' autoplay playsinline controls muted></video>";
        os << "<script>" << JS_MSE_FMP4(sp) << "</script>";
        os << "<script>__cv_fmp4_start('cv-fmp4','" << js_escape(sp) << "');</script>";
    } else {
        os << "<canvas id='cv-raw-canvas' class='cv-canvas'></canvas>";
        os << "<script>" << JS_RAW(sp) << "</script>";
        os << "<script>__cv_raw_start('cv-raw','" << js_escape(sp) << "');</script>";
    }

    os << "  </div>";
    os << "  <div class='cv-side'><div id='cv-controls'></div></div>";
    os << "</div>";

    if (num_controls > 0) {
        os << "<script>window.__CV_CTRL_SPEC=" << spec.str() << ";</script>";
        os << "<script>" << JS_CONTROLS_RT(ws_ctrl_url?ws_ctrl_url:"") << "</script>";
        os << "<script>__cv_controls_start('cv-controls','" << js_escape(ws_ctrl_url?ws_ctrl_url:"") << "');</script>";
    }

    os << SHELL_TAIL();
    return copy_to(dst, cap, os.str());
}

// Image-only webview
std::size_t webview_image_page(char* dst, std::size_t cap,
                               const char* mount_path,
                               const char* image_url,
                               int refresh_ms,
                               const char* title) {
    (void)mount_path;
    return embedded_image_page(dst, cap, image_url, refresh_ms, title);
}

// Tabs (video + image)
std::size_t webview_media_page(char* dst, std::size_t cap,
                               const char* mount_path,
                               const char* ws_video_url,
                               VideoClient video_client,
                               const char* image_url,
                               int refresh_ms,
                               const char* ws_ctrl_url,
                               const ParamSpec* controls,
                               std::size_t num_controls,
                               const char* title) {
    (void)mount_path;

    std::ostringstream spec;
    spec << "[";
    for (std::size_t i=0;i<num_controls;i++) {
        const ParamSpec& p = controls[i];
        if (i) spec << ",";
        spec << "{";
        spec << "\"id\":\"" << js_escape(p.id?p.id:"") << "\",";
        spec << "\"label\":\"" << js_escape(p.label?p.label:p.id?p.id:"") << "\",";
        const char* t = "Text";
        switch (p.type) {
            case ParamType::Number:  t="Number"; break;
            case ParamType::Integer: t="Integer"; break;
            case ParamType::Boolean: t="Boolean"; break;
            case ParamType::Enum:    t="Enum"; break;
            case ParamType::Text:    t="Text"; break;
        }
        spec << "\"type\":\"" << t << "\",";
        spec << "\"defaultValue\":\"" << js_escape(p.defaultValue?p.defaultValue:"") << "\",";
        spec << "\"minValue\":" << p.minValue << ",";
        spec << "\"maxValue\":" << p.maxValue << ",";
        spec << "\"step\":" << p.step << ",";
        spec << "\"readOnly\":" << (p.readOnly?"true":"false") << ",";
        spec << "\"help\":\"" << js_escape(p.help?p.help:"") << "\",";
        spec << "\"group\":\"" << js_escape(p.group?p.group:"") << "\",";
        spec << "\"options\":[";
        for (std::size_t k=0;k<p.numOptions;k++) {
            const ParamOption& o = p.options[k];
            if (k) spec << ",";
            spec << "{\"value\":\"" << js_escape(o.value?o.value:"") << "\",\"label\":\"" << js_escape(o.label?o.label:"") << "\"}";
        }
        spec << "]}";
    }
    spec << "]";

    std::ostringstream os;
    os << SHELL_HEAD(title ? title : "OpenCV Stream")
       << "<div class='cv-wrap'>"
       << "  <div class='cv-col'>"
       << "    <div class='cv-tabs'>"
       << "      <div class='cv-tab active' id='tab-video'>Video</div>"
       << "      <div class='cv-tab' id='tab-image'>Image</div>"
       << "    </div>"
       << "    <div id='panel-video'>";

    if (video_client == VideoClient::WebRTC) {
        os << "<video id='cv-webrtc-video' class='cv-video' autoplay playsinline controls></video>";
        os << "<script>" << JS_WEBRTC_BASE(ws_video_url?ws_video_url:"/webrtc") << "</script>";
        os << "<script>__cv_webrtc_start('cv-webrtc','" << js_escape(ws_video_url?ws_video_url:"/webrtc") << "');</script>";
    } else if (video_client == VideoClient::Fmp4) {
        os << "<video id='cv-fmp4-video' class='cv-video' autoplay playsinline controls muted></video>";
        os << "<script>" << JS_MSE_FMP4(ws_video_url?ws_video_url:"/ws/fmp4") << "</script>";
        os << "<script>__cv_fmp4_start('cv-fmp4','" << js_escape(ws_video_url?ws_video_url:"/ws/fmp4") << "');</script>";
    } else {
        os << "<canvas id='cv-raw-canvas' class='cv-canvas'></canvas>";
        os << "<script>" << JS_RAW(ws_video_url?ws_video_url:"/ws/raw") << "</script>";
        os << "<script>__cv_raw_start('cv-raw','" << js_escape(ws_video_url?ws_video_url:"/ws/raw") << "');</script>";
    }

    os << "    </div>"
       << "    <div id='panel-image' style='display:none'>"
       << "      <img id='cv-img' class='cv-img'>"
       << "    </div>"
       << "  </div>"
       << "  <div class='cv-side'><div id='cv-controls'></div></div>"
       << "</div>";

    // tabs script
    os << "<script>(() => {"
          "const tv=document.getElementById('tab-video');"
          "const ti=document.getElementById('tab-image');"
          "const pv=document.getElementById('panel-video');"
          "const pi=document.getElementById('panel-image');"
          "function act(a){ if(a==='v'){tv.classList.add('active');ti.classList.remove('active');pv.style.display='';pi.style.display='none';}"
          "else{ti.classList.add('active');tv.classList.remove('active');pi.style.display='';pv.style.display='none';}}"
          "tv.onclick=()=>act('v'); ti.onclick=()=>act('i');"
          "})();</script>";

    // image live JS
    os << "<script>" << JS_IMG_LIVE(image_url?image_url:"/image", refresh_ms) << "</script>";
    os << "<script>__cv_img_live('cv-img','" << js_escape(image_url?image_url:"/image") << "'," << refresh_ms << ");</script>";

    // controls
    if (num_controls > 0) {
        os << "<script>window.__CV_CTRL_SPEC=" << spec.str() << ";</script>";
        os << "<script>" << JS_CONTROLS_RT(ws_ctrl_url?ws_ctrl_url:"") << "</script>";
        os << "<script>__cv_controls_start('cv-controls','" << js_escape(ws_ctrl_url?ws_ctrl_url:"") << "');</script>";
    }

    os << SHELL_TAIL();
    return copy_to(dst, cap, os.str());
}

// Jupyter pages
std::size_t webview_jupyter_page(char* dst, std::size_t cap,
                                 const char* mount_path,
                                 const char* ws_url) {
    (void)mount_path;
    // heuristic choose
    std::string sp = ws_url ? ws_url : "";
    VideoClient client = VideoClient::Raw;
    if (sp.find("webrtc") != std::string::npos) client = VideoClient::WebRTC;
    else if (sp.find("fmp4") != std::string::npos) client = VideoClient::Fmp4;

    std::ostringstream os;
    os << "<!doctype html><meta charset='utf-8'><style>body{margin:0;background:#000}</style>";
    if (client == VideoClient::WebRTC) {
        os << "<video id='cv-webrtc-video' style='width:100%;height:auto' autoplay playsinline controls></video>";
        os << "<script>" << JS_WEBRTC_BASE(sp) << "</script>";
        os << "<script>__cv_webrtc_start('cv-webrtc','" << js_escape(sp) << "');</script>";
    } else if (client == VideoClient::Fmp4) {
        os << "<video id='cv-fmp4-video' style='width:100%;height:auto' autoplay playsinline controls muted></video>";
        os << "<script>" << JS_MSE_FMP4(sp) << "</script>";
        os << "<script>__cv_fmp4_start('cv-fmp4','" << js_escape(sp) << "');</script>";
    } else {
        os << "<canvas id='cv-raw-canvas' style='width:100%;height:auto;background:#000;display:block'></canvas>";
        os << "<script>" << JS_RAW(sp) << "</script>";
        os << "<script>__cv_raw_start('cv-raw','" << js_escape(sp) << "');</script>";
    }
    return copy_to(dst, cap, os.str());
}

std::size_t webview_jupyter_image_page(char* dst, std::size_t cap,
                                       const char* mount_path,
                                       const char* image_url,
                                       int refresh_ms) {
    (void)mount_path;
    std::ostringstream os;
    os << "<!doctype html><meta charset='utf-8'><style>body{margin:0;background:#000}</style>"
       << "<img id='cv-img' style='max-width:100%;display:block;margin:0 auto'>"
       << "<script>" << JS_IMG_LIVE(image_url?image_url:"/image", refresh_ms) << "</script>"
       << "<script>__cv_img_live('cv-img','" << js_escape(image_url?image_url:"/image") << "'," << refresh_ms << ");</script>";
    return copy_to(dst, cap, os.str());
}

std::size_t webview_jupyter_media_page(char* dst, std::size_t cap,
                                       const char* mount_path,
                                       const char* ws_video_url,
                                       VideoClient video_client,
                                       const char* image_url,
                                       int refresh_ms,
                                       const char* ws_ctrl_url,
                                       const ParamSpec* controls,
                                       std::size_t num_controls) {
    (void)mount_path; (void)ws_ctrl_url; (void)controls; (void)num_controls;
    std::ostringstream os;
    os << "<!doctype html><meta charset='utf-8'><style>body{margin:0;background:#000;color:#fff;font-family:sans-serif} .t{display:flex;gap:8px;padding:8px} .b{padding:6px 10px;border:1px solid #333;border-radius:999px;cursor:pointer} .a{background:#222}</style>"
       << "<div class='t'><div class='b a' id='tv'>Video</div><div class='b' id='ti'>Image</div></div>"
       << "<div id='pv'>";
    if (video_client == VideoClient::WebRTC) {
        os << "<video id='cv-webrtc-video' style='width:100%;height:auto' autoplay playsinline controls></video>"
           << "<script>" << JS_WEBRTC_BASE(ws_video_url?ws_video_url:"/webrtc") << "</script>"
           << "<script>__cv_webrtc_start('cv-webrtc','" << js_escape(ws_video_url?ws_video_url:"/webrtc") << "');</script>";
    } else if (video_client == VideoClient::Fmp4) {
        os << "<video id='cv-fmp4-video' style='width:100%;height:auto' autoplay playsinline controls muted></video>"
           << "<script>" << JS_MSE_FMP4(ws_video_url?ws_video_url:"/ws/fmp4") << "</script>"
           << "<script>__cv_fmp4_start('cv-fmp4','" << js_escape(ws_video_url?ws_video_url:"/ws/fmp4") << "');</script>";
    } else {
        os << "<canvas id='cv-raw-canvas' style='width:100%;height:auto;background:#000;display:block'></canvas>"
           << "<script>" << JS_RAW(ws_video_url?ws_video_url:"/ws/raw") << "</script>"
           << "<script>__cv_raw_start('cv-raw','" << js_escape(ws_video_url?ws_video_url:"/ws/raw") << "');</script>";
    }
    os << "</div><div id='pi' style='display:none'><img id='cv-img' style='max-width:100%;display:block;margin:0 auto'></div>"
       << "<script>" << JS_IMG_LIVE(image_url?image_url:"/image", refresh_ms) << "</script>"
       << "<script>__cv_img_live('cv-img','" << js_escape(image_url?image_url:"/image") << "'," << refresh_ms << ");</script>"
       << "<script>(()=>{const tv=document.getElementById('tv'),ti=document.getElementById('ti'),pv=document.getElementById('pv'),pi=document.getElementById('pi');"
          "tv.onclick=()=>{tv.classList.add('a');ti.classList.remove('a');pv.style.display='';pi.style.display='none';};"
          "ti.onclick=()=>{ti.classList.add('a');tv.classList.remove('a');pi.style.display='';pv.style.display='none';};})();</script>";
    return copy_to(dst, cap, os.str());
}

// ============================================================================
// JavaScript asset functions (standalone)
// ============================================================================

std::size_t js_webrtc(char* dst, std::size_t cap, const char* signaling_path) {
    return copy_to(dst, cap, JS_WEBRTC_BASE(signaling_path?signaling_path:"/webrtc"));
}
std::size_t js_fmp4(char* dst, std::size_t cap, const char* ws_media_path) {
    return copy_to(dst, cap, JS_MSE_FMP4(ws_media_path?ws_media_path:"/ws/fmp4"));
}
std::size_t js_raw(char* dst, std::size_t cap, const char* ws_media_path) {
    return copy_to(dst, cap, JS_RAW(ws_media_path?ws_media_path:"/ws/raw"));
}
std::size_t js_image_live(char* dst, std::size_t cap, const char* image_url, int refresh_ms) {
    return copy_to(dst, cap, JS_IMG_LIVE(image_url?image_url:"/image", refresh_ms));
}
std::size_t js_controls_runtime(char* dst, std::size_t cap, const char* ws_ctrl_path) {
    return copy_to(dst, cap, JS_CONTROLS_RT(ws_ctrl_path?ws_ctrl_path:""));
}

// ============================================================================
// CSS
// ============================================================================
std::size_t css_default_theme(char* dst, std::size_t cap) {
    return copy_to(dst, cap, CSS_DEFAULT());
}

} // namespace webpage
} // namespace stream
} // namespace cv
