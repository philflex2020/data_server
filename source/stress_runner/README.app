# mqtt_viewer — standalone desktop app

The MQTT live viewer (`html/writer/mqtt_viewer.html`) can be packaged as a standalone
Ubuntu desktop executable using three approaches.

---

## Option A — C++ + webview (recommended: small binary, fits existing build)

### Dependencies

```bash
sudo apt install libwebkit2gtk-4.0-dev libgtk-3-dev
```

### One-time setup (download header + local MQTT.js)

```bash
cd source/stress_runner
wget https://raw.githubusercontent.com/webview/webview/master/webview.h
wget https://unpkg.com/mqtt@5.10.1/dist/mqtt.min.js -O mqtt.min.js
```

### Build

```bash
# Add to Makefile or build manually:
g++ -std=c++17 mqtt_viewer_app.cpp -o mqtt_viewer_app \
    $(pkg-config --cflags --libs gtk+-3.0 webkit2gtk-4.0)
```

### Run

```bash
./mqtt_viewer_app --host 192.168.86.46 --port 8083
```

Opens a native GTK/WebKit window (~2 MB binary).
The HTML and MQTT.js are embedded at compile time — no browser or internet required.

---

## Option B — Electron → AppImage (easiest, most portable)

### Dependencies (build machine only)

```bash
sudo apt install nodejs npm
```

### Setup

```bash
mkdir mqtt-viewer-electron && cd mqtt-viewer-electron
npm init -y
npm install electron electron-builder --save-dev

# Copy viewer HTML and download MQTT.js locally
cp ../../html/writer/mqtt_viewer.html index.html
wget https://unpkg.com/mqtt@5.10.1/dist/mqtt.min.js

# Edit index.html: change CDN script tag to local:
#   <script src="mqtt.min.js"></script>
```

`main.js`:
```js
const { app, BrowserWindow } = require('electron')
const path = require('path')

app.whenReady().then(() => {
  const win = new BrowserWindow({ width: 1400, height: 900,
    title: 'MQTT Viewer',
    webPreferences: { nodeIntegration: false }
  })
  win.loadFile('index.html')
})
app.on('window-all-closed', () => app.quit())
```

`package.json` (add build section):
```json
{
  "main": "main.js",
  "scripts": { "start": "electron .", "build": "electron-builder build --linux AppImage" },
  "build": {
    "appId": "com.ems.mqtt-viewer",
    "linux": { "target": "AppImage" }
  }
}
```

### Build AppImage

```bash
npm run build
# Output: dist/mqtt-viewer-1.0.0.AppImage  (~150 MB, runs on any Ubuntu)
```

### Run

```bash
chmod +x dist/mqtt-viewer-*.AppImage
./dist/mqtt-viewer-*.AppImage
```

---

## Option C — Python + PyWebView + PyInstaller

### Dependencies

```bash
pip install pywebview pyinstaller
sudo apt install python3-gi python3-gi-cairo gir1.2-webkit2-4.0
```

### Launcher script (`mqtt_viewer_launcher.py`)

```python
import webview, os, sys

host = sys.argv[1] if len(sys.argv) > 1 else '192.168.86.46'
port = sys.argv[2] if len(sys.argv) > 2 else '8083'
html = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mqtt_viewer.html')

webview.create_window(
    'MQTT Viewer',
    f'file://{html}?host={host}&port={port}',
    width=1400, height=900
)
webview.start()
```

### Build single binary

```bash
pyinstaller --onefile --windowed \
    --add-data "mqtt_viewer.html:." \
    mqtt_viewer_launcher.py
# Output: dist/mqtt_viewer_launcher  (~30 MB)
```

### Note on CDN

For all options, replace the CDN script tag in `mqtt_viewer.html`:
```html
<!-- Change this: -->
<script src="https://unpkg.com/mqtt@5.10.1/dist/mqtt.min.js"></script>
<!-- To this (after downloading the file locally): -->
<script src="mqtt.min.js"></script>
```

---

## FlashMQ WebSocket prerequisite

All approaches require FlashMQ on the target machine to have a WebSocket listener.
On phil-dev (192.168.86.46) this is already configured on port 8083.

For a new machine, add to `/etc/flashmq/flashmq.conf`:
```
listen {
    port 8083
    protocol websockets
}
```
Then `sudo systemctl reload flashmq`.
