# S32DS 3.5 — Install & Run from WSL via X11

Target: S32DS 3.5 installed on **phil-dev** (192.168.86.46), launched from a
Windows/WSL machine using XLaunch for the GUI.

---

## 1. Install VcXsrv on Windows

Download and install **VcXsrv** (includes XLaunch):
https://sourceforge.net/projects/vcxsrv/

---

## 2. Launch XLaunch (every session)

Run **XLaunch** from the Start menu with these settings:

| Screen | Setting |
|---|---|
| Display settings | Multiple windows |
| Display number | 0 |
| Session type | Start no client |
| Extra settings | **Check "Disable access control"** |

Click **Finish**. VcXsrv icon appears in the system tray.

> "Disable access control" is required — without it, remote X11 connections
> are rejected even with correct auth cookies.

---

## 3. Configure WSL (one-time)

In WSL, add to `~/.bashrc`:

```bash
export DISPLAY=$(grep -m1 nameserver /etc/resolv.conf | awk '{print $2}'):0
```

Then reload:
```bash
source ~/.bashrc
```

---

## 4. Install S32DS on phil-dev (one-time)

Copy the installer to phil-dev (or download directly there), then:

```bash
# From WSL — open X11 forwarding SSH session as root
ssh -X phil@192.168.86.46 "sudo -E DISPLAY=\$DISPLAY /tmp/S32DS.3.5_b220726_linux.x86_64.bin"
```

- The GUI installer window appears on your Windows desktop via XLaunch.
- When asked for install location, accept the default (`/root/NXP/S32DS.3.5`).
- Complete the wizard normally.

> **Note:** The installer must run as root (`sudo -E`) because it calls `sudo`
> internally during installation. Running as phil causes a "not a sudoer" error
> mid-install.

> **Note:** `sudo -E` preserves the SSH X11 `DISPLAY` and `XAUTHORITY`
> environment variables so the GUI reaches your Windows display.

### Prerequisites on phil-dev (already configured)

`/etc/sudoers.d/phil` must contain:
```
phil ALL=(ALL) NOPASSWD:ALL
Defaults:phil !requiretty
Defaults:phil !use_pty
Defaults:phil timestamp_timeout=-1
```

---

## 5. Run S32DS

```bash
ssh -X phil@192.168.86.46 "sudo -E /root/NXP/S32DS.3.5/s32ds.sh"
```

The S32DS IDE window opens on your Windows desktop via XLaunch.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `X11 connection rejected because of wrong authentication` | Restart XLaunch with "Disable access control" checked |
| `Warning: No xauth data; using fake authentication data` | Harmless warning — proceed |
| `Graphical installers are not supported by the VM` | X11 not reaching the process — ensure `sudo -E` not just `sudo` |
| S32DS window doesn't appear | Check VcXsrv is running (system tray icon) |
| Installer says "not a sudoer" mid-install | Must run installer with `sudo -E`, not as phil directly |
