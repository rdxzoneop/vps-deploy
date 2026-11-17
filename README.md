# Discord VPS Bot (discord.py v2, MySQL, LXD + Docker)

## Setup
```bash
sudo ./install.sh        # installs deps, MySQL, Docker, LXD, prompts for .env, starts systemd service
journalctl -u vpsbot -f  # logs
```
Manual: `cp .env.example .env`, fill it, `python3 -m venv venv && venv/bin/pip install -r requirements.txt && venv/bin/python bot.py`.

Discord portal: enable **Server Members Intent**; invite bot with `applications.commands` + `bot` scopes
(Manage Server permission is needed for invite tracking).

## Command map (Discord can't do some spaced names, so)
| Spec | Command |
|---|---|
| /vps-shred revoke | `/vps-shred-revoke` |
| /invite bord, /invite @user | `/invite-board`, `/invite-user` |
| /deploy os, /deploy ram core ssd os | `/deploy-os`, `/deploy-plan` |
| /vps @user 30 | `/vps-extend user days` |
| /termius,/tmate,/sshx allow/deny | `/tool name allow` |
| /lxc on/off | `/lxc mode` |
| /set invite/boost | `/set-count` |
| /admin all delete | `/admin wipe` |

## Notes
- RAM masking: cgroup limit + **lxcfs** (installed by install.sh). LXD uses lxcfs natively; Docker gets lxcfs `/proc` bind mounts.
- Disk limit on Docker needs an xfs+pquota backing fs; otherwise falls back to no disk cap (LXD enforces it natively).
- Cooldown/rate-limit use Redis if `REDIS_URL` is set, else MySQL / memory.
- Invite flow: below `REQUIRED_INVITES` after a leave → warning DM → `CHECKIN_WINDOW_HOURS` to re-invite and `/check-in` → else invites reset + VPS suspended.
- Discord buttons can't be custom-colored; primary/secondary/danger styles are used.
