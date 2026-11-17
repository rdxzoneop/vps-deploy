# -*- coding: utf-8 -*-
"""Discord VPS Deploy Bot - single file. discord.py v2 + aiomysql + LXD/Docker (CLI)."""
import asyncio, json, logging, os, random, secrets, string, time
from datetime import datetime, timedelta
from typing import Literal, Optional

import aiomysql, discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv

load_dotenv()
try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None

E = os.getenv
def _int(k, d): 
    try: return int(E(k, d))
    except (TypeError, ValueError): return int(d)
def _col(k, d):
    try: return int(E(k, d).lstrip('#'), 16)
    except ValueError: return int(d.lstrip('#'), 16)
def _ids(s): return {int(x) for x in (s or '').replace(' ', '').split(',') if x.isdigit()}

BOT_NAME = E('BOT_NAME', 'VPS Bot'); LOGO = E('LOGO_URL') or None
FOOT_LOGO = E('FOOTER_LOGO_URL') or None; FOOT = E('FOOTER_TEXT', 'Powered by VPS Bot')
C_MAIN = _col('EMBED_COLOR', '#0D0D0D'); C_BROWN = _col('BRAND_BROWN', '#8B5E3C'); C_RED = 0xE53935
OWNER_ID = _int('OWNER_ID', 0); ADMIN_IDS = _ids(E('ADMIN_IDS')); GUILD_ID = _int('GUILD_ID', 0)
LOG_CH = _int('LOG_CHANNEL_ID', 0)
DEPLOY_CD = _int('DEPLOY_COOLDOWN', 300); MANAGE_CD = _int('MANAGE_COOLDOWN', 500)
MANAGE_RESET = _int('MANAGE_COOLDOWN_RESET', 100)
REQ_INV = _int('REQUIRED_INVITES', 10); CHECKIN_H = _int('CHECKIN_WINDOW_HOURS', 5)
INV_DAYS = _int('INVITE_VALIDITY_DAYS', 60); DEFAULT_ISO = E('DEFAULT_ISO', 'debian12')
HOST_IP = E('LXC_HOST_IP') or '127.0.0.1'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('vpsbot')
now = datetime.utcnow
pool: Optional[aiomysql.Pool] = None
rds = None

# ───────────────────────── DATABASE ─────────────────────────
SCHEMA = [
"""CREATE TABLE IF NOT EXISTS users (id BIGINT PRIMARY KEY AUTO_INCREMENT, discord_id BIGINT UNIQUE NOT NULL,
 username VARCHAR(64), banned TINYINT DEFAULT 0, role ENUM('owner','admin','mod','user') DEFAULT 'user',
 invites INT DEFAULT 0, boosts INT DEFAULT 0, balance DECIMAL(10,2) DEFAULT 0, limit_ram_mb INT DEFAULT 4096,
 limit_cpu_cores INT DEFAULT 2, limit_ssd_gb INT DEFAULT 20, max_vps INT DEFAULT 2,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"""CREATE TABLE IF NOT EXISTS vps (id BIGINT PRIMARY KEY AUTO_INCREMENT, vps_id CHAR(4) UNIQUE NOT NULL,
 owner_id BIGINT NOT NULL, type ENUM('lxc','docker') NOT NULL, ram_mb INT NOT NULL, cpu_cores INT NOT NULL,
 ssd_gb INT NOT NULL, os VARCHAR(32) NOT NULL, container_name VARCHAR(64) UNIQUE NOT NULL, ip VARCHAR(45),
 port INT, ssh_user VARCHAR(32), ssh_password VARCHAR(128),
 status ENUM('creating','running','stopped','suspended','deleted') DEFAULT 'creating',
 expiry DATE NOT NULL, suspended TINYINT DEFAULT 0, branding_logo_url VARCHAR(255) NULL,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 FOREIGN KEY (owner_id) REFERENCES users(id), INDEX(owner_id), INDEX(status)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"""CREATE TABLE IF NOT EXISTS vps_shares (id BIGINT PRIMARY KEY AUTO_INCREMENT, vps_id BIGINT NOT NULL,
 shared_with BIGINT NOT NULL, shared_by BIGINT NOT NULL, permissions ENUM('full','manage','view') DEFAULT 'manage',
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE KEY uq_share (vps_id, shared_with),
 FOREIGN KEY (vps_id) REFERENCES vps(id) ON DELETE CASCADE, FOREIGN KEY (shared_with) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"""CREATE TABLE IF NOT EXISTS deploy_queue (id BIGINT PRIMARY KEY AUTO_INCREMENT, user_id BIGINT NOT NULL,
 position INT NOT NULL, payload JSON NOT NULL, status ENUM('waiting','processing','done','failed') DEFAULT 'waiting',
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, INDEX(status, position),
 FOREIGN KEY (user_id) REFERENCES users(id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"""CREATE TABLE IF NOT EXISTS cooldowns (user_id BIGINT PRIMARY KEY, deploy_until DATETIME NULL,
 manage_until DATETIME NULL, manage_reset_until DATETIME NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"""CREATE TABLE IF NOT EXISTS settings (`key` VARCHAR(64) PRIMARY KEY, `value` TEXT,
 updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"""CREATE TABLE IF NOT EXISTS invite_log (id BIGINT PRIMARY KEY AUTO_INCREMENT, user_id BIGINT NOT NULL,
 invited_id BIGINT NOT NULL, joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, left_at TIMESTAMP NULL, valid TINYINT DEFAULT 1,
 INDEX(user_id), INDEX(invited_id), FOREIGN KEY (user_id) REFERENCES users(id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"""CREATE TABLE IF NOT EXISTS boost_log (id BIGINT PRIMARY KEY AUTO_INCREMENT, user_id BIGINT NOT NULL,
 boosted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, active TINYINT DEFAULT 1, INDEX(user_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"""CREATE TABLE IF NOT EXISTS audit_log (id BIGINT PRIMARY KEY AUTO_INCREMENT, actor_id BIGINT NOT NULL,
 action VARCHAR(64) NOT NULL, target VARCHAR(128), meta JSON, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 INDEX(actor_id), INDEX(action)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"""CREATE TABLE IF NOT EXISTS global_limits (id TINYINT PRIMARY KEY DEFAULT 1, total_ram_mb INT DEFAULT 131072,
 total_cpu INT DEFAULT 64, total_ssd_gb INT DEFAULT 1024, max_containers INT DEFAULT 100, alert_percent TINYINT DEFAULT 85
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"""CREATE TABLE IF NOT EXISTS admin_roles (id BIGINT PRIMARY KEY AUTO_INCREMENT, user_id BIGINT NOT NULL, perms JSON,
 granted_by BIGINT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"INSERT IGNORE INTO global_limits (id) VALUES (1)",
]
DEFAULT_OS = {
 'debian12': {'label': 'Debian 12', 'lxc': 'images:debian/12', 'docker': 'debian:12'},
 'debian13': {'label': 'Debian 13', 'lxc': 'images:debian/13', 'docker': 'debian:trixie'},
 'ubuntu22': {'label': 'Ubuntu 22.04', 'lxc': 'images:ubuntu/22.04', 'docker': 'ubuntu:22.04'},
 'ubuntu24': {'label': 'Ubuntu 24.04', 'lxc': 'images:ubuntu/24.04', 'docker': 'ubuntu:24.04'}}
DEFAULT_PLANS = [{'name': 'Starter', 'ram': 1024, 'cpu': 1, 'ssd': 10}, {'name': 'Basic', 'ram': 2048, 'cpu': 1, 'ssd': 20},
 {'name': 'Pro', 'ram': 4096, 'cpu': 2, 'ssd': 40}, {'name': 'Max', 'ram': 8192, 'cpu': 4, 'ssd': 50}]

async def q(sql, args=None, fetch=None):
    async with pool.acquire() as c:
        async with c.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, args)
            if fetch == 'one': return await cur.fetchone()
            if fetch == 'all': return await cur.fetchall()
            if fetch == 'id': return cur.lastrowid
            return cur.rowcount

async def get_s(k, d=None):
    r = await q("SELECT `value` FROM settings WHERE `key`=%s", (k,), 'one')
    return r['value'] if r else d
async def set_s(k, v):
    await q("INSERT INTO settings(`key`,`value`) VALUES(%s,%s) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)", (k, str(v)))
async def del_s(k): await q("DELETE FROM settings WHERE `key`=%s", (k,))

async def init_db():
    global pool, rds
    pool = await aiomysql.create_pool(host=E('DB_HOST', 'localhost'), port=_int('DB_PORT', 3306), user=E('DB_USER', 'vpsbot'),
        password=E('DB_PASS', ''), db=E('DB_NAME', 'vpsbot'), charset='utf8mb4', autocommit=True, minsize=1, maxsize=10)
    for s in SCHEMA: await q(s)
    defaults = {'lxc_mode': E('LXC_MODE', 'off'), 'emergency_shutdown': E('EMERGENCY_DEPLOY_OFF', 'false'),
        'termius_allow': 'false', 'sshx_allow': 'false', 'tmate_allow': 'false', 'auto_claim': 'false',
        'about_text': f'{BOT_NAME} — Deploy fast.', 'os_list': json.dumps(DEFAULT_OS), 'plans': json.dumps(DEFAULT_PLANS),
        'default_plan': json.dumps(DEFAULT_PLANS[1])}
    for k, v in defaults.items(): await q("INSERT IGNORE INTO settings(`key`,`value`) VALUES(%s,%s)", (k, v))
    if E('REDIS_URL') and aioredis:
        try:
            rds = aioredis.from_url(E('REDIS_URL'), decode_responses=True); await rds.ping()
        except Exception as ex:
            log.warning('Redis unavailable (%s), falling back to MySQL', ex); rds = None

async def get_user(did, name=None):
    await q("INSERT INTO users(discord_id,username) VALUES(%s,%s) ON DUPLICATE KEY UPDATE username=COALESCE(%s,username)", (did, name, name))
    return await q("SELECT * FROM users WHERE discord_id=%s", (did,), 'one')

# ───────────────────────── ROLES / AUDIT / HELPERS ─────────────────────────
LV = {'user': 0, 'mod': 1, 'admin': 2, 'owner': 3}
async def role_of(did):
    if did == OWNER_ID: return 'owner'
    if did in ADMIN_IDS: return 'admin'
    r = await q("SELECT role FROM users WHERE discord_id=%s", (did,), 'one')
    return r['role'] if r else 'user'

async def need(i, lvl):
    if LV[await role_of(i.user.id)] >= LV[lvl]: return True
    await send(i, embed=emb('⛔ Permission denied', f'Requires **{lvl}** or higher.', C_RED), ephemeral=True); return False

def emb(title, desc=None, color=None):
    e = discord.Embed(title=title, description=desc, color=color or C_MAIN)
    e.set_footer(text=FOOT, icon_url=FOOT_LOGO); return e

async def send(i, **kw):
    if i.response.is_done(): return await i.followup.send(**kw)
    return await i.response.send_message(**kw)

async def audit(actor, action, target='', meta=None):
    await q("INSERT INTO audit_log(actor_id,action,target,meta) VALUES(%s,%s,%s,%s)", (actor, action, str(target)[:128], json.dumps(meta or {}, default=str)))
    ch = client.get_channel(LOG_CH) if LOG_CH else None
    if ch:
        try:
            e = emb('📝 ' + action, f'**Actor:** <@{actor}>\n**Target:** {target}\n```json\n{json.dumps(meta or {}, default=str)[:900]}\n```', C_BROWN)
            await ch.send(embed=e)
        except Exception as ex: log.warning('log channel: %s', ex)

async def dm(did, **kw):
    try:
        u = await client.fetch_user(int(did)); await u.send(**kw); return True
    except Exception: return False

def fmt_ram(mb): return f'{mb // 1024} GB' if mb % 1024 == 0 else f'{mb} MB'
STATUS = {'creating': '🟡 Creating', 'running': '🟢 Running', 'stopped': '🔴 Stopped', 'suspended': '⛔ Suspended', 'deleted': '⚫ Deleted'}
async def os_map(): return json.loads(await get_s('os_list', '{}') or '{}') or DEFAULT_OS
async def plans(): return json.loads(await get_s('plans', '[]') or '[]') or DEFAULT_PLANS
def rand_pw(n=16): return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(n))

async def cd_left(did, col):
    try:
        if rds:
            t = await rds.ttl(f'cd:{col}:{did}')
            if t and t > 0: return t
    except Exception: pass
    r = await q(f"SELECT {col} FROM cooldowns WHERE user_id=%s", (did,), 'one')
    if r and r[col] and r[col] > now(): return int((r[col] - now()).total_seconds())
    return 0
async def cd_set(did, col, secs):
    await q(f"INSERT INTO cooldowns(user_id,{col}) VALUES(%s,%s) ON DUPLICATE KEY UPDATE {col}=VALUES({col})", (did, now() + timedelta(seconds=secs)))
    try:
        if rds: await rds.setex(f'cd:{col}:{did}', secs, 1)
    except Exception: pass

# ───────────────────────── BACKENDS (LXD / Docker via CLI) ─────────────────────────
async def sh(*args, timeout=600):
    try:
        p = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                                                 env={**os.environ, 'PATH': os.environ.get('PATH', '') + ':/snap/bin'})
        out, _ = await asyncio.wait_for(p.communicate(), timeout)
        return p.returncode, out.decode(errors='replace').strip()
    except asyncio.TimeoutError: return 124, 'timeout'
    except FileNotFoundError as ex: return 127, f'{args[0]} not installed ({ex})'

def ssh_script(pw, systemd):
    tail = 'systemctl enable --now ssh || systemctl enable --now sshd' if systemd else 'exec /usr/sbin/sshd -D'
    return ("export DEBIAN_FRONTEND=noninteractive; for n in 1 2 3 4 5; do apt-get update -qq && break; sleep 5; done; "
            "apt-get install -y -qq openssh-server sudo curl procps ca-certificates >/dev/null; mkdir -p /run/sshd; "
            f"echo 'root:{pw}' | chpasswd; sed -i 's/^#\\?PermitRootLogin.*/PermitRootLogin yes/;s/^#\\?PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config; {tail}")

async def bk_create(v, image, pw):
    n, ram, cpu, ssd, port = v['container_name'], v['ram_mb'], v['cpu_cores'], v['ssd_gb'], v['port']
    if v['type'] == 'docker':
        base = ['docker', 'run', '-d', '--name', n, '--hostname', n, '--memory', f'{ram}m', '--memory-swap', f'{ram}m',
                '--cpus', str(cpu), '-p', f'{port}:22', '--restart', 'unless-stopped']
        if os.path.isdir('/var/lib/lxcfs/proc'):   # /proc masking -> container sees only its allocation
            for f in ('cpuinfo', 'meminfo', 'stat', 'uptime', 'diskstats', 'swaps'):
                if os.path.exists(f'/var/lib/lxcfs/proc/{f}'): base += ['-v', f'/var/lib/lxcfs/proc/{f}:/proc/{f}:rw']
        cmd = [image, 'bash', '-c', ssh_script(pw, False)]
        rc, out = await sh(*base[:2], '--storage-opt', f'size={ssd}g', *base[2:], *cmd)
        if rc != 0:
            await sh('docker', 'rm', '-f', n); rc, out = await sh(*base, *cmd)
        if rc != 0: return False, out
        for _ in range(60):
            rc, _o = await sh('docker', 'exec', n, 'sh', '-c', '[ -f /run/sshd.pid ]')
            if rc == 0: return True, ''
            await asyncio.sleep(5)
        return False, 'sshd did not start in time'
    rc, out = await sh('lxc', 'init', image, n, '-c', f'limits.memory={ram}MiB', '-c', f'limits.cpu={cpu}', '-d', f'root,size={ssd}GiB')
    if rc != 0: return False, out
    rc, out = await sh('lxc', 'start', n)
    if rc != 0: return False, out
    for _ in range(30):
        if (await sh('lxc', 'exec', n, '--', 'true'))[0] == 0: break
        await asyncio.sleep(2)
    await asyncio.sleep(5)
    rc, out = await sh('lxc', 'exec', n, '--', 'bash', '-c', ssh_script(pw, True))
    if rc != 0: return False, out
    rc, out = await sh('lxc', 'config', 'device', 'add', n, 'ssh', 'proxy', f'listen=tcp:0.0.0.0:{port}', 'connect=tcp:127.0.0.1:22')
    return (rc == 0), out

async def bk_action(v, act):
    n = v['container_name']
    if v['type'] == 'docker':
        return await sh('docker', 'rm', '-f', n) if act == 'delete' else await sh('docker', act, n)
    if act == 'delete': return await sh('lxc', 'delete', n, '--force')
    return await sh('lxc', act, n, *(['--force'] if act in ('stop', 'restart') else []))

async def bk_exec(v, cmd, timeout=120):
    n = v['container_name']
    return await sh(*(['docker', 'exec', n] if v['type'] == 'docker' else ['lxc', 'exec', n, '--']), *cmd, timeout=timeout)

def _hb(b):
    b = float(b)
    for u in ('B', 'KB', 'MB', 'GB', 'TB'):
        if b < 1024: return f'{b:.1f} {u}'
        b /= 1024
    return f'{b:.1f} PB'
def _tob(s):
    s = s.strip(); mult = {'B': 1, 'KB': 1e3, 'KIB': 1024, 'MB': 1e6, 'MIB': 1024**2, 'GB': 1e9, 'GIB': 1024**3, 'TB': 1e12, 'TIB': 1024**4}
    num = ''.join(c for c in s if c in '0123456789.'); unit = s[len(num):].strip().upper() or 'B'
    return float(num or 0) * mult.get(unit, 1)

async def get_stats(v):
    n = v['container_name']; alloc_ram = v['ram_mb'] * 1024 * 1024; d = {}
    if v['type'] == 'docker':
        rc, out = await sh('docker', 'stats', '--no-stream', '--format', '{{json .}}', n)
        j = json.loads(out) if rc == 0 and out.startswith('{') else {}
        used = _tob(j.get('MemUsage', '0B / 0B').split('/')[0])
        d.update(cpu=j.get('CPUPerc', 'n/a'), ram=f'{_hb(used)} / {fmt_ram(v["ram_mb"])}', net=j.get('NetIO', 'n/a'))
        rc, out = await sh('docker', 'inspect', '-f', '{{.State.StartedAt}}', n)
        try:
            st = datetime.strptime(out[:19], '%Y-%m-%dT%H:%M:%S'); d['up'] = str(now() - st).split('.')[0]
        except Exception: d['up'] = 'n/a'
    else:
        async def st():
            rc, out = await sh('lxc', 'query', f'/1.0/instances/{n}/state'); return json.loads(out) if rc == 0 else {}
        a = await st(); await asyncio.sleep(1); b = await st()
        try:
            dc = b['cpu']['usage'] - a['cpu']['usage']; d['cpu'] = f'{dc / (1e9 * v["cpu_cores"]) * 100:.1f}%'
        except Exception: d['cpu'] = 'n/a'
        d['ram'] = f'{_hb(b.get("memory", {}).get("usage", 0))} / {fmt_ram(v["ram_mb"])}'
        eth = (b.get('network') or {}).get('eth0', {}).get('counters', {})
        d['net'] = f'↓ {_hb(eth.get("bytes_received", 0))} / ↑ {_hb(eth.get("bytes_sent", 0))}'
        rc, out = await bk_exec(v, ['uptime', '-p']); d['up'] = out if rc == 0 else 'n/a'
    rc, out = await bk_exec(v, ['df', '-B1', '/'])
    try:
        used = int(out.splitlines()[-1].split()[2]); d['disk'] = f'{_hb(used)} / {v["ssd_gb"]} GB'
    except Exception: d['disk'] = f'n/a / {v["ssd_gb"]} GB'
    return d

# ───────────────────────── DEPLOY SERVICE ─────────────────────────
async def check_deploy(did, name, ram, cpu, ssd, bypass_cd=False):
    u = await get_user(did, name)
    if u['banned']: return None, 'You are banned from deploying.'
    if await get_s('emergency_shutdown') == 'true': return None, '🛑 Deployments are frozen by admins.'
    if E('AUTO_DEPLOY_ENABLED', 'true').lower() != 'true': return None, 'Auto deploy is disabled.'
    if not bypass_cd and await role_of(did) != 'owner':
        left = await cd_left(did, 'deploy_until')
        if left: return None, f'⏳ Deploy cooldown: **{left}s** left.'
    if await q("SELECT id FROM deploy_queue WHERE user_id=%s AND status IN('waiting','processing')", (u['id'],), 'one'):
        return None, 'You already have a deployment in the queue.'
    m = await q("SELECT COUNT(*) n, COALESCE(SUM(ram_mb),0) r, COALESCE(SUM(cpu_cores),0) c, COALESCE(SUM(ssd_gb),0) s FROM vps WHERE owner_id=%s AND status<>'deleted'", (u['id'],), 'one')
    if m['n'] >= u['max_vps']: return None, f'VPS cap reached ({u["max_vps"]}).'
    if int(m['r']) + ram > u['limit_ram_mb']: return None, f'RAM limit exceeded ({fmt_ram(u["limit_ram_mb"])}).'
    if int(m['c']) + cpu > u['limit_cpu_cores']: return None, f'CPU limit exceeded ({u["limit_cpu_cores"]} cores).'
    if int(m['s']) + ssd > u['limit_ssd_gb']: return None, f'SSD limit exceeded ({u["limit_ssd_gb"]} GB).'
    g = await q("SELECT * FROM global_limits WHERE id=1", fetch='one')
    t = await q("SELECT COUNT(*) n, COALESCE(SUM(ram_mb),0) r, COALESCE(SUM(cpu_cores),0) c, COALESCE(SUM(ssd_gb),0) s FROM vps WHERE status<>'deleted'", fetch='one')
    if t['n'] + 1 > g['max_containers'] or int(t['r']) + ram > g['total_ram_mb'] or int(t['c']) + cpu > g['total_cpu'] or int(t['s']) + ssd > g['total_ssd_gb']:
        return None, '🚫 Host capacity reached. Try later.'
    return u, None

async def enqueue(u, payload):
    pos = (await q("SELECT COALESCE(MAX(position),0)+1 p FROM deploy_queue", fetch='one'))['p']
    await q("INSERT INTO deploy_queue(user_id,position,payload) VALUES(%s,%s,%s)", (u['id'], pos, json.dumps(payload)))
    n = (await q("SELECT COUNT(*) n FROM deploy_queue WHERE status='waiting'", fetch='one'))['n']
    return n

async def free_port():
    used = {r['port'] for r in await q("SELECT port FROM vps WHERE port IS NOT NULL", fetch='all')}
    while True:
        p = random.randint(20000, 40000)
        if p not in used: return p

async def new_vps_id():
    while True:
        c = ''.join(random.choices('0123456789ABCDEF', k=4))
        if not await q("SELECT id FROM vps WHERE vps_id=%s", (c,), 'one'): return c

def creds_embed(v, pw):
    e = emb(f'✅ VPS #{v["vps_id"]} is ready', color=C_BROWN)
    e.description = (f'```\nHost : {v["ip"]}\nPort : {v["port"]}\nUser : root\nPass : {pw}\n```'
                     f'`ssh root@{v["ip"]} -p {v["port"]}`\nChange your password after login.')
    return e

async def process_job(job):
    p = json.loads(job['payload']) if isinstance(job['payload'], str) else job['payload']
    u = await q("SELECT * FROM users WHERE id=%s", (job['user_id'],), 'one')
    try:
        if u['banned']: raise RuntimeError('user banned')
        if await get_s('emergency_shutdown') == 'true': raise RuntimeError('deployments frozen')
        osm = await os_map(); o = osm.get(p['os']) or osm[next(iter(osm))]
        typ = 'lxc' if await get_s('lxc_mode') == 'on' else 'docker'
        vid = await new_vps_id(); port = await free_port(); pw = rand_pw()
        exp = (now() + timedelta(days=30)).date()
        v = dict(vps_id=vid, owner_id=u['id'], type=typ, ram_mb=p['ram'], cpu_cores=p['cpu'], ssd_gb=p['ssd'], os=p['os'],
                 container_name=f'vps-{vid.lower()}', ip=HOST_IP, port=port, ssh_user='root', ssh_password=pw, expiry=exp)
        v['id'] = await q("INSERT INTO vps(vps_id,owner_id,type,ram_mb,cpu_cores,ssd_gb,os,container_name,ip,port,ssh_user,ssh_password,status,expiry) "
                          "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'root',%s,'creating',%s)",
                          (vid, u['id'], typ, p['ram'], p['cpu'], p['ssd'], p['os'], v['container_name'], HOST_IP, port, pw, exp), 'id')
        ok, out = await bk_create(v, o[typ], pw)
        if not ok:
            await bk_action(v, 'delete'); await q("UPDATE vps SET status='deleted',port=NULL WHERE id=%s", (v['id'],))
            raise RuntimeError(out[-300:])
        await q("UPDATE vps SET status='running' WHERE id=%s", (v['id'],))
        await q("UPDATE deploy_queue SET status='done' WHERE id=%s", (job['id'],))
        if not p.get('claim'): await cd_set(u['discord_id'], 'deploy_until', DEPLOY_CD)
        await dm(u['discord_id'], embed=creds_embed(v, pw))
        await audit(u['discord_id'], 'vps_deploy', vid, {'type': typ, 'ram': p['ram'], 'cpu': p['cpu'], 'ssd': p['ssd']})
        g = await q("SELECT * FROM global_limits WHERE id=1", fetch='one')
        r = int((await q("SELECT COALESCE(SUM(ram_mb),0) r FROM vps WHERE status<>'deleted'", fetch='one'))['r'])
        if r * 100 >= g['total_ram_mb'] * g['alert_percent']: await audit(0, 'capacity_alert', 'global', {'ram_used_mb': r, 'total': g['total_ram_mb']})
    except Exception as ex:
        log.exception('deploy failed')
        await q("UPDATE deploy_queue SET status='failed' WHERE id=%s", (job['id'],))
        await dm(u['discord_id'], embed=emb('❌ Deploy failed', f'```{str(ex)[:500]}```', C_RED))

@tasks.loop(seconds=3)
async def deploy_worker():
    if rds:
        try:
            if not await rds.set('lock:deploy_worker', '1', nx=True, ex=900): return
        except Exception: pass
    try:
        job = None
        async with pool.acquire() as c:
            async with c.cursor(aiomysql.DictCursor) as cur:
                await c.begin()
                await cur.execute("SELECT * FROM deploy_queue WHERE status='waiting' ORDER BY position LIMIT 1 FOR UPDATE SKIP LOCKED")
                job = await cur.fetchone()
                if job: await cur.execute("UPDATE deploy_queue SET status='processing' WHERE id=%s", (job['id'],))
                await c.commit()
        if job: await process_job(job)
    finally:
        try:
            if rds: await rds.delete('lock:deploy_worker')
        except Exception: pass

# ───────────────────────── SUSPEND / DELETE / ACCESS ─────────────────────────
async def suspend_vps(v):
    if v['status'] in ('deleted', 'suspended'): return
    await bk_action(v, 'stop'); await q("UPDATE vps SET status='suspended',suspended=1 WHERE id=%s", (v['id'],))
async def revive_vps(v):
    await bk_action(v, 'start'); await q("UPDATE vps SET status='running',suspended=0 WHERE id=%s", (v['id'],))
async def destroy_vps(v):
    await bk_action(v, 'delete'); await q("UPDATE vps SET status='deleted',port=NULL,suspended=0 WHERE id=%s", (v['id'],))

async def user_vps(did, include_shared=True):
    u = await get_user(did)
    rows = await q("SELECT v.*, 'owner' AS access FROM vps v WHERE owner_id=%s AND status<>'deleted'", (u['id'],), 'all')
    if include_shared:
        rows += await q("SELECT v.*, s.permissions AS access FROM vps v JOIN vps_shares s ON s.vps_id=v.id WHERE s.shared_with=%s AND v.status<>'deleted'", (u['id'],), 'all')
    return rows

# ───────────────────────── UI COMPONENTS ─────────────────────────
class B(discord.ui.Button):
    def __init__(s, label, style, cb, row=None, **kw):
        super().__init__(label=label, style=style, row=row, **kw); s.cb = cb
    async def callback(s, i): await s.cb(i)

class Sel(discord.ui.Select):
    def __init__(s, ph, opts, cb, row=None):
        super().__init__(placeholder=ph, options=opts, row=row); s.cb = cb
    async def callback(s, i): await s.cb(i, s.values[0])

class OwnedView(discord.ui.View):
    def __init__(s, uid, timeout=300): super().__init__(timeout=timeout); s.uid = uid
    async def interaction_check(s, i):
        if i.user.id != s.uid: await i.response.send_message('Not your panel.', ephemeral=True); return False
        return True

class ConfirmModal(discord.ui.Modal):
    def __init__(s, title, word, cb):
        super().__init__(title=title[:45]); s.word, s.cb = word, cb
        s.t = discord.ui.TextInput(label=f'Type {word} to confirm', max_length=20); s.add_item(s.t)
    async def on_submit(s, i):
        if s.t.value.strip() != s.word: return await i.response.send_message('Cancelled.', ephemeral=True)
        await s.cb(i)

class PickView(OwnedView):
    def __init__(s, uid, rows, cb):
        super().__init__(uid)
        async def pick(i, val):
            v = next(r for r in rows if str(r['id']) == val); await cb(i, v)
        s.add_item(Sel('Select VPS', [discord.SelectOption(label=f'#{r["vps_id"]} · {fmt_ram(r["ram_mb"])} · {r["status"]}', value=str(r['id'])) for r in rows[:25]], pick))

async def pick_vps(i, target, cb):
    rows = await user_vps(target.id, include_shared=False)
    if not rows: return await send(i, embed=emb('No VPS', f'{target.mention} has no VPS.'), ephemeral=True)
    if len(rows) == 1: return await cb(i, rows[0])
    await send(i, embed=emb('Select a VPS'), view=PickView(i.user.id, rows, cb), ephemeral=True)

# ----- Deploy -----
class DeployView(OwnedView):
    def __init__(s, uid, osm, pl):
        super().__init__(uid); s.os = None; s.plan = None; s.pl = pl
        async def on_os(i, val): s.os = val; await i.response.defer()
        async def on_plan(i, val): s.plan = pl[int(val)]; await i.response.defer()
        s.add_item(Sel('Choose OS', [discord.SelectOption(label=o['label'], value=k) for k, o in osm.items()][:25], on_os, 0))
        s.add_item(Sel('Choose plan', [discord.SelectOption(label=f'{p["name"]} · {fmt_ram(p["ram"])} / {p["cpu"]}C / {p["ssd"]}GB', value=str(n)) for n, p in enumerate(pl)][:25], on_plan, 1))
        s.add_item(B('Confirm deploy', discord.ButtonStyle.primary, s.confirm, 2))
    async def confirm(s, i):
        if not s.os or not s.plan: return await i.response.send_message('Pick an OS and a plan first.', ephemeral=True)
        await i.response.defer()
        u, err = await check_deploy(i.user.id, str(i.user), s.plan['ram'], s.plan['cpu'], s.plan['ssd'])
        if err: return await i.edit_original_response(embed=emb('Cannot deploy', err, C_RED), view=None)
        n = await enqueue(u, {'os': s.os, **{k: s.plan[k] for k in ('ram', 'cpu', 'ssd')}})
        s.stop(); await i.edit_original_response(embed=emb('⏳ Queued', f'Waiting **{n}/{max(n, 10)}** — credentials will arrive by DM.'), view=None)

# ----- Manage -----
def panel_embed(v, osm):
    e = emb(f'VPS #{v["vps_id"]}', color=C_MAIN)
    e.description = (f'**RAM**   : {fmt_ram(v["ram_mb"])}\n**CPU**   : {v["cpu_cores"]} Core\n**SSD**   : {v["ssd_gb"]} GB\n'
                     f'**OS**    : {osm.get(v["os"], {}).get("label", v["os"])}\n**Status**: {STATUS[v["status"]]}\n**Expiry**: {v["expiry"]}')
    e.set_thumbnail(url=v.get('branding_logo_url') or LOGO); return e

class LogoModal(discord.ui.Modal, title='Change panel logo'):
    url = discord.ui.TextInput(label='Image URL (https://…)', max_length=255)
    def __init__(s, vid): super().__init__(); s.vid = vid
    async def on_submit(s, i):
        u = s.url.value.strip()
        if not u.startswith('https://'): return await i.response.send_message('URL must start with https://', ephemeral=True)
        await q("UPDATE vps SET branding_logo_url=%s WHERE id=%s", (u, s.vid)); await i.response.send_message('✅ Logo updated. Reopen /manage.', ephemeral=True)

class ManageView(discord.ui.View):
    def __init__(s, v, osm, flags, viewer, deadline=None):
        super().__init__(timeout=max(1, (deadline or time.time() + MANAGE_CD) - time.time()))
        s.v, s.osm, s.flags, s.viewer, s.msg = v, osm, flags, viewer, None
        s.deadline = deadline or time.time() + MANAGE_CD
        acc = v['access']; full = acc in ('owner', 'admin'); can_pw = full or acc == 'full'; can_pwr = can_pw or acc == 'manage'
        st = v['status']
        if st in ('running', 'stopped') and can_pwr:
            s.add_item(B('Stop' if st == 'running' else 'Start', discord.ButtonStyle.danger if st == 'running' else discord.ButtonStyle.primary, lambda i: s.power(i, 'stop' if st == 'running' else 'start'), 0))
            if st == 'running': s.add_item(B('Restart', discord.ButtonStyle.secondary, lambda i: s.power(i, 'restart'), 0))
        if can_pw and st in ('running', 'stopped'): s.add_item(B('Reinstall', discord.ButtonStyle.secondary, s.reinstall, 0))
        if st == 'running': s.add_item(B('Stats', discord.ButtonStyle.secondary, s.stats, 0))
        if full and st == 'running':
            for k, lbl in (('sshx', 'SSHX'), ('tmate', 'Tmate'), ('termius', 'Termius')):
                if flags.get(k): s.add_item(B(lbl, discord.ButtonStyle.secondary, lambda i, k=k: s.tool(i, k), 1))
        if full: s.add_item(B('🖼 Logo', discord.ButtonStyle.secondary, lambda i: i.response.send_modal(LogoModal(v['id'])), 1))
    async def interaction_check(s, i):
        if i.user.id != s.viewer: await i.response.send_message('Not your panel.', ephemeral=True); return False
        return True
    async def on_timeout(s):
        await cd_set(s.viewer, 'manage_reset_until', MANAGE_RESET)
        for c in s.children: c.disabled = True
        try:
            if s.msg: await s.msg.edit(view=s)
        except Exception: pass
    async def refresh(s, i):
        nv = await q("SELECT * FROM vps WHERE id=%s", (s.v['id'],), 'one'); nv['access'] = s.v['access']
        view = ManageView(nv, s.osm, s.flags, s.viewer, s.deadline); s.stop()
        view.msg = await i.edit_original_response(embed=panel_embed(nv, s.osm), view=view)
    async def power(s, i, act):
        await i.response.defer()
        v = await q("SELECT * FROM vps WHERE id=%s", (s.v['id'],), 'one')
        if v['status'] in ('suspended', 'deleted', 'creating'): return await i.followup.send('VPS not available.', ephemeral=True)
        rc, out = await bk_action(v, act)
        if rc != 0: return await i.followup.send(f'```{out[-300:]}```', ephemeral=True)
        await q("UPDATE vps SET status=%s WHERE id=%s", ('stopped' if act == 'stop' else 'running', v['id']))
        await audit(i.user.id, f'vps_{act}', v['vps_id']); await s.refresh(i)
    async def stats(s, i):
        await i.response.defer(ephemeral=True, thinking=True)
        d = await get_stats(s.v)
        e = emb(f'📊 VPS #{s.v["vps_id"]} stats')
        for k, n in (('cpu', 'CPU'), ('ram', 'RAM'), ('disk', 'Disk'), ('net', 'Network I/O'), ('up', 'Uptime')): e.add_field(name=n, value=d.get(k, 'n/a'))
        await i.followup.send(embed=e, ephemeral=True)
    async def reinstall(s, i):
        async def go(i2, key):
            await i2.response.defer(ephemeral=True)
            v = await q("SELECT * FROM vps WHERE id=%s", (s.v['id'],), 'one'); pw = rand_pw()
            await q("UPDATE vps SET status='creating',os=%s,ssh_password=%s WHERE id=%s", (key, pw, v['id'])); v.update(os=key, ssh_password=pw)
            await bk_action(v, 'delete'); ok, out = await bk_create(v, (await os_map())[key][v['type']], pw)
            await q("UPDATE vps SET status=%s WHERE id=%s", ('running' if ok else 'stopped', v['id']))
            await audit(i.user.id, 'vps_reinstall', v['vps_id'], {'os': key, 'ok': ok})
            await i2.followup.send(embed=creds_embed(v, pw) if ok else emb('Reinstall failed', f'```{out[-300:]}```', C_RED), ephemeral=True)
        view = OwnedView(i.user.id, 120); view.add_item(Sel('⚠ Pick OS — ALL DATA WILL BE ERASED', [discord.SelectOption(label=o['label'], value=k) for k, o in s.osm.items()][:25], go))
        await i.response.send_message(embed=emb('Reinstall', 'Choose a new OS. This wipes the VPS.', C_RED), view=view, ephemeral=True)
    async def tool(s, i, kind):
        await i.response.defer(ephemeral=True, thinking=True); v = s.v
        if kind == 'termius':
            return await i.followup.send(embed=emb('Termius / SSH', f'```\nHost: {v["ip"]}\nPort: {v["port"]}\nUser: root\nPass: {v["ssh_password"]}\n```\nAdd as a new host in Termius.'), ephemeral=True)
        if kind == 'tmate':
            sc = ("command -v tmate >/dev/null || (apt-get update -qq && apt-get install -y -qq tmate >/dev/null); rm -f /tmp/tm.sock; "
                  "tmate -S /tmp/tm.sock new-session -d && tmate -S /tmp/tm.sock wait tmate-ready && tmate -S /tmp/tm.sock display -p '#{tmate_ssh}'")
        else:
            sc = ("command -v sshx >/dev/null || curl -sSf https://sshx.io/get | sh >/dev/null 2>&1; pkill sshx; "
                  "setsid nohup sshx -q > /tmp/sshx.txt 2>&1 & sleep 8; cat /tmp/sshx.txt")
        rc, out = await bk_exec(v, ['bash', '-c', sc], 180)
        await i.followup.send(embed=emb(kind.upper(), f'```{out[-500:] or "no output"}```'), ephemeral=True)

async def flags_now():
    return {k: (await get_s(f'{k}_allow')) == 'true' for k in ('sshx', 'tmate', 'termius')}

async def open_panel(i, v):
    left = await cd_left(i.user.id, 'manage_reset_until')
    if left and await role_of(i.user.id) != 'owner': return await send(i, embed=emb('⏳ Panel cooldown', f'Try again in **{left}s**.', C_RED), ephemeral=True)
    if await role_of(i.user.id) != 'owner': await cd_set(i.user.id, 'manage_until', MANAGE_CD)
    osm = await os_map(); view = ManageView(v, osm, await flags_now(), i.user.id)
    await send(i, embed=panel_embed(v, osm), view=view, ephemeral=True)
    view.msg = await i.original_response()

# ───────────────────────── BOT / TREE ─────────────────────────
RL = {}
class Tree(app_commands.CommandTree):
    async def interaction_check(self, i):
        if i.type != discord.InteractionType.application_command: return True
        key = f'rl:{i.user.id}:{i.command.qualified_name if i.command else ""}'
        try:
            if rds:
                n = await rds.incr(key)
                if n == 1: await rds.expire(key, 10)
            else:
                ts = [t for t in RL.get(key, []) if t > time.time() - 10] + [time.time()]; RL[key] = ts; n = len(ts)
        except Exception: n = 0
        if n > 5: await i.response.send_message('🐢 Slow down.', ephemeral=True); return False
        return True
    async def on_error(self, i, error):
        log.exception('cmd error', exc_info=error)
        try: await send(i, embed=emb('Error', f'`{error}`', C_RED), ephemeral=True)
        except Exception: pass

intents = discord.Intents.default(); intents.members = True; intents.invites = True
client = discord.Client(intents=intents); tree = Tree(client)
invite_cache = {}

class Client2: pass

@client.event
async def on_ready(): log.info('Logged in as %s', client.user)

async def setup_hook():
    await init_db()
    await q("UPDATE deploy_queue SET status='waiting' WHERE status='processing'")
    for t in (deploy_worker, invite_checker, expiry_cron): t.start()
    if GUILD_ID:
        g = discord.Object(GUILD_ID); tree.copy_global_to(guild=g); await tree.sync(guild=g)
    else: await tree.sync()
client.setup_hook = setup_hook

# ───────────────────────── USER COMMANDS ─────────────────────────
@tree.command(name='deploy', description='Deploy a new VPS')
async def deploy(i: discord.Interaction):
    u, err = await check_deploy(i.user.id, str(i.user), 0, 0, 0)
    if err: return await i.response.send_message(embed=emb('Cannot deploy', err, C_RED), ephemeral=True)
    await i.response.send_message(embed=emb('🚀 Deploy a VPS', 'Choose OS + plan, then confirm.'), view=DeployView(i.user.id, await os_map(), await plans()), ephemeral=True)

@tree.command(name='create', description='Quick deploy with the default plan')
async def create(i: discord.Interaction):
    p = json.loads(await get_s('default_plan')); await i.response.defer(ephemeral=True)
    u, err = await check_deploy(i.user.id, str(i.user), p['ram'], p['cpu'], p['ssd'])
    if err: return await i.followup.send(embed=emb('Cannot deploy', err, C_RED))
    n = await enqueue(u, {'os': DEFAULT_ISO, 'ram': p['ram'], 'cpu': p['cpu'], 'ssd': p['ssd']})
    await i.followup.send(embed=emb('⏳ Queued', f'Waiting **{n}/{max(n, 10)}** — credentials will arrive by DM.'))

@tree.command(name='manage', description='Open your VPS panel')
@app_commands.describe(vps_id='VPS ID, e.g. A3F2')
async def manage(i: discord.Interaction, vps_id: Optional[str] = None):
    rows = await user_vps(i.user.id)
    if await role_of(i.user.id) in ('admin', 'owner') and vps_id:
        r = await q("SELECT *, 'admin' AS access FROM vps WHERE vps_id=%s AND status<>'deleted'", (vps_id.upper(),), 'one')
        if r: return await open_panel(i, r)
    if vps_id: rows = [r for r in rows if r['vps_id'] == vps_id.upper()]
    if not rows: return await i.response.send_message(embed=emb('No VPS found', 'Use /deploy to create one.'), ephemeral=True)
    if len(rows) == 1: return await open_panel(i, rows[0])
    async def pick(i2, v): await open_panel(i2, v)
    await i.response.send_message(embed=emb('Select a VPS'), view=PickView(i.user.id, rows, pick), ephemeral=True)

@tree.command(name='list', description='List your VPS')
async def list_(i: discord.Interaction):
    rows = await user_vps(i.user.id)
    e = emb('📋 Your VPS', '\n'.join(f'`#{r["vps_id"]}` {fmt_ram(r["ram_mb"])}/{r["cpu_cores"]}C/{r["ssd_gb"]}GB · {STATUS[r["status"]]} · exp {r["expiry"]}' + (' · *shared*' if r['access'] != 'owner' else '') for r in rows) or 'None yet.')
    await i.response.send_message(embed=e, ephemeral=True)

@tree.command(name='vps-shred', description='Share a VPS with a user')
async def vps_shred(i: discord.Interaction, vps_id: str, user: discord.User, permission: Literal['full', 'manage', 'view'] = 'manage'):
    me = await get_user(i.user.id, str(i.user)); v = await q("SELECT * FROM vps WHERE vps_id=%s AND owner_id=%s AND status<>'deleted'", (vps_id.upper(), me['id']), 'one')
    if not v: return await i.response.send_message('You do not own that VPS.', ephemeral=True)
    if user.bot or user.id == i.user.id: return await i.response.send_message('Invalid target.', ephemeral=True)
    t = await get_user(user.id, str(user))
    await q("INSERT INTO vps_shares(vps_id,shared_with,shared_by,permissions) VALUES(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE permissions=VALUES(permissions)", (v['id'], t['id'], me['id'], permission))
    await dm(user.id, embed=emb('🎁 VPS shared', f'VPS **{v["vps_id"]}** shared by {i.user.mention}. Use `/manage {v["vps_id"]}`.', C_BROWN))
    await audit(i.user.id, 'vps_share', v['vps_id'], {'to': user.id, 'perm': permission})
    await i.response.send_message(f'✅ Shared **{v["vps_id"]}** with {user.mention}.', ephemeral=True)

@tree.command(name='vps-shred-revoke', description='Revoke a VPS share')
async def vps_revoke(i: discord.Interaction, vps_id: str, user: discord.User):
    me = await get_user(i.user.id); t = await get_user(user.id)
    v = await q("SELECT * FROM vps WHERE vps_id=%s AND owner_id=%s", (vps_id.upper(), me['id']), 'one')
    if not v: return await i.response.send_message('You do not own that VPS.', ephemeral=True)
    n = await q("DELETE FROM vps_shares WHERE vps_id=%s AND shared_with=%s", (v['id'], t['id']))
    await audit(i.user.id, 'vps_share_revoke', v['vps_id'], {'from': user.id})
    await i.response.send_message('✅ Revoked.' if n else 'No such share.', ephemeral=True)

# ----- invites / boosts / claim -----
async def recount(uid):
    n = (await q("SELECT COUNT(*) n FROM invite_log WHERE user_id=%s AND valid=1 AND joined_at > NOW() - INTERVAL %s DAY", (uid, INV_DAYS), 'one'))['n']
    await q("UPDATE users SET invites=%s WHERE id=%s", (n, uid)); return n

async def queue_claim(u):
    p = json.loads(await get_s('default_plan'))
    uu, err = await check_deploy(u['discord_id'], None, p['ram'], p['cpu'], p['ssd'], bypass_cd=True)
    if err: return err
    await enqueue(u, {'os': DEFAULT_ISO, 'ram': p['ram'], 'cpu': p['cpu'], 'ssd': p['ssd'], 'claim': True})
    await set_s(f'claim:{u["discord_id"]}', now().isoformat()); await del_s(f'warn:{u["discord_id"]}'); return None

@tree.command(name='vps-claim', description='Claim a free VPS with invites')
async def vps_claim(i: discord.Interaction):
    u = await get_user(i.user.id, str(i.user)); n = await recount(u['id'])
    if await get_s(f'claim:{i.user.id}'): return await i.response.send_message('You already claimed a VPS.', ephemeral=True)
    if n < REQ_INV: return await i.response.send_message(embed=emb('Not enough invites', f'You have **{n}/{REQ_INV}** valid invites.', C_RED), ephemeral=True)
    err = await queue_claim(u)
    await i.response.send_message(embed=emb('🎉 Claim queued' if not err else 'Cannot claim', err or 'Credentials will arrive by DM.', C_BROWN if not err else C_RED), ephemeral=True)

@tree.command(name='check-in', description='Confirm your invites after a warning')
async def check_in(i: discord.Interaction):
    u = await get_user(i.user.id, str(i.user)); n = await recount(u['id'])
    if not await get_s(f'claim:{i.user.id}'): return await i.response.send_message('No claimed VPS to check in for.', ephemeral=True)
    if n >= REQ_INV:
        await del_s(f'warn:{i.user.id}'); await set_s(f'checkin:{i.user.id}', now().isoformat())
        return await i.response.send_message(embed=emb('✅ Checked in', f'Valid invites: **{n}/{REQ_INV}**.'), ephemeral=True)
    w = await get_s(f'warn:{i.user.id}'); left = ''
    if w: left = f'\nWindow ends in {max(0, int((datetime.fromisoformat(w) + timedelta(hours=CHECKIN_H) - now()).total_seconds() // 60))} min.'
    await i.response.send_message(embed=emb('Not enough invites', f'**{n}/{REQ_INV}** valid invites.{left}', C_RED), ephemeral=True)

@tree.command(name='invite-board', description='Invite leaderboard')
async def invite_board(i: discord.Interaction):
    rows = await q("SELECT discord_id,invites FROM users WHERE invites>0 ORDER BY invites DESC LIMIT 10", fetch='all')
    await i.response.send_message(embed=emb('🏆 Invite leaderboard', '\n'.join(f'**{n}.** <@{r["discord_id"]}> — {r["invites"]}' for n, r in enumerate(rows, 1)) or 'No invites yet.'))

@tree.command(name='invite-user', description='Invite stats for a user')
async def invite_user(i: discord.Interaction, user: discord.User):
    u = await get_user(user.id, str(user)); n = await recount(u['id'])
    await i.response.send_message(embed=emb(f'Invites · {user}', f'Valid invites: **{n}/{REQ_INV}**\nBoosts: **{u["boosts"]}**'), ephemeral=True)

@client.event
async def on_invite_create(inv): invite_cache.setdefault(inv.guild.id, {})[inv.code] = inv.uses
@client.event
async def on_invite_delete(inv): invite_cache.get(inv.guild.id, {}).pop(inv.code, None)
@client.event
async def on_guild_available(g):
    try: invite_cache[g.id] = {x.code: x.uses for x in await g.invites()}
    except discord.Forbidden: log.warning('No Manage Server permission for invite tracking')

@client.event
async def on_member_join(m):
    if m.bot: return
    try: invs = await m.guild.invites()
    except discord.Forbidden: return
    old = invite_cache.get(m.guild.id, {}); used = next((x for x in invs if x.uses > old.get(x.code, 0)), None)
    invite_cache[m.guild.id] = {x.code: x.uses for x in invs}
    if not used or not used.inviter or used.inviter.id == m.id: return
    inv = await get_user(used.inviter.id, str(used.inviter)); await get_user(m.id, str(m))
    await q("INSERT INTO invite_log(user_id,invited_id) VALUES(%s,%s)", (inv['id'], m.id)); n = await recount(inv['id'])
    if n >= REQ_INV and await get_s('auto_claim') == 'true' and not await get_s(f'claim:{used.inviter.id}'):
        if not await queue_claim(inv): await dm(used.inviter.id, embed=emb('🎉 Auto-claim', 'Your free VPS is being deployed.', C_BROWN))

@client.event
async def on_member_remove(m):
    r = await q("SELECT * FROM invite_log WHERE invited_id=%s AND valid=1 ORDER BY id DESC LIMIT 1", (m.id,), 'one')
    if not r: return
    await q("UPDATE invite_log SET valid=0,left_at=NOW() WHERE id=%s", (r['id'],)); n = await recount(r['user_id'])
    inv = await q("SELECT discord_id FROM users WHERE id=%s", (r['user_id'],), 'one')
    if inv and await get_s(f'claim:{inv["discord_id"]}') and n < REQ_INV and not await get_s(f'warn:{inv["discord_id"]}'):
        await set_s(f'warn:{inv["discord_id"]}', now().isoformat())
        await dm(inv['discord_id'], embed=emb('⚠ Invite dropped', f'An invited member left. You have **{n}/{REQ_INV}**. Re-invite and `/check-in` within **{CHECKIN_H}h** or your VPS is suspended.', C_RED))

@client.event
async def on_member_update(b, a):
    if b.premium_since is None and a.premium_since is not None:
        u = await get_user(a.id, str(a)); await q("INSERT INTO boost_log(user_id) VALUES(%s)", (u['id'],)); await q("UPDATE users SET boosts=boosts+1 WHERE id=%s", (u['id'],))
    elif b.premium_since is not None and a.premium_since is None:
        u = await get_user(a.id); await q("UPDATE boost_log SET active=0 WHERE user_id=%s AND active=1", (u['id'],)); await q("UPDATE users SET boosts=GREATEST(boosts-1,0) WHERE id=%s", (u['id'],))

@tasks.loop(minutes=10)
async def invite_checker():
    for r in await q("SELECT `key`,`value` FROM settings WHERE `key` LIKE 'claim:%'", fetch='all'):
        did = int(r['key'].split(':')[1]); u = await q("SELECT * FROM users WHERE discord_id=%s", (did,), 'one')
        if not u: continue
        n = await recount(u['id']); w = await get_s(f'warn:{did}')
        if n >= REQ_INV:
            if w: await del_s(f'warn:{did}')
            continue
        if not w:
            await set_s(f'warn:{did}', now().isoformat())
            await dm(did, embed=emb('⚠ Invites below requirement', f'You have **{n}/{REQ_INV}**. Fix it and `/check-in` within **{CHECKIN_H}h**.', C_RED)); continue
        if now() - datetime.fromisoformat(w) > timedelta(hours=CHECKIN_H):
            await q("UPDATE invite_log SET valid=0 WHERE user_id=%s", (u['id'],)); await q("UPDATE users SET invites=0 WHERE id=%s", (u['id'],))
            for v in await q("SELECT * FROM vps WHERE owner_id=%s AND status IN('running','stopped')", (u['id'],), 'all'): await suspend_vps(v)
            await del_s(f'warn:{did}'); await del_s(f'claim:{did}')
            await dm(did, embed=emb('⛔ VPS suspended', 'Invite requirement not met. Invites were reset.', C_RED))
            await audit(0, 'invite_suspend', did)

@tasks.loop(hours=1)
async def expiry_cron():
    for v in await q("SELECT * FROM vps WHERE expiry < CURDATE() AND status IN('running','stopped')", fetch='all'):
        await suspend_vps(v); o = await q("SELECT discord_id FROM users WHERE id=%s", (v['owner_id'],), 'one')
        await dm(o['discord_id'], embed=emb('⛔ VPS expired', f'VPS **{v["vps_id"]}** is suspended. Ask an admin to extend it.', C_RED)); await audit(0, 'expiry_suspend', v['vps_id'])
    for v in await q("SELECT * FROM vps WHERE expiry BETWEEN CURDATE() AND CURDATE() + INTERVAL 3 DAY AND status IN('running','stopped')", fetch='all'):
        k = f'remind:{v["vps_id"]}:{v["expiry"]}'
        if await get_s(k): continue
        await set_s(k, '1'); o = await q("SELECT discord_id FROM users WHERE id=%s", (v['owner_id'],), 'one')
        await dm(o['discord_id'], embed=emb('⏰ Expiry reminder', f'VPS **{v["vps_id"]}** expires on **{v["expiry"]}**.', C_BROWN))

@tree.command(name='about', description='About this bot')
async def about(i: discord.Interaction):
    e = emb(f'ℹ {BOT_NAME}', await get_s('about_text')); e.set_thumbnail(url=LOGO); await i.response.send_message(embed=e)

@tree.command(name='help', description='All commands')
async def help_(i: discord.Interaction):
    r = LV[await role_of(i.user.id)]
    t = ('**User**\n/deploy /create /manage /list /vps-shred /vps-shred-revoke /vps-claim /check-in /invite-board /invite-user /about')
    if r >= 1: t += '\n\n**Mod**\n/admin all'
    if r >= 2: t += ('\n\n**Admin**\n/admin delete|suspend|limit|ban|shutdown /vps-extend /deploy-os /deploy-plan /tool /lxc /about-edit /set-count /auto-claim')
    if r >= 3: t += '\n\n**Owner**\n/admin roles /admin wipe'
    await i.response.send_message(embed=emb('📖 Help', t), ephemeral=True)

# ───────────────────────── ADMIN COMMANDS ─────────────────────────
admin = app_commands.Group(name='admin', description='Admin tools'); tree.add_command(admin)

@admin.command(name='all', description='View all VPS')
async def admin_all(i: discord.Interaction):
    if not await need(i, 'mod'): return
    rows = await q("SELECT v.*, u.discord_id FROM vps v JOIN users u ON u.id=v.owner_id WHERE v.status<>'deleted' ORDER BY v.id DESC LIMIT 40", fetch='all')
    t = '\n'.join(f'`#{r["vps_id"]}` <@{r["discord_id"]}> {fmt_ram(r["ram_mb"])}/{r["cpu_cores"]}C/{r["ssd_gb"]}G {r["type"]} {STATUS[r["status"]]} {r["expiry"]}' for r in rows)
    await i.response.send_message(embed=emb(f'🗂 All VPS ({len(rows)})', t[:4000] or 'None.'), ephemeral=True)

@admin.command(name='delete', description='Delete a user VPS')
async def admin_delete(i: discord.Interaction, user: discord.User):
    if not await need(i, 'admin'): return
    async def ok(i2, v):
        async def done(i3):
            await i3.response.defer(ephemeral=True); await destroy_vps(v); await audit(i.user.id, 'admin_delete', v['vps_id'], {'owner': user.id})
            await i3.followup.send(f'🗑 Deleted **{v["vps_id"]}**.', ephemeral=True)
        await i2.response.send_modal(ConfirmModal(f'Delete {v["vps_id"]}', 'DELETE', done))
    await pick_vps_modal(i, user, ok)

async def pick_vps_modal(i, target, cb):
    rows = await user_vps(target.id, include_shared=False)
    if not rows: return await i.response.send_message('No VPS.', ephemeral=True)
    if len(rows) == 1: return await cb(i, rows[0])
    await i.response.send_message(embed=emb('Select a VPS'), view=PickView(i.user.id, rows, cb), ephemeral=True)

@admin.command(name='suspend', description='Suspend a user VPS')
async def admin_suspend(i: discord.Interaction, user: discord.User):
    if not await need(i, 'admin'): return
    async def go(i2, v):
        await i2.response.defer(ephemeral=True); await suspend_vps(v); await audit(i.user.id, 'admin_suspend', v['vps_id'])
        await dm(user.id, embed=emb('⛔ VPS suspended', f'VPS **{v["vps_id"]}** was suspended by an admin.', C_RED)); await i2.followup.send('✅ Suspended.', ephemeral=True)
    await pick_vps(i, user, go)

@admin.command(name='limit', description='Set per-user limits')
async def admin_limit(i: discord.Interaction, user: discord.User, ram_mb: Optional[int] = None, cpu: Optional[int] = None, ssd_gb: Optional[int] = None, max_vps: Optional[int] = None):
    if not await need(i, 'admin'): return
    await get_user(user.id, str(user)); sets = {k: v for k, v in dict(limit_ram_mb=ram_mb, limit_cpu_cores=cpu, limit_ssd_gb=ssd_gb, max_vps=max_vps).items() if v is not None and v >= 0}
    if not sets: return await i.response.send_message('Provide at least one value.', ephemeral=True)
    await q(f"UPDATE users SET {','.join(k + '=%s' for k in sets)} WHERE discord_id=%s", (*sets.values(), user.id)); await audit(i.user.id, 'admin_limit', user.id, sets)
    await i.response.send_message(f'✅ Limits updated: `{sets}`', ephemeral=True)

@admin.command(name='ban', description='Ban/unban a user from deploying')
async def admin_ban(i: discord.Interaction, user: discord.User, unban: bool = False):
    if not await need(i, 'admin'): return
    if user.id == OWNER_ID: return await i.response.send_message('Cannot ban the owner.', ephemeral=True)
    await i.response.defer(ephemeral=True); u = await get_user(user.id, str(user)); await q("UPDATE users SET banned=%s WHERE id=%s", (0 if unban else 1, u['id']))
    if not unban:
        for v in await q("SELECT * FROM vps WHERE owner_id=%s AND status IN('running','stopped')", (u['id'],), 'all'): await suspend_vps(v)
    await audit(i.user.id, 'admin_unban' if unban else 'admin_ban', user.id); await i.followup.send('✅ Done.', ephemeral=True)

@admin.command(name='wipe', description='OWNER: delete ALL VPS')
async def admin_wipe(i: discord.Interaction):
    if not await need(i, 'owner'): return
    async def go(i2):
        await i2.response.defer(ephemeral=True); rows = await q("SELECT * FROM vps WHERE status<>'deleted'", fetch='all')
        for v in rows: await destroy_vps(v)
        await audit(i.user.id, 'admin_wipe', 'all', {'count': len(rows)}); await i2.followup.send(f'🗑 Wiped {len(rows)} VPS.', ephemeral=True)
    await i.response.send_modal(ConfirmModal('Wipe ALL VPS', 'WIPE ALL', go))

@admin.command(name='shutdown', description='Freeze/unfreeze all deployments')
async def admin_shutdown(i: discord.Interaction, state: Literal['on', 'off']):
    if not await need(i, 'admin'): return
    await set_s('emergency_shutdown', 'true' if state == 'on' else 'false'); await audit(i.user.id, 'emergency_shutdown', state)
    await i.response.send_message(f'🛑 Emergency shutdown **{state}**.', ephemeral=True)

@admin.command(name='roles', description='OWNER: grant admin/mod/user role')
async def admin_roles(i: discord.Interaction, user: discord.User, role: Literal['admin', 'mod', 'user']):
    if not await need(i, 'owner'): return
    u = await get_user(user.id, str(user)); await q("UPDATE users SET role=%s WHERE id=%s", (role, u['id']))
    await q("INSERT INTO admin_roles(user_id,perms,granted_by) VALUES(%s,%s,%s)", (u['id'], json.dumps({'role': role}), i.user.id)); await audit(i.user.id, 'role_grant', user.id, {'role': role})
    await i.response.send_message(f'✅ {user.mention} is now **{role}**.', ephemeral=True)

@tree.command(name='deploy-os', description='ADMIN: manage OS images')
async def deploy_os(i: discord.Interaction, action: Literal['list', 'add', 'remove'], key: Optional[str] = None, label: Optional[str] = None, lxc_image: Optional[str] = None, docker_image: Optional[str] = None):
    if not await need(i, 'admin'): return
    m = await os_map()
    if action == 'add':
        if not (key and label and lxc_image and docker_image): return await i.response.send_message('Need key, label, lxc_image, docker_image.', ephemeral=True)
        m[key] = {'label': label, 'lxc': lxc_image, 'docker': docker_image}
    elif action == 'remove':
        if key not in m or len(m) == 1: return await i.response.send_message('Cannot remove.', ephemeral=True)
        m.pop(key)
    if action != 'list': await set_s('os_list', json.dumps(m)); await audit(i.user.id, f'os_{action}', key)
    await i.response.send_message(embed=emb('OS images', '\n'.join(f'`{k}` {o["label"]} — {o["lxc"]} | {o["docker"]}' for k, o in m.items())), ephemeral=True)

@tree.command(name='deploy-plan', description='ADMIN: set default plan')
async def deploy_plan(i: discord.Interaction, ram_mb: int, cores: int, ssd_gb: int, os: Optional[str] = None):
    if not await need(i, 'admin'): return
    if min(ram_mb, cores, ssd_gb) < 1: return await i.response.send_message('Values must be positive.', ephemeral=True)
    await set_s('default_plan', json.dumps({'name': 'Default', 'ram': ram_mb, 'cpu': cores, 'ssd': ssd_gb}))
    pl = await plans(); pl = [p for p in pl if p['name'] != 'Default'] + [{'name': 'Default', 'ram': ram_mb, 'cpu': cores, 'ssd': ssd_gb}]; await set_s('plans', json.dumps(pl))
    await audit(i.user.id, 'default_plan', f'{ram_mb}/{cores}/{ssd_gb}', {'os': os}); await i.response.send_message('✅ Default plan set.', ephemeral=True)

@tree.command(name='vps-extend', description='ADMIN: extend expiry (and revive if suspended)')
async def vps_extend(i: discord.Interaction, user: discord.User, days: app_commands.Range[int, 0, 365]):
    if not await need(i, 'admin'): return
    async def go(i2, v):
        await i2.response.defer(ephemeral=True); base = max(v['expiry'], now().date())
        await q("UPDATE vps SET expiry=%s WHERE id=%s", (base + timedelta(days=days), v['id']))
        u = await q("SELECT banned FROM users WHERE id=%s", (v['owner_id'],), 'one')
        if v['status'] == 'suspended' and not u['banned']: await revive_vps(v)
        await audit(i.user.id, 'vps_extend', v['vps_id'], {'days': days}); await i2.followup.send(f'✅ **{v["vps_id"]}** now expires {base + timedelta(days=days)}.', ephemeral=True)
    await pick_vps(i, user, go)

@tree.command(name='tool', description='ADMIN: allow/deny SSHX, Tmate or Termius buttons')
async def tool(i: discord.Interaction, name: Literal['sshx', 'tmate', 'termius'], allow: bool):
    if not await need(i, 'admin'): return
    await set_s(f'{name}_allow', 'true' if allow else 'false'); await audit(i.user.id, 'tool_toggle', name, {'allow': allow})
    await i.response.send_message(f'✅ {name} {"allowed" if allow else "denied"}.', ephemeral=True)

@tree.command(name='lxc', description='ADMIN: switch backend LXD (on) / Docker (off)')
async def lxc(i: discord.Interaction, mode: Literal['on', 'off']):
    if not await need(i, 'admin'): return
    await set_s('lxc_mode', mode); await audit(i.user.id, 'lxc_mode', mode); await i.response.send_message(f'✅ Backend: **{"LXD" if mode == "on" else "Docker"}**.', ephemeral=True)

@tree.command(name='about-edit', description='ADMIN: edit /about text')
async def about_edit(i: discord.Interaction, text: str):
    if not await need(i, 'admin'): return
    await set_s('about_text', text[:1500]); await audit(i.user.id, 'about_edit'); await i.response.send_message('✅ Updated.', ephemeral=True)

@tree.command(name='set-count', description='ADMIN: set a user invite/boost count')
async def set_count(i: discord.Interaction, user: discord.User, kind: Literal['invite', 'boost'], value: app_commands.Range[int, 0, 10000]):
    if not await need(i, 'admin'): return
    await get_user(user.id, str(user)); await q(f"UPDATE users SET {'invites' if kind == 'invite' else 'boosts'}=%s WHERE discord_id=%s", (value, user.id))
    await audit(i.user.id, 'set_count', user.id, {kind: value}); await i.response.send_message('✅ Updated (invites recalc from logs on next check).', ephemeral=True)

@tree.command(name='auto-claim', description='ADMIN: auto-claim VPS at required invites')
async def auto_claim(i: discord.Interaction, state: Literal['on', 'off']):
    if not await need(i, 'admin'): return
    await set_s('auto_claim', 'true' if state == 'on' else 'false'); await audit(i.user.id, 'auto_claim', state); await i.response.send_message(f'✅ Auto-claim **{state}**.', ephemeral=True)

if __name__ == '__main__':
    if not E('BOT_TOKEN'): raise SystemExit('BOT_TOKEN missing in .env')
    client.run(E('BOT_TOKEN'), log_handler=None)
