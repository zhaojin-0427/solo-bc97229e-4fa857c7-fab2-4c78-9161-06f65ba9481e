"""遥测数据合并服务

上传 CSV（站点, UTC秒, 读数；全部按文本保存）-> 每次上传生成一个 Parquet；
后台把 >=2 个 Parquet 合并成一个；目录（SQLite 目录库）原子切换；
下载全库 CSV 使用受理时刻的文件快照，旧文件等下载结束后回收；
崩溃重启后未切换的合并保留旧数据并清理残件，已切换的接续回收。

所有读写路径都按批流式处理，内存占用不随总行数增长。
"""

import csv
import io
import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from flask import (Flask, Response, flash, jsonify, redirect,
                   render_template_string, request, url_for)
from werkzeug.utils import secure_filename

# ---------------------------------------------------------------- 配置

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
PARQUET_DIR = DATA_DIR / "parquet"      # 活动 + 已退役待回收的 parquet
TMP_DIR = DATA_DIR / "tmp"              # 落盘中的临时 parquet（上传/合并）
DB_PATH = DATA_DIR / "catalog.db"

BATCH_ROWS = int(os.environ.get("BATCH_ROWS", "20000"))
PROGRESS_INTERVAL = 0.5                 # 合并进度落库节流（秒）
# 演示/测试钩子：每处理一批人为延时（毫秒），便于观察进度、慢下载、崩溃恢复
MERGE_BATCH_DELAY_MS = int(os.environ.get("MERGE_BATCH_DELAY_MS", "0"))
DOWNLOAD_BATCH_DELAY_MS = int(os.environ.get("DOWNLOAD_BATCH_DELAY_MS", "0"))

SCHEMA = pa.schema([
    ("site", pa.string()),
    ("ts", pa.string()),
    ("reading", pa.string()),
])
CSV_HEADER = ["site", "ts", "reading"]

MERGE_ACTIVE_STATES = ("scanning", "writing", "committing")
MERGE_STATE_LABELS = {
    "scanning": "扫描输入文件",
    "writing": "写出合并文件",
    "committing": "切换目录",
    "switched": "目录已切换",
    "done": "完成",
    "failed": "失败",
}

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "telemetry-merge-demo")
app.config["MAX_CONTENT_LENGTH"] = 4 * 1024 * 1024 * 1024  # 4 GiB

merge_lock = threading.Lock()   # 同时只执行一次合并
reap_lock = threading.Lock()

# ---------------------------------------------------------------- 目录库


def db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def init_db():
    con = db()
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS files (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT NOT NULL,              -- 原始上传文件名
            path       TEXT NOT NULL UNIQUE,       -- parquet 目录下的文件名
            rows       INTEGER NOT NULL,
            state      TEXT NOT NULL DEFAULT 'active',  -- active | retired
            created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS merges (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            state         TEXT NOT NULL,
            input_ids     TEXT NOT NULL,           -- JSON 数组，受理时的输入快照
            output_path   TEXT,
            output_rows   INTEGER,
            progress_rows INTEGER NOT NULL DEFAULT 0,
            total_rows    INTEGER NOT NULL DEFAULT 0,
            error         TEXT,
            created_at    REAL NOT NULL,
            updated_at    REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS file_refs (
            file_id INTEGER PRIMARY KEY,
            refs    INTEGER NOT NULL DEFAULT 0     -- 进行中的下载引用数
        );
        """
    )
    con.commit()
    con.close()


# ---------------------------------------------------------------- 落盘工具


def fsync_file(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


# ---------------------------------------------------------------- 回收


def reap_retired():
    """删除 refs=0 的退役文件（旧文件保留到相关下载结束后再回收）。"""
    if not reap_lock.acquire(blocking=False):
        return
    try:
        con = db()
        try:
            rows = con.execute(
                """
                SELECT f.id, f.path FROM files f
                LEFT JOIN file_refs r ON r.file_id = f.id
                WHERE f.state = 'retired' AND COALESCE(r.refs, 0) = 0
                """
            ).fetchall()
            for r in rows:
                try:
                    (PARQUET_DIR / r["path"]).unlink(missing_ok=True)
                except OSError:
                    continue  # 下次再试
                con.execute("DELETE FROM files WHERE id=?", (r["id"],))
                con.execute("DELETE FROM file_refs WHERE file_id=?", (r["id"],))
                con.commit()
        finally:
            con.close()
    finally:
        reap_lock.release()


def reaper_loop():
    while True:
        time.sleep(5)
        try:
            reap_retired()
        except Exception:
            app.logger.exception("reaper failed")


# ---------------------------------------------------------------- 启动恢复


def startup_recover():
    """崩溃/中断后重启：

    - 未切换的合并：保留旧数据（输入仍是 active），清除残件，标记失败；
    - 已切换的合并：保留新数据，接续回收退役输入；
    - 下载不跨进程存活，引用计数清零；
    - 清理 tmp 残件与目录库不认识的孤儿 parquet。
    """
    init_db()
    now = time.time()
    con = db()
    try:
        con.execute("DELETE FROM file_refs")
        interrupted = con.execute(
            "SELECT id FROM merges WHERE state IN ('scanning','writing','committing')"
        ).fetchall()
        for m in interrupted:
            con.execute(
                "UPDATE merges SET state='failed', error=?, updated_at=? WHERE id=?",
                ("进程中断，合并未完成；已保留原始文件", now, m["id"]),
            )
        con.execute(
            "UPDATE merges SET state='done', updated_at=? WHERE state='switched'",
            (now,),
        )
        con.commit()
        known = {r["path"] for r in con.execute("SELECT path FROM files")}
    finally:
        con.close()

    for p in TMP_DIR.glob("*"):
        p.unlink(missing_ok=True)
    for p in PARQUET_DIR.glob("*.parquet"):
        if p.name not in known:
            p.unlink(missing_ok=True)
    reap_retired()


# ---------------------------------------------------------------- CSV -> Parquet


def _write_rows(writer, rows):
    cols = list(zip(*rows))
    batch = pa.RecordBatch.from_arrays(
        [pa.array(cols[0]), pa.array(cols[1]), pa.array(cols[2])], schema=SCHEMA
    )
    writer.write_batch(batch)


def csv_stream_to_parquet(stream, out_path):
    """流式把 CSV 写成 Parquet；重复行原样保留；首行按表头跳过。"""
    text = io.TextIOWrapper(stream, encoding="utf-8-sig", newline="")
    reader = csv.reader(text)
    writer = pq.ParquetWriter(out_path, SCHEMA, compression="zstd")
    total = 0
    batch = []
    try:
        for i, row in enumerate(reader):
            if i == 0:
                continue  # 表头
            if not row:
                continue  # 空行
            row = (list(row) + ["", "", ""])[:3]
            batch.append(row)
            if len(batch) >= BATCH_ROWS:
                _write_rows(writer, batch)
                total += len(batch)
                batch.clear()
        if batch:
            _write_rows(writer, batch)
            total += len(batch)
    finally:
        writer.close()
    fsync_file(out_path)
    return total


# ---------------------------------------------------------------- 合并 worker


def set_merge(merge_id, **fields):
    fields["updated_at"] = time.time()
    sql = "UPDATE merges SET " + ", ".join(f"{k}=?" for k in fields) + " WHERE id=?"
    con = db()
    try:
        con.execute(sql, (*fields.values(), merge_id))
        con.commit()
    finally:
        con.close()


def run_merge(merge_id, inputs, out_name):
    """扫描/写文件阶段不持有目录库写事务；切换是单个短事务。"""
    tmp_path = TMP_DIR / f"merge-{merge_id}-{uuid.uuid4().hex}.parquet"
    final_path = PARQUET_DIR / out_name
    try:
        # 扫描：只读 parquet 页脚统计总行数
        total = 0
        for inp in inputs:
            total += pq.ParquetFile(PARQUET_DIR / inp["path"]).metadata.num_rows
        set_merge(merge_id, state="writing", total_rows=total)

        # 写出：流式逐批，内存不随总行数增长
        writer = pq.ParquetWriter(tmp_path, SCHEMA, compression="zstd")
        written = 0
        last = 0.0
        try:
            for inp in inputs:
                pf = pq.ParquetFile(PARQUET_DIR / inp["path"])
                for batch in pf.iter_batches(batch_size=BATCH_ROWS):
                    if batch.schema != SCHEMA:
                        batch = batch.cast(SCHEMA)
                    writer.write_batch(batch)
                    written += batch.num_rows
                    if MERGE_BATCH_DELAY_MS:
                        time.sleep(MERGE_BATCH_DELAY_MS / 1000)
                    now = time.time()
                    if now - last >= PROGRESS_INTERVAL:
                        set_merge(merge_id, progress_rows=written)
                        last = now
        finally:
            writer.close()

        # 新文件完整落盘（fsync + 原子改名）后才切换目录
        fsync_file(tmp_path)
        os.replace(tmp_path, final_path)
        fsync_dir(PARQUET_DIR)

        # 目录切换：单个短事务，只替换所选文件；期间新上传不受影响
        set_merge(merge_id, state="committing", progress_rows=written)
        con = db()
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "INSERT INTO files(name,path,rows,state,created_at) VALUES (?,?,?,?,?)",
                (f"merged-{merge_id}.parquet", out_name, written, "active", time.time()),
            )
            n = con.execute(
                "UPDATE files SET state='retired' WHERE state='active' AND id IN (%s)"
                % ",".join("?" * len(inputs)),
                [i["id"] for i in inputs],
            ).rowcount
            if n != len(inputs):
                raise RuntimeError("输入文件状态已变化，放弃切换")
            con.execute(
                "UPDATE merges SET state='switched', progress_rows=?, output_rows=?,"
                " updated_at=? WHERE id=?",
                (written, written, time.time(), merge_id),
            )
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

        set_merge(merge_id, state="done")
        reap_retired()
    except Exception as e:
        app.logger.exception("merge %s failed", merge_id)
        tmp_path.unlink(missing_ok=True)
        try:  # 切换事务未提交 -> 输出文件是残件，清除；已提交则保留新数据
            con = db()
            try:
                known = con.execute(
                    "SELECT 1 FROM files WHERE path=?", (out_name,)
                ).fetchone()
            finally:
                con.close()
            if not known:
                final_path.unlink(missing_ok=True)
        except Exception:
            pass
        set_merge(merge_id, state="failed", error=str(e)[:500])
    finally:
        merge_lock.release()


# ---------------------------------------------------------------- 下载引用计数


def release_refs(ids):
    con = db()
    try:
        con.execute("BEGIN IMMEDIATE")
        for fid in ids:
            con.execute(
                "UPDATE file_refs SET refs=refs-1 WHERE file_id=? AND refs>0", (fid,)
            )
        con.execute("DELETE FROM file_refs WHERE refs<=0")
        con.commit()
    finally:
        con.close()
    reap_retired()


# ---------------------------------------------------------------- 路由


@app.get("/healthz")
def healthz():
    return "ok"


@app.post("/upload")
def upload():
    f = request.files.get("file")
    if f is None or not f.filename:
        flash("请选择要上传的 CSV 文件")
        return redirect(url_for("index"))
    name = secure_filename(f.filename) or "upload.csv"
    tmp_path = TMP_DIR / f"upload-{uuid.uuid4().hex}.parquet"
    try:
        rows = csv_stream_to_parquet(f.stream, tmp_path)
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        flash(f"上传失败：{e}")
        return redirect(url_for("index"))
    final_name = f"{uuid.uuid4().hex}.parquet"
    os.replace(tmp_path, PARQUET_DIR / final_name)
    fsync_dir(PARQUET_DIR)
    con = db()
    try:
        con.execute(
            "INSERT INTO files(name,path,rows,state,created_at) VALUES (?,?,?,?,?)",
            (name, final_name, rows, "active", time.time()),
        )
        con.commit()
    finally:
        con.close()
    flash(f"已上传 {name}（{rows} 行）")
    return redirect(url_for("index"))


@app.get("/download")
def download():
    # 受理时刻固定完整文件集合，并对每个文件加引用（短写事务）
    con = db()
    try:
        con.execute("BEGIN IMMEDIATE")
        rows = con.execute(
            "SELECT id, path FROM files WHERE state='active' ORDER BY id"
        ).fetchall()
        for r in rows:
            con.execute(
                "INSERT INTO file_refs(file_id,refs) VALUES (?,1) "
                "ON CONFLICT(file_id) DO UPDATE SET refs=refs+1",
                (r["id"],),
            )
        con.commit()
    finally:
        con.close()
    snapshot = [(r["id"], r["path"]) for r in rows]

    def generate():
        try:
            yield ",".join(CSV_HEADER) + "\r\n"
            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\r\n")
            for _, rel in snapshot:
                pf = pq.ParquetFile(PARQUET_DIR / rel)
                for batch in pf.iter_batches(batch_size=BATCH_ROWS):
                    cols = [c.to_pylist() for c in batch.columns]
                    for row in zip(*cols):
                        w.writerow(row)
                    if buf.tell():
                        yield buf.getvalue()
                        buf.seek(0)
                        buf.truncate(0)
                    if DOWNLOAD_BATCH_DELAY_MS:
                        time.sleep(DOWNLOAD_BATCH_DELAY_MS / 1000)
            if buf.tell():
                yield buf.getvalue()
        finally:
            # 正常结束或客户端断开（GeneratorExit）都会释放引用
            release_refs([fid for fid, _ in snapshot])

    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": 'attachment; filename="telemetry-all.csv"'},
    )


@app.post("/merge")
def merge():
    ids = sorted(set(request.form.getlist("file_ids", type=int)))
    if len(ids) < 2:
        flash("请至少选择两个文件进行合并")
        return redirect(url_for("index"))
    con = db()
    try:
        rows = con.execute(
            "SELECT id, path, rows FROM files WHERE state='active' AND id IN (%s)"
            % ",".join("?" * len(ids)),
            ids,
        ).fetchall()
        busy = con.execute(
            "SELECT COUNT(*) c FROM merges WHERE state IN ('scanning','writing','committing')"
        ).fetchone()["c"]
        if len(rows) != len(ids):
            flash("所选文件不存在或已被合并，请刷新后重试")
            return redirect(url_for("index"))
        if busy or not merge_lock.acquire(blocking=False):
            flash("已有合并任务正在进行，请稍候")
            return redirect(url_for("index"))
    finally:
        con.close()
    try:
        now = time.time()
        out_name = f"{uuid.uuid4().hex}.parquet"
        con = db()
        try:
            cur = con.execute(
                "INSERT INTO merges(state,input_ids,output_path,progress_rows,total_rows,"
                "created_at,updated_at) VALUES ('scanning',?,?,0,0,?,?)",
                (json.dumps([r["id"] for r in rows]), out_name, now, now),
            )
            merge_id = cur.lastrowid
            con.commit()
        finally:
            con.close()
        inputs = [dict(r) for r in rows]
        threading.Thread(
            target=run_merge, args=(merge_id, inputs, out_name), daemon=True
        ).start()
    except Exception:
        merge_lock.release()
        raise
    flash(f"合并任务 #{merge_id} 已启动（{len(ids)} 个文件）")
    return redirect(url_for("index"))


def _fmt_ts(t):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))


@app.get("/api/status")
def api_status():
    con = db()
    try:
        m = con.execute("SELECT * FROM merges ORDER BY id DESC LIMIT 1").fetchone()
        agg = con.execute(
            "SELECT COUNT(*) c, COALESCE(SUM(rows),0) s FROM files WHERE state='active'"
        ).fetchone()
    finally:
        con.close()
    payload = {"files": agg["c"], "rows": agg["s"], "merge": None}
    if m:
        payload["merge"] = {
            "id": m["id"],
            "state": m["state"],
            "state_label": MERGE_STATE_LABELS.get(m["state"], m["state"]),
            "active": m["state"] in MERGE_ACTIVE_STATES,
            "progress_rows": m["progress_rows"],
            "total_rows": m["total_rows"],
            "output_rows": m["output_rows"],
            "error": m["error"],
        }
    return jsonify(payload)


TEMPLATE = """<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>遥测数据合并服务</title>
<style>
 body{font-family:system-ui,-apple-system,sans-serif;max-width:980px;margin:2rem auto;padding:0 1rem;color:#222}
 h1{font-size:1.4rem}
 table{border-collapse:collapse;width:100%;margin:.8rem 0}
 th,td{border:1px solid #ddd;padding:.4rem .6rem;text-align:left;font-size:.9rem}
 th{background:#f5f5f5}
 .card{border:1px solid #ddd;border-radius:8px;padding:1rem;margin:1rem 0}
 .bar{background:#eee;border-radius:4px;height:14px;overflow:hidden;margin-top:.5rem}
 .bar>div{background:#3b82f6;height:100%;width:0;transition:width .3s}
 .err{color:#b91c1c}.muted{color:#777;font-size:.85rem}
 .flash{background:#fef9c3;border:1px solid #fde047;padding:.5rem .8rem;border-radius:6px;margin:.5rem 0}
 button{padding:.4rem .9rem;cursor:pointer}
 button:disabled{opacity:.5;cursor:not-allowed}
</style>
</head>
<body>
<h1>遥测数据合并服务</h1>
{% with msgs = get_flashed_messages() %}
  {% for m in msgs %}<div class="flash">{{ m }}</div>{% endfor %}
{% endwith %}

<div class="card">
  <form method="post" action="{{ url_for('upload') }}" enctype="multipart/form-data">
    <b>上传 CSV</b>
    <span class="muted">列：站点, UTC秒, 读数（首行为表头，全部按文本保存，重复行保留）</span><br>
    <input type="file" name="file" accept=".csv,text/csv" required>
    <button type="submit">上传并生成 Parquet</button>
  </form>
  <hr>
  <form method="get" action="{{ url_for('download') }}">
    <b>导出</b>
    <button type="submit">下载全库 CSV</button>
    <span class="muted">受理时刻快照；当前 {{ files|length }} 个活动文件，共 {{ total_rows }} 行</span>
  </form>
</div>

<div class="card">
  <b>合并状态</b>
  <div id="mergeInfo" class="muted">暂无合并任务</div>
  <div class="bar"><div id="mergeBar"></div></div>
  <div id="mergeErr" class="err"></div>
</div>

<form method="post" action="{{ url_for('merge') }}">
<div class="card">
  <b>活动文件</b>
  <table>
    <tr><th></th><th>ID</th><th>文件名</th><th>行数</th><th>上传时间</th></tr>
    {% for f in files %}
    <tr>
      <td><input type="checkbox" name="file_ids" value="{{ f.id }}"></td>
      <td>{{ f.id }}</td><td>{{ f.name }}</td><td>{{ f.rows }}</td><td>{{ f.created }}</td>
    </tr>
    {% endfor %}
    {% if not files %}<tr><td colspan="5" class="muted">暂无文件</td></tr>{% endif %}
  </table>
  <button type="submit" id="mergeBtn">合并所选（至少 2 个）</button>
  <span class="muted">后台执行；同时只运行一个合并；切换只替换所选文件，不影响期间新上传</span>
</div>
</form>

<div class="card">
  <b>最近合并</b>
  <table>
    <tr><th>ID</th><th>状态</th><th>进度(行)</th><th>错误</th><th>更新时间</th></tr>
    {% for m in merges %}
    <tr>
      <td>{{ m.id }}</td><td>{{ m.state_label }}</td>
      <td>{{ m.progress_rows }}/{{ m.total_rows }}</td>
      <td class="err">{{ m.error or "" }}</td><td>{{ m.updated }}</td>
    </tr>
    {% endfor %}
    {% if not merges %}<tr><td colspan="5" class="muted">暂无</td></tr>{% endif %}
  </table>
</div>

<script>
let lastActive = false;
async function poll(){
  try{
    const r = await fetch('/api/status');
    const j = await r.json();
    const info = document.getElementById('mergeInfo');
    const bar = document.getElementById('mergeBar');
    const err = document.getElementById('mergeErr');
    const btn = document.getElementById('mergeBtn');
    if(j.merge){
      const m = j.merge;
      info.textContent = '任务 #' + m.id + '：' + m.state_label +
        '，已写入 ' + m.progress_rows + '/' + m.total_rows + ' 行';
      bar.style.width = m.total_rows > 0 ? (100*m.progress_rows/m.total_rows)+'%'
                                         : (m.state==='done' ? '100%' : '0%');
      err.textContent = m.error || '';
      btn.disabled = m.active;
      if(lastActive && !m.active){ location.reload(); }
      lastActive = m.active;
    }else{
      info.textContent = '暂无合并任务';
      btn.disabled = false;
    }
  }catch(e){}
  setTimeout(poll, 1200);
}
poll();
</script>
</body>
</html>
"""


@app.get("/")
def index():
    con = db()
    try:
        files = con.execute(
            "SELECT * FROM files WHERE state='active' ORDER BY id"
        ).fetchall()
        merges = con.execute("SELECT * FROM merges ORDER BY id DESC LIMIT 5").fetchall()
    finally:
        con.close()
    files = [dict(f, created=_fmt_ts(f["created_at"])) for f in files]
    merges = [
        dict(
            m,
            state_label=MERGE_STATE_LABELS.get(m["state"], m["state"]),
            updated=_fmt_ts(m["updated_at"]),
        )
        for m in merges
    ]
    return render_template_string(
        TEMPLATE, files=files, merges=merges, total_rows=sum(f["rows"] for f in files)
    )


# ---------------------------------------------------------------- 启动

for _d in (DATA_DIR, PARQUET_DIR, TMP_DIR):
    _d.mkdir(parents=True, exist_ok=True)
startup_recover()
threading.Thread(target=reaper_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), threaded=True)
