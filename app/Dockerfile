FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

ENV DATA_DIR=/data \
    BATCH_ROWS=20000
VOLUME /data
EXPOSE 8000

# 单 worker + 多线程：保证“同时只执行一次合并”的进程内语义，
# 慢下载只占用各自线程，不阻塞后台合并。
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "16", \
     "--timeout", "0", "app:app"]
