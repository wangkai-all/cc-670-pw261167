#!/usr/bin/env bash
# 一键启动三个独立服务（接收 8071 / 配对 8072 / 目标 8073）
set -euo pipefail
cd "$(dirname "$0")"
export CDC_DATA_DIR="${CDC_DATA_DIR:-$(pwd)/data}"
mkdir -p "$CDC_DATA_DIR"
PYTHON="${PYTHON:-python3}"

echo "数据目录: $CDC_DATA_DIR"
$PYTHON -m cdc_pipeline.target.service   --port "${TARGET_PORT:-8073}"   --data-dir "$CDC_DATA_DIR" &
T_PID=$!
$PYTHON -m cdc_pipeline.receiver.service --port "${RECEIVER_PORT:-8071}" --data-dir "$CDC_DATA_DIR" &
R_PID=$!
$PYTHON -m cdc_pipeline.pairing.service  --port "${PAIRING_PORT:-8072}"  --data-dir "$CDC_DATA_DIR" \
  --receiver-url "http://127.0.0.1:${RECEIVER_PORT:-8071}" \
  --target-url   "http://127.0.0.1:${TARGET_PORT:-8073}" &
P_PID=$!

echo "PID: receiver=$R_PID pairing=$P_PID target=$T_PID"
echo "页面: http://127.0.0.1:8071/  http://127.0.0.1:8072/  http://127.0.0.1:8073/"
trap 'kill $R_PID $P_PID $T_PID 2>/dev/null || true' INT TERM
wait
