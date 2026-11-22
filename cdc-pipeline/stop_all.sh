#!/usr/bin/env bash
# 停止三个服务
pkill -f "cdc_pipeline.target.service" 2>/dev/null || true
pkill -f "cdc_pipeline.receiver.service" 2>/dev/null || true
pkill -f "cdc_pipeline.pairing.service" 2>/dev/null || true
echo "已停止"
