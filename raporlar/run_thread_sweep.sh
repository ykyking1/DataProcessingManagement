#!/bin/bash
LOG="c:/Users/PC_4150_YD26/DataProcessingManagement/raporlar/thread_sweep_log.txt"
> "$LOG"
for v in 2 4 8 16; do
  docker restart clickhouse >/dev/null
  sleep 8
  MSYS_NO_PATHCONV=1 docker exec t2p-cmp3 python3 /host/scripts/thread_settings_sweep.py threads $v >> "$LOG" 2>&1
done
for v in 1 2 4 8; do
  docker restart clickhouse >/dev/null
  sleep 8
  MSYS_NO_PATHCONV=1 docker exec t2p-cmp3 python3 /host/scripts/thread_settings_sweep.py insert_threads $v >> "$LOG" 2>&1
done
echo "SWEEP_TAMAMLANDI" >> "$LOG"
