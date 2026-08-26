#!/bin/bash
LOG="c:/Users/PC_4150_YD26/DataProcessingManagement/raporlar/combined_sweep_log.txt"
> "$LOG"
for combo in "2 8" "3 6" "6 3" "4 5" "6 1"; do
  MSYS_NO_PATHCONV=1 docker exec t2p-cmp3 python3 /host/scripts/combined_process_thread_sweep.py $combo >> "$LOG" 2>&1
  echo "---" >> "$LOG"
done
echo "COMBINED_SWEEP_TAMAMLANDI" >> "$LOG"
