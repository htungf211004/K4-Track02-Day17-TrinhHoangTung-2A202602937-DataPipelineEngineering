# K4-Track02-Day17 — Report cá nhân

- **Họ tên / MSSV:** Trịnh Hoàng Tùng / 2A202602937
- **Repo:** Chưa cấu hình repo bài nộp; `origin` hiện vẫn là repo đề bài
- **Commit bài nộp:** Chưa commit — cập nhật SHA sau khi chốt bài
- **AI đã dùng và phạm vi hỗ trợ:** OpenAI Codex hỗ trợ đọc code, sửa ba lỗi
- **Nguồn tham khảo khác:** 

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Contract một hàng/ticket và trạng thái cuối T-91 fail; replay đổi checksum. | Feature lệch full recompute; u05 ngày 12/08 thiếu sự kiện tới ngày 15. | T-97 đã xoá vẫn còn trong dữ liệu hiện hành/Gold. |
| **Nguyên nhân gốc** | Chỉ `INSERT`, nên trùng khoá và batch cũ có thể quay lại. | `LOOKBACK_DAYS=0` không bao phủ P99 trễ 3 ngày. | Chỉ đọc khoá từ `after`; delete có `after=null` nên bị lọc mất. |
| **Cách sửa** | `silver.py`: dedup nội batch rồi `MERGE` theo `ticket_id`, chỉ update khi LSN mới lớn hơn. | `config.py`: đặt lookback `ceil(P99)=3`; Gold vẫn gộp/overwrite theo event date. | `staging.py`: `coalesce(after.ticket_id,key.ticket_id)`; delete thành tombstone Silver, Kafka tombstone `value=null` không phải thay đổi. |
| **Khái niệm** | Silver có khoá; MERGE; thứ tự CDC; idempotency. | Event time/ingest time; late data; measured lookback. | Phong bì Debezium; compaction tombstone; “xoá phải lan”. |

## 2. Các con số

- P99 lateness từ 43 bản ghi Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3` bằng `ceil`.
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`.
- dbt build: `PASS=19`; parity: **PARITY** cho `silver_tickets` và `gold_feature_daily`.

## 3. Lựa chọn công cụ / kỹ thuật

- MERGE + LSN phù hợp bảng trạng thái theo khoá; overwrite-partition phù hợp aggregate event-date trong cửa sổ nhỏ có thể tính lại trọn vẹn.
- Tombstone giữ LSN chống “hồi sinh”, xoá PII và cho downstream lọc `is_deleted`; hard delete làm mất dấu thứ tự CDC.
- Snapshot “as of” từ Bronze tái lập được tri thức lúc tạo; dữ liệu muộn sinh version mới thay vì sửa snapshot cũ.
- DuckDB nhẹ, zero-key cho seed nhỏ; dbt kiểm chứng cùng logic bằng model/test, chưa cần vận hành phân tán của Spark.
- dbt dùng `unique_key='ticket_id'`; `merge_update_condition` nhận LSN lớn hơn; `batch_size='day'`, `lookback=3` xử lý late data theo ngày.

## 4. Hai câu hỏi suy ngẫm

1. Quyền xoá ưu tiên hơn bất biến. Production cần deletion registry + lineage, chặn dùng artifact cũ, tạo version sạch, rồi purge/crypto-shred snapshot cũ theo SLA và lưu audit không PII. Lab còn T-97 trong snapshot cũ nên chưa phải xoá PII production đầy đủ.
2. Đặt PII gate Bronze→Silver và kiểm tra lại trước Gold/index: NER/DLP cho tên, regex cho email/điện thoại, pseudonymize và quarantine khi độ tin cậy thấp. Đo precision/recall/F1 trên tập tiếng Việt gán nhãn, false-negative theo loại PII và leak rate qua scan Silver/Gold.

## 5. Output thực tế

```text
> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

> .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 2.58s

> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

> $env:DO_NOT_TRACK = '1'
> Push-Location dbt_project
> ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Running with dbt=1.12.5
Registered adapter: duckdb=1.11.0
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
Concurrency: 1 threads (target='dev')
1 of 19 OK created sql view model main.stg_events
2 of 19 OK created sql view model main.stg_ticket_changes
3 of 19 OK created sql incremental model main.silver_events
4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone
8 of 19 OK created sql incremental model main.silver_tickets
5 of 19 PASS not_null_silver_events_event_id
6 of 19 PASS not_null_silver_events_user_id
7 of 19 PASS unique_silver_events_event_id
9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other
10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high
11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed
12 of 19 PASS not_null_silver_tickets__lsn
13 of 19 PASS not_null_silver_tickets_is_deleted
14 of 19 PASS not_null_silver_tickets_ticket_id
15 of 19 PASS unique_silver_tickets_ticket_id
16 of 19 OK created sql microbatch model main.gold_feature_daily (7 daily batches)
17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date
18 of 19 PASS not_null_gold_feature_daily_event_date
19 of 19 PASS not_null_gold_feature_daily_user_id
Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 1.01 seconds.
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

> Pop-Location
> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
