# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Đỗ Thái Sơn / 2A202603021
**Repo:** https://github.com/tsun165/K4-Track02-Day17-DoThaiSon-2A202603021-DataPipelineEngineering
**Commit bài nộp:** `c2e7b90` (commit code cuối; commit sau chỉ thêm REPORT + checksums)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5): đọc code, chạy verify/test, đề xuất và viết 3 bản sửa trong `pipeline/`, soạn nháp REPORT. Tôi đã review từng dòng sửa và tự chạy lại toàn bộ kiểm tra.
**Nguồn tham khảo khác (nếu có):** Slide Ngày 17; tài liệu Debezium (định dạng change event, tombstone); bản dbt trong `dbt_project/` dùng để đối chiếu.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: **24 hàng cho 12 ticket**; T-91 ra 3 hàng `low/open`, `high/open`, `high/closed/bug` | verify: feature daily lệch full recompute (`c50b8851 != 8630e04a`); u05 ngày 08-12 = `(2, 0)` thay vì `(5, 1)` | verify: T-97 vẫn `is_deleted = false`, còn tên; còn trong training snapshot mới nhất và 2 chunk RAG |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT` (append), không có khoá | `LOOKBACK_DAYS = 0` giả định event tới ngay; event 08-12 đến ngày 08-15 không được tính lại | Staging lấy `ticket_id` từ `after`; với `op = 'd'` thì `after = null` → bị lọc mất |
| **Cách sửa** | `silver.py`: `MERGE ... ON ticket_id`, `UPDATE` chỉ khi `s._lsn > t._lsn` | `config.py`: `LOOKBACK_DAYS = 3` = ceil(P99) | `staging.py`: `coalesce(after.ticket_id, before.ticket_id)` |
| **Khái niệm** | Silver có khoá; MERGE + LSN guard | Data về muộn; lookback đo từ Bronze | CDC log-based; xoá phải lan |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY**

## 3. Lựa chọn công cụ / kỹ thuật

- MERGE vs overwrite-partition: ticket là thực thể có trạng thái, thay đổi có thứ tự LSN → MERGE + LSN guard idempotent cả khi chạy lại batch cũ. Feature daily là tổng hợp theo ngày event → tính lại cả cửa sổ `[day − 3, day]` đơn giản và đúng với event muộn.
- Tombstone thay vì xoá hẳn: hàng giữ `ticket_id` + `_lsn` nên chạy lại batch cũ không làm T-97 "sống lại" (xoá hẳn thì `WHEN NOT MATCHED` chèn lại PII). Đánh đổi: hàng tồn tại mãi, nhưng chỉ còn khoá.
- Snapshot dựng lại từ Bronze "as of" ngày đó: model train trên `v<day>` phải tái lập được; feedback muộn tạo version mới, không sửa lén version cũ.
- DuckDB/dbt thay vì Spark: vài chục dòng/ngày vừa một máy; DuckDB có MERGE, window, đọc Parquet, không cần cluster; dbt thêm contract, test, microbatch.

## 4. Hai câu hỏi suy ngẫm

1. Quyền xoá là nghĩa vụ pháp lý nên thắng; "bất biến" nghĩa là không sửa lén. Job erasure ghi lại các version chứa T-97 thành bản đã loại T-97, kèm audit log (version, checksum cũ → mới), đánh dấu model liên quan cần retrain, và purge cả Bronze, không thì rebuild sẽ khôi phục lại. Bền hơn: crypto-shredding (mã hoá văn bản theo khoá từng user, xoá khoá).
2. Chốt ở ranh giới Bronze → Silver: NER tiếng Việt (Presidio + PhoBERT/underthesea) che `PERSON`, địa chỉ, số định danh bên cạnh regex; thêm contract chạy NER trên Silver/Gold, còn PII thì fail run. Đo recall/precision trên tập ticket gán nhãn tay, theo dõi tỷ lệ phát hiện mỗi ngày.

## 5. Output (dán nguyên văn)

Chạy bằng PowerShell/Git Bash với Python 3.12 (env `VinAI`), `PYTHONIOENCODING=utf-8`, lệnh tương đương theo [SUBMISSION.md](../docs/SUBMISSION.md).

```text
$ python -m scripts.verify
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

$ python -m pytest
C:\Users\sondo\miniforge3\envs\VinAI\Lib\site-packages\pytest_asyncio\plugin.py:247: PytestDeprecationWarning: The configuration option "asyncio_default_fixture_loop_scope" is unset.
The event loop scope for asynchronous fixtures will default to the fixture caching scope. Future versions of pytest-asyncio will default the loop scope for asynchronous fixtures to function scope. Set the default fixture loop scope explicitly in order to avoid unexpected behavior in the future. Valid fixture loop scopes are: "function", "class", "module", "package", "session"

  warnings.warn(PytestDeprecationWarning(_DEFAULT_FIXTURE_LOOP_SCOPE_UNSET))
..................................                                       [100%]
34 passed in 3.01s

$ python -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ python main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ python main.py --land-only
$ cd dbt_project; dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
02:31:12  Running with dbt=1.12.5
02:31:12  Registered adapter: duckdb=1.11.0
02:31:13  Unable to do partial parsing because saved manifest not found. Starting full parse.
02:31:16  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
02:31:16  
02:31:16  Concurrency: 1 threads (target='dev')
02:31:16  
02:31:18  1 of 19 START sql view model main.stg_events ................................... [RUN]
02:31:18  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.08s]
02:31:18  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
02:31:18  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
02:31:18  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
02:31:18  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.10s]
02:31:18  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
02:31:18  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.14s]
02:31:18  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
02:31:18  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.09s]
02:31:18  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
02:31:18  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
02:31:18  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
02:31:18  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
02:31:18  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
02:31:19  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
02:31:19  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
02:31:19  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
02:31:19  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
02:31:19  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
02:31:19  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
02:31:19  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
02:31:19  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
02:31:19  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
02:31:19  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
02:31:19  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
02:31:19  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
02:31:19  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
02:31:19  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
02:31:19  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
02:31:19  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
02:31:19  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
02:31:19  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.02s]
02:31:19  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
02:31:19  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.05s]
02:31:19  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
02:31:19  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
02:31:19  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
02:31:19  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.03s]
02:31:19  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
02:31:19  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.03s]
02:31:19  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
02:31:19  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.03s]
02:31:19  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
02:31:19  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.03s]
02:31:19  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.23s]
02:31:19  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
02:31:19  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
02:31:19  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
02:31:19  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
02:31:19  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
02:31:19  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
02:31:19  
02:31:19  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 3.29 seconds (3.29s).
02:31:19  
02:31:19  Completed successfully
02:31:19  
02:31:19  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ python -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
