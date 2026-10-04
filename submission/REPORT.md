# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Đức Danh / 2A202602722
**Repo:** https://github.com/mysorf-9239/K4-Track02-Day17-NguyenDucDanh-2A202602722-DataPipelineEngineering (tạo repo này ở chế độ public trước khi nộp)
**Commit bài nộp:** `6a1d0bb304c12177c0320f45a37939b480d1b925` (mã nguồn đã kiểm chứng; REPORT được commit kế tiếp).
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** OpenAI Codex hỗ trợ đọc đề/code, sửa ba lỗi trong pipeline, chạy kiểm chứng và soạn report; học viên cần rà soát và giải thích được các thay đổi.
**Nguồn tham khảo khác (nếu có):** Không.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có nhiều dòng/ticket; T-91 không có duy nhất trạng thái `high/closed/bug`; rerun làm checksum thay đổi. | Feature không reconcile full recompute; u05 ngày 12/08 thiếu event đến ngày 15/08; P99 lateness 3 ngày nhưng lookback 0. | T-97 vẫn có thể tồn tại trong downstream; CDC delete có `after=null` nên thiếu `ticket_id` khi parse. |
| **Nguyên nhân gốc** | Chỉ dedup trong batch; câu lệnh `INSERT` không upsert theo khóa và không so LSN giữa các batch. | Lookback 0 chỉ tính partition ngày chạy, bỏ qua event đến muộn theo event time. | Staging chỉ đọc khóa từ `value.after`; delete để `after=null`, trong khi khóa vẫn có trong Kafka `key`. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: `MERGE` theo `ticket_id`, chỉ update khi `source._lsn > dest._lsn`; tombstone delete cũng giữ LSN. | `pipeline/config.py`: `LOOKBACK_DAYS = 3`; Gold ghi đè các partition từ `day-3` đến `day`, nhóm theo `event_time`. | `pipeline/staging.py`: lấy `ticket_id` bằng `coalesce(key.ticket_id, after.ticket_id)`; giữ CDC delete và bỏ Kafka tombstone (`_op IS NULL`). |
| **Khái niệm trên slide** | CDC upsert, khóa thực thể, LSN/high-water ordering, idempotency. | Event time vs ingest time, lateness, lookback, overwrite partition. | Debezium CDC delete vs Kafka compaction tombstone; logical delete và propagation. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3` (ceil P99 theo ngày lịch).
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f` (C0=C1=C2=C3).
- `make parity`: **PARITY** cho `silver_tickets` và `gold_feature_daily`.

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE giữ một trạng thái hiện tại và điều kiện LSN chặn replay cũ; overwrite partition làm phép tổng hợp theo event date chạy lại an toàn khi có dữ liệu muộn.
- Tombstone thay vì xoá hẳn hàng trong Silver: lưu dấu delete cùng LSN để ngăn bản ghi cũ hồi sinh, đồng thời xóa PII khỏi hàng hiện tại và loại ticket khỏi Gold/RAG.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: giữ khả năng tái lập và lịch sử point-in-time; khi có yêu cầu xóa thật cần quy trình erasure/retention riêng để xóa hoặc tái tạo cả bản sao lịch sử theo luật và chính sách.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu cục bộ nhỏ, DuckDB đủ chạy SQL/Parquet; dbt thể hiện incremental merge, microbatch và test mà không cần cụm Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   Trong lab, snapshot cũ bất biến để bảo toàn khả năng tái lập; snapshot mới và RAG loại T-97. Production phải ưu tiên nghĩa vụ xóa: lập inventory toàn bộ bản sao (snapshot, lake, cache, index, backup), hạn chế truy cập ngay, xóa/crypto-shred hoặc tái tạo phiên bản chứa dữ liệu theo chính sách lưu trữ và pháp luật, rồi ghi audit. “Immutable” không được miễn trừ yêu cầu xóa.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   Đặt phát hiện/che PII bổ sung tại biên Bronze→Silver trước khi dữ liệu đi vào analytics, log, snapshot hay RAG; dùng NER/DLP có chính sách theo loại dữ liệu và quarantine khi không chắc. Đo precision/recall trên tập gán nhãn đại diện, tỷ lệ rò PII qua kiểm tra định kỳ/sampling và tỷ lệ false positive; giới hạn quyền truy cập Bronze vì đó là bản raw.

## 5. Output (dán nguyên văn)

```text
$ make verify
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

$ make test
..................................                                       [100%]
34 passed in 1.36s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12
run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
