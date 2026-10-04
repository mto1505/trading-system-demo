package com.trading.outbox;

import java.util.List;

/**
 * ADAPTER do mỗi service tự cài đặt: khoá một lô event chưa publish.
 * Cài đặt chuẩn dùng SELECT ... FOR UPDATE SKIP LOCKED để nhiều pod relay
 * chạy song song mà không giẫm chân nhau.
 */
public interface OutboxStore {
    List<? extends OutboxRecord> lockUnpublishedBatch(int limit);
}
