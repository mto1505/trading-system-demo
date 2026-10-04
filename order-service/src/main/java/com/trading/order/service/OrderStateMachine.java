package com.trading.order.service;

import java.util.Map;
import java.util.Set;

/**
 * MÁY TRẠNG THÁI LỆNH - hai vai trò:
 *
 *  1. Chặn chuyển trạng thái vô lý (lệnh đã FILLED không thể quay lại ACCEPTED).
 *  2. LÀ MỘT LỚP IDEMPOTENCY MIỄN PHÍ: execution report tới lần thứ hai sẽ thấy
 *     lệnh đã ở trạng thái cuối -> transition không hợp lệ -> bỏ qua.
 *     Không cần thêm bảng processed_event ở phía order-service.
 *
 * Trạng thái cuối (terminal) không có đường ra - đơn giản hoá có chủ đích:
 * hệ chưa hỗ trợ partial fill (khớp một phần). Muốn hỗ trợ thì cần thêm
 * PARTIALLY_FILLED với self-loop và cộng dồn filledQty.
 */
public final class OrderStateMachine {

    public static final String ACCEPTED = "ACCEPTED";
    public static final String FILLED = "FILLED";
    public static final String REJECTED = "REJECTED";
    public static final String REJECTED_BY_MATCHING = "REJ_MATCH";

    private static final Map<String, Set<String>> ALLOWED = Map.of(
            ACCEPTED, Set.of(FILLED, REJECTED_BY_MATCHING),
            FILLED, Set.of(),
            REJECTED, Set.of(),
            REJECTED_BY_MATCHING, Set.of()
    );

    private OrderStateMachine() {}

    public static boolean canTransition(String from, String to) {
        return ALLOWED.getOrDefault(from, Set.of()).contains(to);
    }
}
