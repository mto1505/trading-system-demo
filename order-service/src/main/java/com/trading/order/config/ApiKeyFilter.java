package com.trading.order.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Bảo vệ tối thiểu API đặt lệnh bằng API key (header X-API-KEY).
 *
 * ĐÂY LÀ SÀN, KHÔNG PHẢI TRẦN. Production tài chính cần:
 * - OAuth2/JWT (Spring Security + Authorization Server) gắn quyền theo account
 * - mTLS giữa các service (service mesh: Istio/Linkerd, hoặc Kafka SASL_SSL)
 * - Rate limiting per-account (chặn máy đặt lệnh loạn)
 * Ưu điểm của filter đơn giản: 0 phụ thuộc, ~0 latency, đủ chặn truy cập vu vơ.
 * Nhược: key tĩnh, lộ là hết; không phân quyền theo user.
 */
@Component
public class ApiKeyFilter extends OncePerRequestFilter {

    @Value("${security.api-key:demo-key}")
    private String apiKey;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String path = request.getRequestURI();
        // Chỉ bảo vệ API nghiệp vụ; actuator để K8s probe + Prometheus scrape
        if (path.startsWith("/api/") && !apiKey.equals(request.getHeader("X-API-KEY"))) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.getWriter().write("{\"error\":\"invalid or missing X-API-KEY\"}");
            return;
        }
        chain.doFilter(request, response);
    }
}
