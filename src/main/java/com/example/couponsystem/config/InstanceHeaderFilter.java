package com.example.couponsystem.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class InstanceHeaderFilter extends OncePerRequestFilter {

    private static final String HEADER_NAME = "X-App-Instance";
    private final String instanceName;

    public InstanceHeaderFilter(@Value("${HOSTNAME:local}") String instanceName) {
        this.instanceName = instanceName;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        response.setHeader(HEADER_NAME, instanceName);
        filterChain.doFilter(request, response);
    }
}
