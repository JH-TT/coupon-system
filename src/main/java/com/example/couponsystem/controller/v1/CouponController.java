package com.example.couponsystem.controller.v1;

import com.example.couponsystem.dto.CouponRequest;
import com.example.couponsystem.facade.CouponFacade;
import com.example.couponsystem.service.CouponService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/coupons")
public class CouponController {

    private final CouponService couponService;
    private final CouponFacade couponFacade;

    @PostMapping(path = "/issue")
    public ResponseEntity<Object> issueCoupon(@RequestBody CouponRequest request) throws Exception {

        couponFacade.issueCouponWithRedis(request);

        return ResponseEntity.ok().build();
    }
}

