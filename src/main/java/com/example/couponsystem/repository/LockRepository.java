package com.example.couponsystem.repository;

import com.example.couponsystem.entity.Coupon;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface LockRepository extends JpaRepository<Coupon, Long> {

    @Query(value = "SELECT GET_LOCK(:key, 10)", nativeQuery = true)
    Integer getLock(@Param("key") String key);

    @Query(value = "SELECT RELEASE_LOCK(:key)", nativeQuery = true)
    Integer releaseLock(@Param("key") String key);
}
