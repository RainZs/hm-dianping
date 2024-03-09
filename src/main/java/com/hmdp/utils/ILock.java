package com.hmdp.utils;

public interface ILock {

    // 尝试获取锁  true表示获取成功，false表示获取失败
    boolean tryLock(Long timeoutSec);

    // 释放锁
    void unlock();
}
