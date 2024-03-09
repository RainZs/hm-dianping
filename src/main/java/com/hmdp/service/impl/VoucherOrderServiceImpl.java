package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    };
    private class VoucherOrderHandler implements Runnable{
        String queName = "stream.orders";
        @Override
        public void run() {
            while(true){
                try {
                    // 获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queName, ReadOffset.lastConsumed())
                    );
                    // 判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queName, "g1", record.getId());
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                try {
                    // 获取Pending-List中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queName, ReadOffset.from("0"))
                    );
                    // 判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 如果获取失败，说明Pending-List没有异常消息，结束循环
                        break;
                    }
                    // 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queName, "g1", record.getId());
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理Pending-List异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }
            }
        }
    }
//    private class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//            while(true){
//                // 获取队列中的订单信息
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // 创建锁对象 （分布式锁）
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        boolean isLock = lock.tryLock(); // redisson提供的
        // 判断是否获取锁成功
        if(!isLock){
            // 如果不成功，那就失败
            log.error("不允许重复下单！");
            return;
        }
        try {
             proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 获取订单id
        Long orderId = redisIdWorker.nextId("order");
        // 1. 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        // 2. 判断结果是否为0
        int r = result.intValue();
        if(r != 0){
            // 2.1 不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足！" : "用户不可重复下单！");
        }

        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 2.3 返回订单id
        return Result.ok(orderId);
    }

//    @Override
//    public Result  seckillVoucher(Long voucherId) {
//        // 获取用户
//        Long userId = UserHolder.getUser().getId();
//        // 1. 执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        // 2. 判断结果是否为0
//        int r = result.intValue();
//        if(r != 0){
//            // 2.1 不为0，代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足！" : "用户不可重复下单！");
//        }
//
//        // 2.2 为0，有购买资格，把下单信息保存到阻塞队列
//        // 创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 填入订单id
//        Long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 填入用户id
//        voucherOrder.setUserId(userId);
//        // 填入代金券id
//        voucherOrder.setVoucherId(voucherId);
//        // 放入阻塞队列
//        orderTasks.add(voucherOrder);
//        // 获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        // 2.3 返回订单id
//        return Result.ok(orderId);
//    }
//    @Override
//    public Result  seckillVoucher(Long voucherId) {
//        // 查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 判断秒杀是否开始
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始！");
//        }
//        // 判断秒杀是否结束
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束！");
//        }
//        // 判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//        Long userId = UserHolder.getUser().getId();
//        // 创建锁对象 （分布式锁）
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//        //boolean isLock = lock.tryLock((long) 1200); // 参数是超时时间（分布式锁的）
//        boolean isLock = lock.tryLock(); // redisson提供的
//        // 判断是否获取锁成功
//        if(!isLock){
//            // 如果不成功，那就失败
//            return Result.fail("不允许重复下单！");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//    }

    // 我们不在createVoucherOrder这个方法上加锁的原因是，这个事务是由spring管理的，所以，我们进行了
    // 修改之后，是先释放锁，然后才会提交事务（spring管理，这个方法完全执行完之后才会提交，也就是执行直到最后一个 } 才会提交）
    // 而锁的释放就是在return后面的那个大括号那个就释放了，这就可能会出现线程安全问题，所以我们要锁住整个方法
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 进行一人一单判断
        Long userId = voucherOrder.getUserId();
        // 查询订单
        int count = query()
                .eq("user_id", userId)  // 同一个用户
                .eq("voucher_id", voucherOrder.getVoucherId()).count();  // 对于同一个优惠券只能下一单
        // 判断是否存在
        if (count > 0) {
            // 用户已经购买过，不允许下单‘
            log.error("用户已经购买过一次！");
            return;
        }
        // 扣减库存
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) { // 如果扣减失败说明库存不足
            log.error("库存不足");
            return;
        }

        save(voucherOrder);
    }
}
