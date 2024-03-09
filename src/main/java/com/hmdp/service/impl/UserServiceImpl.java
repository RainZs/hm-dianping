package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import jodd.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    // 发送验证码功能
    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1. 校验手机号（是否符合正常手机号的命名规范）
        // 已经在工具类中给出校验手机号的代码，可以直接调用
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2. 如果不符合，返回错误信息
            return Result.fail("手机号格式错误，请重新输入！");
        }
        // 3. 如果符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
        // 原来是保存到session，然后优化成了保存到redis
        // 4. 保存验证码到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        // 5. 模拟发送验证码
        log.debug("发送短信验证码成功，验证码{}" , code);
        // 6. 返回
        return Result.ok();
    }

    /*

    // 登录功能
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1.校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        // 2.从redis中获取验证码，并且校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        System.out.println(code);
        if(cacheCode == null || !cacheCode.equals(code)){
            // 3. 不一致，直接报错
            return Result.fail("验证码错误！");
        }

        // 4. 一致，根据手机号查询用户
        User user = query().eq("phone", phone).one();
        // 5. 用户不存在，创建新用户
        if (user == null) {
            user = createUserWithPhone(phone);
        }
        // 6. 保存用户信息到redis中
        // 6.1 随机生成token，作为登录令牌，这个token就是我们redis中的key
        String token = UUID.randomUUID().toString(true);
        // 6.2 将user对象转为HashMap存储
        // 使用hash存储的原因是相比于string，耗费内存较少（String要多存一些"", : 等符号），而且修改比较方便，可以只改一个字段
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true).setFieldValueEditor((fieldName,fieldValue) -> fieldValue.toString()));
        // 6.3 存储
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY + token,userMap);
        // 6.4 设置token有效期
        stringRedisTemplate.expire(LOGIN_USER_KEY + token,LOGIN_USER_TTL,TimeUnit.MINUTES);
        // 7. 返回token
        return Result.ok(token);
    }

     */

    // 登录功能
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1.校验手机号
        String phone = loginForm.getPhone();
        String password = loginForm.getPassword();
        User user = null;
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }

        if(!StringUtil.isEmpty(password)){
            user = query().eq("phone", phone).one(); // 这里是用mybatisPlus进行的查询，根据手机号查询用户
            if (user == null) {
                return Result.fail("用户不存在，请使用验证码进行注册登录！");
            }
            String realPassword = user.getPassword();
            if(realPassword.equals(password)){
                System.out.println("登录成功");
            } else{
                return Result.fail("密码错误！");
            }
        } else {
            // 2.从redis中获取验证码，并且校验验证码
            String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
            String code = loginForm.getCode();
            System.out.println(code);
            if (cacheCode == null || !cacheCode.equals(code)) {
                // 3. 不一致，直接报错
                return Result.fail("验证码错误！");
            }

            // 4. 一致，根据手机号查询用户
            user = query().eq("phone", phone).one();
            // 5. 用户不存在，创建新用户
            if (user == null) {
                user = createUserWithPhone(phone);
            }
        }
        // 6. 保存用户信息到redis中
        // 6.1 随机生成token，作为登录令牌，这个token就是我们redis中的key
        String token = UUID.randomUUID().toString(true);
        // 6.2 将user对象转为Hash存储
        // 使用hash存储的原因是相比于string，耗费内存较少（String要多存一些"", : 等符号），而且修改比较方便，可以只改一个字段
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true).setFieldValueEditor((fieldName,fieldValue) -> fieldValue.toString()));
        // 6.3 存储
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY + token,userMap);
        // 6.4 设置token有效期
        stringRedisTemplate.expire(LOGIN_USER_KEY + token,LOGIN_USER_TTL,TimeUnit.MINUTES);
        // 7. 返回token
        return Result.ok(token);
    }

    @Override
    public Result logout() {
        UserHolder.removeUser();
        return Result.ok("已退出登录！");
    }

    @Override
    public Result sign() {
        // 获取当前登录的用户
        Long userId = UserHolder.getUser().getId();
        // 获取日期
        LocalDateTime now = LocalDateTime.now();
        // 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 写入redis
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 获取当前登录的用户
        Long userId = UserHolder.getUser().getId();
        // 获取日期
        LocalDateTime now = LocalDateTime.now();
        // 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 获取本月到今天为止签到记录，返回一个十进制的数字
        List<Long> result = stringRedisTemplate.opsForValue().bitField(key, BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        if(result == null || result.isEmpty()) {
            return Result.ok(0);
        }
        Long num = result.get(0);
        if(num == null || num == 0) {
            return Result.ok(0);
        }
        // 循环遍历
        int count = 0;
        while(true) {
            // 让这个数字与1做与运算，得到最后一个bit位
            if((num & 1) == 0) {
                // 如果为0，结束
                break;
            }else {
                // 不为0，计数器+1
                count ++;
            }
            // 右移一位，进行下一个bit位的比较
            num >>>= 1;
        }
        return Result.ok(count);
    }

    // 根据手机号创建用户
    private User createUserWithPhone(String phone) {
        // 1. 创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        // 2. 保存用户
        save(user);
        return user;
    }
}
