package com.jzo2o.market.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.jzo2o.common.expcetions.BadRequestException;
import com.jzo2o.common.model.PageResult;
import com.jzo2o.common.utils.*;
import com.jzo2o.market.constants.TabTypeConstants;
import com.jzo2o.market.enums.ActivityStatusEnum;
import com.jzo2o.market.mapper.ActivityMapper;
import com.jzo2o.market.model.domain.Activity;
import com.jzo2o.market.model.dto.request.ActivityQueryForPageReqDTO;
import com.jzo2o.market.model.dto.request.ActivitySaveReqDTO;
import com.jzo2o.market.model.dto.response.ActivityInfoResDTO;
import com.jzo2o.market.model.dto.response.SeizeCouponInfoResDTO;
import com.jzo2o.market.service.IActivityService;
import com.jzo2o.market.service.ICouponService;
import com.jzo2o.market.service.ICouponWriteOffService;
import com.jzo2o.mysql.utils.PageUtils;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.jzo2o.market.constants.RedisConstants.RedisKey.*;
import static com.jzo2o.market.enums.ActivityStatusEnum.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author itcast
 * @since 2023-09-16
 */
@Service
public class ActivityServiceImpl extends ServiceImpl<ActivityMapper, Activity> implements IActivityService {
    private static final int MILLION = 1000000;

    @Resource
    private RedisTemplate redisTemplate;

    @Resource
    private ICouponService couponService;

    @Resource
    private ICouponWriteOffService couponWriteOffService;

    @Override
    public PageResult<ActivityInfoResDTO> queryForPage(ActivityQueryForPageReqDTO activityQueryForPageReqDTO) {
        LocalDateTime now = DateUtils.now();
        // 1.查询准备
        LambdaQueryWrapper<Activity> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        // 查询条件
        lambdaQueryWrapper.eq(ObjectUtils.isNotNull(activityQueryForPageReqDTO.getId()), Activity::getId, activityQueryForPageReqDTO.getId())
                .like(StringUtils.isNotEmpty(activityQueryForPageReqDTO.getName()), Activity::getName, activityQueryForPageReqDTO.getName())
                .eq(ObjectUtils.isNotNull(activityQueryForPageReqDTO.getType()), Activity::getType, activityQueryForPageReqDTO.getType())
                .eq(ObjectUtils.isNotNull(activityQueryForPageReqDTO.getStatus()), Activity::getStatus, activityQueryForPageReqDTO.getStatus());

        // 排序
        lambdaQueryWrapper.orderByDesc(Activity::getId);
        // 分页
        Page<Activity> activityPage = new Page<>(activityQueryForPageReqDTO.getPageNo().intValue(), activityQueryForPageReqDTO.getPageSize().intValue());
        activityPage = baseMapper.selectPage(activityPage, lambdaQueryWrapper);
        return PageUtils.toPage(activityPage, ActivityInfoResDTO.class);
    }

    @Override
    public ActivityInfoResDTO queryById(Long id) {
        // 1.获取活动
        Activity activity = baseMapper.selectById(id);
        // 判空
        if (activity == null) {
            return new ActivityInfoResDTO();
        }
        // 2.数据转换，并返回信息
        ActivityInfoResDTO activityInfoResDTO = BeanUtils.toBean(activity, ActivityInfoResDTO.class);
        // 设置状态
//        activityInfoResDTO.setStatus(getStatus(activity.getDistributeStartTime(), activity.getDistributeEndTime(), activity.getStatus()));
        // 3.领取数量
//        Integer receiveNum = couponService.countReceiveNumByActivityId(activity.getId());
        Integer receiveNum = activity.getTotalNum() - activity.getStockNum();
        activityInfoResDTO.setReceiveNum(receiveNum);
        // 4.核销量
        Integer writeOffNum = couponWriteOffService.countByActivityId(id);
        activityInfoResDTO.setWriteOffNum(NumberUtils.null2Zero(writeOffNum));

        //
        return activityInfoResDTO;
    }

    @Override
    public void save(ActivitySaveReqDTO activitySaveReqDTO) {
        // 1.逻辑校验
        activitySaveReqDTO.check();
        // 2.活动数据组装
        // 转换
        Activity activity = BeanUtils.toBean(activitySaveReqDTO, Activity.class);
        // 状态
        activity.setStatus(NO_DISTRIBUTE.getStatus());
        //库存
        activity.setStockNum(activitySaveReqDTO.getTotalNum());
        if (activitySaveReqDTO.getId() == null) {
            activity.setId(IdUtils.getSnowflakeNextId());
        }
        //排序字段
//        long sortBy = DateUtils.toEpochMilli(activity.getDistributeStartTime()) * MILLION + activity.getId() % MILLION;
        // 3.保存
        saveOrUpdate(activity);
    }


    @Override
    public void updateStatus() {
        LocalDateTime now = DateUtils.now();
        // 1.更新已经进行中的状态
        lambdaUpdate()
                .set(Activity::getStatus, ActivityStatusEnum.DISTRIBUTING.getStatus())//更新活动状态为进行中
                .eq(Activity::getStatus, NO_DISTRIBUTE)//检索待生效的活动
                .le(Activity::getDistributeStartTime, now)//活动开始时间小于等于当前时间
                .gt(Activity::getDistributeEndTime, now)//活动结束时间大于当前时间
                .update();
        // 2.更新已经结束的
        lambdaUpdate()
                .set(Activity::getStatus, LOSE_EFFICACY.getStatus())//更新活动状态为已失效
                .in(Activity::getStatus, Arrays.asList(DISTRIBUTING.getStatus(), NO_DISTRIBUTE.getStatus()))//检索待生效及进行中的活动
                .lt(Activity::getDistributeEndTime, now)//活动结束时间小于当前时间
                .update();
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void revoke(Long id) {
        // 1.活动作废
        boolean update = lambdaUpdate()
                .set(Activity::getStatus, ActivityStatusEnum.VOIDED.getStatus())
                .eq(Activity::getId, id)
                .in(Activity::getStatus, Arrays.asList(NO_DISTRIBUTE.getStatus(), DISTRIBUTING.getStatus()))
                .update();
        if (!update) {
            return;
        }
        // 2.未使用优惠券作废
        couponService.revoke(id);

    }

    /**
     * 预热活动数据到Redis缓存中
     *
     * <p>该方法会查询未来30天内开始分发且状态为未分发或分发中的活动，
     * 按照分发开始时间升序排列，并将结果转换为JSON格式存储到Redis中。</p>
     *
     * <p>主要逻辑：
     * 1. 查询符合条件的活动数据
     * 2. 将活动数据转换为DTO列表
     * 3. 序列化为JSON字符串并存储到Redis
     * </p>
     */
    @Override
    public void preHeat() {
        // 获取当前时间
        LocalDateTime now = DateUtils.now();

        // 查询未来30天内开始分发且状态为未分发或分发中的活动，按分发开始时间升序排列
        List<Activity> list = lambdaQuery()
                .le(Activity::getDistributeStartTime, now.plusDays(30))
                .in(Activity::getStatus, Arrays.asList(NO_DISTRIBUTE.getStatus(), DISTRIBUTING.getStatus()))
                .orderByAsc(Activity::getDistributeStartTime)
                .list();

        // 确保列表不为null
        if (CollUtils.isEmpty(list)) {
            list = new ArrayList<>();
        }

        // 根据活动状态过滤并设置库存缓存：状态为1时直接更新，状态为2时仅在不存在时设置
        list.stream().filter(v->getStatus(v.getDistributeStartTime(), v.getDistributeEndTime(), v.getStatus())==1).forEach(
                v-> redisTemplate.opsForHash().put(String.format(COUPON_RESOURCE_STOCK, v.getId()%10),v.getId(),v.getTotalNum())
        );
        list.stream().filter(v->getStatus(v.getDistributeStartTime(), v.getDistributeEndTime(), v.getStatus())==2).forEach(
                v->redisTemplate.opsForHash().putIfAbsent(String.format(COUPON_RESOURCE_STOCK, v.getId()%10),v.getId(),v.getTotalNum())
        );

        // 将活动列表转换为抢券信息DTO列表
        List<SeizeCouponInfoResDTO> seizeCouponInfoResDTOS = BeanUtils.copyToList(list, SeizeCouponInfoResDTO.class);

        // 将DTO列表序列化为JSON字符串并存储到Redis缓存中
        String jsonStr = JsonUtils.toJsonStr(seizeCouponInfoResDTOS);
        redisTemplate.opsForValue().set(ACTIVITY_CACHE_LIST, jsonStr);
    }



    /**
     * 从缓存中查询抢券信息列表
     *
     * @param tabType 标签类型，用于区分抢券中和未开始分发的券
     * @return 抢券信息结果列表，根据tabType过滤出对应状态的数据
     */
    @Override
    public List<SeizeCouponInfoResDTO> queryForListFromCache(Integer tabType) {
        // 从Redis缓存中获取活动数据
        Object seizeCouponInfoStr = redisTemplate.opsForValue().get(ACTIVITY_CACHE_LIST);
        if (ObjectUtils.isNull(seizeCouponInfoStr)) {
            return CollUtils.emptyList();
        }

        // 将缓存中的JSON字符串转换为抢券信息列表
        List<SeizeCouponInfoResDTO> list = JsonUtils.toList(seizeCouponInfoStr.toString(), SeizeCouponInfoResDTO.class);

        // 根据tabType确定查询状态：抢券中对应分发中状态，否则为未分发状态
        int queryStatus = tabType == TabTypeConstants.SEIZING ? DISTRIBUTING.getStatus() : NO_DISTRIBUTE.getStatus();

        // 过滤出符合查询状态的数据，并设置相应的状态和剩余数量
        List<SeizeCouponInfoResDTO> collect = list.stream().filter(item -> queryStatus == getStatus(item.getDistributeStartTime(), item.getDistributeEndTime(), item.getStatus()))
                .peek(item -> {
                    item.setStatus(queryStatus);
                    item.setRemainNum(item.getTotalNum());
                }).collect(Collectors.toList());
        return collect;
    }


        /**
     * 根据发放开始时间、结束时间和当前状态，计算并返回最新的状态
     *
     * @param distributeStartTime 发放开始时间
     * @param distributeEndTime 发放结束时间
     * @param status 当前状态
     * @return 计算后的新状态
     */
    private int getStatus(LocalDateTime distributeStartTime, LocalDateTime distributeEndTime, Integer status) {
        // 判断是否处于发放中状态：未发放状态下，当前时间在发放时间范围内
        if (NO_DISTRIBUTE.equals(status)
                && distributeStartTime.isBefore(DateUtils.now())
                && distributeEndTime.isAfter(DateUtils.now())) {
            return DISTRIBUTING.getStatus();
        // 判断是否已失效：未发放状态下，当前时间已超过发放结束时间
        } else if (NO_DISTRIBUTE.equals(status)
                && distributeEndTime.isBefore(DateUtils.now())) {
            return LOSE_EFFICACY.getStatus();
        // 判断是否已失效：发放中状态下，当前时间已超过发放结束时间
        } else if (DISTRIBUTING.equals(status)
                && distributeEndTime.isBefore(DateUtils.now())) {
            return LOSE_EFFICACY.getStatus();
        }
        return status;
    }

}
