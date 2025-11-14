package com.jzo2o.market.handler;

import com.jzo2o.api.customer.CommonUserApi;
import com.jzo2o.api.customer.dto.response.CommonUserResDTO;
import com.jzo2o.common.expcetions.CommonException;
import com.jzo2o.common.utils.IdUtils;
import com.jzo2o.common.utils.ObjectUtils;
import com.jzo2o.market.constants.RedisConstants;
import com.jzo2o.market.model.domain.Coupon;
import com.jzo2o.market.model.dto.response.ActivityInfoResDTO;
import com.jzo2o.market.service.IActivityService;
import com.jzo2o.market.service.ICouponService;
import com.jzo2o.redis.handler.SyncProcessHandler;
import com.jzo2o.redis.model.SyncMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;


//定义bean的名称
//@Component("COUPON:SEIZE:SYNC")
@Component(RedisConstants.RedisKey.COUPON_SEIZE_SYNC_QUEUE_NAME)
@Slf4j
public class SeizeCouponSyncProcessHandler implements SyncProcessHandler<Object> {

    @Resource
    private IActivityService activityService;
    @Resource
    private ICouponService couponService;

    @Resource
    private CommonUserApi commonUserApi;

    //用于批量处理
    @Override
    public void batchProcess(List<SyncMessage<Object>> multiData) {
        //暂不支持针对hash结构的数据批量处理
        log.info("暂不支持针对hash结构的数据批量处理");
    }

    /**
     * 单条处理
     *
     * @param singleData  从hash结构中拿到的数据
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void singleProcess(SyncMessage<Object> singleData) {
        //hash结构的key
        String userId = singleData.getKey();
        //远程请求customer查询用户信息
        CommonUserResDTO commonUserResDTO = commonUserApi.findById(Long.parseLong(userId));
        if(ObjectUtils.isNull(commonUserResDTO)){
            throw new CommonException("没有此用户");
        }
        //取hash中value
        Object value = singleData.getValue();
        //活动id
        long activityId = Long.parseLong(value.toString());
        //查询活动
        ActivityInfoResDTO activityInfoResDTO = activityService.queryById(activityId);
        if(ObjectUtils.isNull(activityInfoResDTO)){
            throw new CommonException("没有此活动");
        }

        log.info("从抢券hash中拿到数据,userId:{},activityId:{}",userId,activityId);

        //修改活动表的库存
        activityService.deductStock(activityId);

        //向优惠券表插入数据
        Coupon coupon = new Coupon();
        //主键
        coupon.setId(IdUtils.getSnowflakeNextId());
        //活动名称
        coupon.setName(activityInfoResDTO.getName());
        //活动id
        coupon.setActivityId(activityId);
        //活动类型
        coupon.setType(activityInfoResDTO.getType());
        //优惠金额
        coupon.setDiscountAmount(activityInfoResDTO.getDiscountAmount());
        //满减金额
        coupon.setAmountCondition(activityInfoResDTO.getAmountCondition());
        //折扣率
        coupon.setDiscountRate(activityInfoResDTO.getDiscountRate());
        //优惠券的有效期
        coupon.setValidityTime(LocalDateTime.now().plusDays(activityInfoResDTO.getValidityDays()));
        //优惠券的状态默认为未使用
        coupon.setStatus(1);
        //用户id
        coupon.setUserId(Long.parseLong(userId));
        //用户名称
        coupon.setUserName(commonUserResDTO.getNickname());
        //用户电话
        coupon.setUserPhone(commonUserResDTO.getPhone());
        //插入优惠券表

        boolean save = couponService.save(coupon);
        if(!save){
            throw new CommonException("保存优惠券失败");
        }


    }
}
