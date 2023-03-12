package com.maple.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

public interface GMVMapper {

    @Select("select sum(order_amount) from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date}")
    Double selectGmv(int date);


}
