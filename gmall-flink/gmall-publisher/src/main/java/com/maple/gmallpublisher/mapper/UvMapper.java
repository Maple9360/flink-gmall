package com.maple.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

public interface UvMapper {

    @Select("select ch,sum(uv_ct) uv,sum(uj_ct) uj from dws_traffic_vc_ch_ar_is_new_page_view_window where toYYYYMMDD(stt)=#{date} group by ch")
    List<Map> selectUvByCh(int date);

}
