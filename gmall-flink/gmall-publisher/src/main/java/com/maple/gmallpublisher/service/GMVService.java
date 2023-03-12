package com.maple.gmallpublisher.service;

import com.maple.gmallpublisher.mapper.GMVMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class GMVService {

    @Autowired // 注入，防止空指针
    GMVMapper gmvMapper;


    public Double selectGmv(int date){
        return gmvMapper.selectGmv(date);
    }




}
