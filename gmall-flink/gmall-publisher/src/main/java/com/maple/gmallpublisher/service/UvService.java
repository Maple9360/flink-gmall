package com.maple.gmallpublisher.service;

import com.maple.gmallpublisher.mapper.UvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
@Service
public class UvService {

    @Autowired
    UvMapper uvMapper;




    public List<Map> selectUvByCh(int date){
        return uvMapper.selectUvByCh(date);
    }
}
