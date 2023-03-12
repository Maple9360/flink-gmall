package com.maple.gmallpublisher.controller;

import com.maple.gmallpublisher.service.GMVService;
import com.maple.gmallpublisher.service.UvService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;

// @Controller
@RestController
// @RestController = @Controller + @ResponseBody 将返回结果直接返回到页面
@RequestMapping("/api/sugar/")
public class SugarController {


    @RequestMapping("test1")
    public String test1(){
        System.out.println("aaa");
        return "index.html";
    }


    @RequestMapping("test2")
    // requestParam接收前端参数，可加默认值
    public String test2(@RequestParam("nn") String name,
                        @RequestParam(value = "age",defaultValue = "18") int age){
        System.out.println(name + ":" + age);
        return "success";

    }


    @Autowired
    GMVService gmvService;

    @RequestMapping("gmv")
    // requestParam接收前端参数，可加默认值
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") int date){

        if (date == 0){
            date = getToday();
        }

        Double gmv = gmvService.selectGmv(date);

        return "{ " +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": "+ gmv +
                "}";
    }

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(sdf.format(ts));
    }



    @Autowired
    UvService uvService;

    @RequestMapping("uv")
    // requestParam接收前端参数，可加默认值
    public String getUV(@RequestParam(value = "date", defaultValue = "0") int date){

        if (date == 0){
            date = getToday();
        }

        HashMap<String, BigInteger> resultMap = new HashMap<>();

        List<Map> maps = uvService.selectUvByCh(date);

        for (Map map : maps) {

            resultMap.put((String)map.get("ch"),(BigInteger)map.get("uv"));
        }

        Set<String> chs = resultMap.keySet();
        Collection<BigInteger> uvs = resultMap.values();


        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\": [ \"" +
                StringUtils.join(chs,"\",\"") +
                "\"], " +
                "    \"series\": [ " +
                "      { " +
                "        \"name\": \"手机品牌\", " +
                "        \"data\": [ " +
                StringUtils.join(uvs,",") +
                "] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";





    }


}
