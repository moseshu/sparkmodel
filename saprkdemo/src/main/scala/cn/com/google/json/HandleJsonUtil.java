package cn.com.google.json;

import org.apache.commons.collections.map.HashedMap;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by moses on 2017/10/26.
 */
public class HandleJsonUtil {
    public static void main(String[] args) {
    Map<String,String> a=new HashedMap();
    Map<String,String> clenResult=new HashedMap();
    a.put("userId","1111");
    a.put("channel","weibo");
    a.put("phone_num","15910630151");
    a.put("13035510109@0","0,1");
    a.put("18786101452@1","0,1");
    a.put("18285018616@0","0,1");
    a.put("13678503616@1","0,1");
    a.put("18685222231@0","0,1");
    a.put("18076275824@1","0,1");
    a.put("17785826328@0","0,1");
    a.put("18786824280@0","0,1");
    a.put("13035510109@1","0,1");
        Pattern pattern = Pattern.compile("\\d+\\@\\d");

        for(String key:a.keySet()){
            Matcher isNum = pattern.matcher(key);
            if(isNum.matches()){
            clenResult.put(key.split("@")[0],a.get(key));}
            else {
                clenResult.put(key,a.get(key));

            }
        }
        String applyedMobiles="";
        StringBuffer str=new StringBuffer();
        for(String mobile:applyedMobiles.split(",")){
            for(String filter:a.keySet()){
                if(filter.contains(mobile))
                    str.append(",").append(filter);
            }
        }
        System.out.println(str.toString().substring(1));
    }


}
