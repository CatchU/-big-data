package com.catchu.second.sort;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 某个设备的访问信息
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class AccessLogInfo implements Serializable,Comparable<AccessLogInfo> {

    /**
     * 时间戳
     */
    private Long timestamp;

    /**
     * 上行流量
     */
    private Integer upTraffic;

    /**
     * 下行流量
     */
    private Integer downTraffic;

    @Override
    public int compareTo(AccessLogInfo o1) {
        //先比较上行流量，如果相等，则比较下行流量
        if(getUpTraffic()-o1.getUpTraffic()==0){
            return getDownTraffic()-o1.getDownTraffic();
        }else{
            return getUpTraffic()-o1.getUpTraffic();
        }
    }
}
