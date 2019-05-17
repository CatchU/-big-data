package com.catchu.second.sort;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * 用于生成需要的模拟数据，并把数据写入到file中
 */
public class DataFileGenerator {
    public static void main(String[] args) {
        //生成100个设备号
        List<String> deviceIds = new ArrayList<>();
        for(int i=0;i<100;i++){
            deviceIds.add(getUuid());
        }

        //生成10000个时间戳和上下行流量
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<10000;i++){
            //时间戳
            long timestamp = System.currentTimeMillis() - random.nextInt(10000);
            //设备id
            String deviceId = deviceIds.get(random.nextInt(100));
            //上行流量
            int upTraffic = random.nextInt(10000);
            int downTraffic = random.nextInt(10000);

            sb.append(timestamp).append("\t").append(deviceId).append("\t").append(upTraffic).append("\t").append(downTraffic).append("\n");
        }

        //将数据写出到磁盘
        PrintWriter printWriter = null;
        try {
            printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream("./app-log.txt")));
            printWriter.write(sb.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }finally {
            printWriter.close();
        }
    }

    protected static String getUuid(){
        return UUID.randomUUID().toString().replace("-","");
    }
}
