package com.example.demo_postgresql.service.impl;

import com.example.demo_postgresql.Entity.BrewLautertunEntity;
import com.example.demo_postgresql.Entity.BrewMaltrollermillEntity;
import com.example.demo_postgresql.dto.TopicConfigDTO;
import com.example.demo_postgresql.mapper.BrewLautertunMapper;
import com.example.demo_postgresql.mapper.BrewMaltrollermillMapper;
import com.example.demo_postgresql.service.MyMQTTClient;
import com.example.demo_postgresql.service.OperationService;
import com.example.demo_postgresql.util.Constants;
import com.example.demo_postgresql.util.GsonUtils;
import com.example.demo_postgresql.util.RedisUtil;
import com.sun.istack.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class OperationServiceImpl implements OperationService {

    @Value("${topic}")
    private String topic;

    @Autowired
    private BrewLautertunMapper brewLautertunMapper;

    @Autowired
    private BrewMaltrollermillMapper brewMaltrollermillMapper;

    @Autowired
    RedisUtil redisUtil;

    @Autowired
    MyMQTTClient myMQTTClient;

    List<TopicConfigDTO> topicConfigDTOList;
    private List<TopicConfigDTO> getFactory(){
        if (topicConfigDTOList == null){
            topicConfigDTOList = new ArrayList<>();
            TopicConfigDTO example1 = new TopicConfigDTO(
                    topic,
                    1,
                    "APAC-CN01223001-0301",
                    "APAC-CN01222001-2001"
            );
            topicConfigDTOList.add(example1);
            TopicConfigDTO example2 = new TopicConfigDTO(
                    topic,
                    2,
                    "APAC-CN01223002-0301",
                    "APAC-CN01222001-2002"
            );
            topicConfigDTOList.add(example2);
        }
        return topicConfigDTOList;
    }
    @Override
    public void getAndPushData() {
        List<TopicConfigDTO> topicConfigDTOList = getFactory();
        Long nowTime = System.currentTimeMillis()/1000;
        for (TopicConfigDTO topicConfigDTO : topicConfigDTOList) {
            List<Map<String, Object>> lautertunDataList;
            List<Map<String, Object>> maltrollermillDataList;

            Object object_l = redisUtil.get(Constants.KEY_LAST_TIME_PREFIX + topicConfigDTO.getLautertunTableUnitId());
            if (object_l != null){
                Long lastTime = Long.valueOf(String.valueOf(object_l));
                lautertunDataList = lautertunData(topicConfigDTO, nowTime, lastTime); // brew_lautertun表数据
            } else { // 第一次执行获取之前所有的数据并push
                lautertunDataList = lautertunDataWithStart(topicConfigDTO, nowTime); // brew_lautertun表数据
            }

            Object object_m = redisUtil.get(Constants.KEY_LAST_TIME_PREFIX + topicConfigDTO.getMaltrollermillTableUnitId());
            if (object_m != null){
                Long lastTime = Long.valueOf(String.valueOf(object_m));
                maltrollermillDataList = maltrollermillData(topicConfigDTO, nowTime, lastTime); // brew_maltrollermill表数据
            } else { // 第一次执行获取之前所有的数据并push
                maltrollermillDataList = maltrollermillDataWithStart(topicConfigDTO, nowTime); // brew_maltrollermill表数据
            }

            // 合并两张表查到的数据
            List<Map<String, Object>> pushDataList = new LinkedList<>();
            if (lautertunDataList != null && !lautertunDataList.isEmpty()){
                pushDataList.addAll(lautertunDataList);
            }
            if (maltrollermillDataList != null && 0 < maltrollermillDataList.size()){
                pushDataList.addAll(maltrollermillDataList);
            }

            // 整合数据并发送，如果某个特征值没有或值为null，则添加该特征值的初始值或最近一次的值
            Collections.sort(pushDataList, new Comparator<Map<String, Object>>() {
                public int compare(Map<String, Object> m1, Map<String, Object> m2) {
                    int m1Ts = Integer.parseInt(String.valueOf(m1.get("ts"))); // ts为10位的时间戳所以用int没关系
                    int m2Ts = Integer.parseInt(String.valueOf(m2.get("ts")));
                    return Integer.compare(m1Ts, m2Ts);
                }
            });
            Map<String,Object> initData = redisUtil.hmget(Constants.KEY_INIT_DATA); // 获取redis中每个特征值的初始值
            for (Map<String, Object> dataMap : pushDataList) {
                for (Map.Entry<String, Object> entry : initData.entrySet()) {
                    // 如果包含了该key
                    if (dataMap.containsKey(entry.getKey())){
                        if (dataMap.get(entry.getKey()) != null){
                            entry.setValue(dataMap.get(entry.getKey()));
                        } else {
                            dataMap.put(entry.getKey(), entry.getValue());
                        }
                    } else { // 如果没有该key,则新增该key
                        dataMap.put(entry.getKey(), entry.getValue());
                    }
                }
                // 数据整合后将该数据推送出去
                push(dataMap, topicConfigDTO.getAddr(), topicConfigDTO.getTopic());
            }
            // 将更新过的初始数据更新到redis，下次调用
            redisUtil.hmset(Constants.KEY_INIT_DATA, initData, -1);
        }
    }

    private List<Map<String, Object>> lautertunData(TopicConfigDTO topicConfigDTO, Long nowTime, Long lastTime){
        List<Map<String, Object>> mapList = null;
        Integer countLast = brewLautertunMapper.getCountByTime(topicConfigDTO.getLautertunTableUnitId(), lastTime); // 上次查询的总数
        Integer countNow = brewLautertunMapper.getCountByTime(topicConfigDTO.getLautertunTableUnitId(), nowTime); // 本次次查询的总数
        List<BrewLautertunEntity> brewLautertunEntityList =
                brewLautertunMapper.getEigenvalueDataByUnitIdOfRange(
                        topicConfigDTO.getLautertunTableUnitId(),
                        countNow - countLast,
                        countLast
                );

        redisUtil.set(Constants.KEY_LAST_TIME_PREFIX + topicConfigDTO.getLautertunTableUnitId(), nowTime, -1);
        if (brewLautertunEntityList != null){
            mapList = new LinkedList<>();
            for (BrewLautertunEntity brewLautertunEntity : brewLautertunEntityList) {
                Map<String, Object> map = GsonUtils.json2map(GsonUtils.toJson(brewLautertunEntity));
                clearNullData(map);
                mapList.add(map);
            }
        }
        return mapList;
    }

    /**
     * 第一次时使用（redis中没有记录）
     * @param topicConfigDTO
     * @param nowTime
     */
    private List<Map<String, Object>> lautertunDataWithStart(TopicConfigDTO topicConfigDTO, Long nowTime){
        List<Map<String, Object>> mapList = null;
        Integer count = brewLautertunMapper.getCountByTime(topicConfigDTO.getLautertunTableUnitId(), nowTime);
        List<BrewLautertunEntity> brewLautertunEntityList =
                brewLautertunMapper.getEigenvalueDataByUnitIdOfRange(
                        topicConfigDTO.getLautertunTableUnitId(),
                        count,
                        0
                );

        redisUtil.set(Constants.KEY_LAST_TIME_PREFIX + topicConfigDTO.getLautertunTableUnitId(), nowTime, -1);
        if (brewLautertunEntityList != null){
            mapList = new LinkedList<>();
            for (BrewLautertunEntity brewLautertunEntity : brewLautertunEntityList) {
                Map<String, Object> map = GsonUtils.json2map(GsonUtils.toJson(brewLautertunEntity));
                clearNullData(map);
                mapList.add(map);
            }
        }
        return mapList;
    }

    private List<Map<String, Object>> maltrollermillData(TopicConfigDTO topicConfigDTO, Long nowTime, Long lastTime){
        List<Map<String, Object>> mapList = null;
        Integer countLast = brewMaltrollermillMapper.getCountByTime(topicConfigDTO.getMaltrollermillTableUnitId(), lastTime); // 上次查询的总数
        Integer countNow = brewMaltrollermillMapper.getCountByTime(topicConfigDTO.getMaltrollermillTableUnitId(), nowTime); // 本次次查询的总数
        List<BrewMaltrollermillEntity> brewLautertunEntityList =
                brewMaltrollermillMapper.getEigenvalueDataByUnitIdOfRange(
                        topicConfigDTO.getMaltrollermillTableUnitId(),
                        countNow - countLast,
                        countLast
                );

        redisUtil.set(Constants.KEY_LAST_TIME_PREFIX + topicConfigDTO.getMaltrollermillTableUnitId(), nowTime, -1);
        if (brewLautertunEntityList != null){
            mapList = new LinkedList<>();
            for (BrewMaltrollermillEntity brewMaltrollermillEntity : brewLautertunEntityList) {
                Map<String, Object> map = GsonUtils.json2map(GsonUtils.toJson(brewMaltrollermillEntity));
                clearNullData(map);
                mapList.add(map);
            }
        }
        return mapList;
    }

    /**
     * 第一次时使用（redis中没有记录）
     * @param topicConfigDTO
     * @param nowTime
     */
    private List<Map<String, Object>> maltrollermillDataWithStart(TopicConfigDTO topicConfigDTO, Long nowTime){
        List<Map<String, Object>> mapList = null;
        Integer count = brewMaltrollermillMapper.getCountByTime(topicConfigDTO.getMaltrollermillTableUnitId(), nowTime);
        List<BrewMaltrollermillEntity> brewLautertunEntityList =
                brewMaltrollermillMapper.getEigenvalueDataByUnitIdOfRange(
                        topicConfigDTO.getMaltrollermillTableUnitId(),
                        count,
                        0
                );

        redisUtil.set(Constants.KEY_LAST_TIME_PREFIX + topicConfigDTO.getMaltrollermillTableUnitId(), nowTime, -1);
        if (brewLautertunEntityList != null){
            mapList = new LinkedList<>();
            for (BrewMaltrollermillEntity brewMaltrollermillEntity : brewLautertunEntityList) {
                Map<String, Object> map = GsonUtils.json2map(GsonUtils.toJson(brewMaltrollermillEntity));
                clearNullData(map);
                mapList.add(map);
            }
        }
        return mapList;
    }

    /**
     * 发送mqtt消息
     * @param object
     * @param addr
     * @param topic
     */
    private void push(@NotNull Object object, @NotNull Integer addr, @NotNull String topic){
        Map<String, Object> dataMap = GsonUtils.json2map(GsonUtils.toJson(object));
        clearNullData(dataMap);
        if (dataMap.size() > 1){ // 因为有一个元素为ts
            dataMap.put("addr", addr);
            List<Map<String, Object>> data = new ArrayList<>();
            data.add(dataMap);
            // 发送数据
            myMQTTClient.publish(GsonUtils.toJson(data), topic);
        }
    }

    /**
     * 删除map中值为null的元素
     * @param map
     * @return
     */
    private Map<String, Object> clearNullData(Map<String, Object> map){
        map.entrySet().removeIf(entry -> entry.getValue() == null);
        return map;
    }
}


