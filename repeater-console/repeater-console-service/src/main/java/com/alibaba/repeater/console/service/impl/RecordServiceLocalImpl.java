package com.alibaba.repeater.console.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.RecordWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.SerializerWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeatModel;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeaterResult;
import com.alibaba.repeater.console.dal.model.Record;
import com.alibaba.repeater.console.service.RecordService;
import com.alibaba.repeater.console.service.util.ConvertUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * {@link RecordServiceLocalImpl} 本地内存存储(示例DEMO）
 * <p>
 *
 * @author zhaoyb1990
 */
@Service("recordServiceLocal")
public class RecordServiceLocalImpl extends AbstractRecordService implements RecordService {

    /**
     * key:appName_traceId
     */
    private volatile Map<String, Record> recordCache = new ConcurrentHashMap<String, Record>(4096);

    /**
     * key:repeatId
     */
    private volatile Map<String, Record> repeatCache = new ConcurrentHashMap<String, Record>(4096);

    /**
     * key:repeatId
     */
    private volatile Map<String, RepeatModel> repeatModelCache = new ConcurrentHashMap<String, RepeatModel>(4096);

    /**
     * key:repeatId, value:record的traceId
     */
    private volatile Map<String, String> recordRepeatMap = new ConcurrentHashMap<String, String>(4096);

    @Override
    public RepeaterResult<String> saveRecord(String body) {
        try {
            RecordWrapper wrapper = SerializerWrapper.hessianDeserialize(body, RecordWrapper.class);
            if (wrapper == null || StringUtils.isEmpty(wrapper.getAppName())) {
                return RepeaterResult.builder().success(false).message("invalid request").build();
            }
            Record record = ConvertUtil.convertWrapper(wrapper, body);
            recordCache.put(buildUniqueKey(wrapper.getAppName(), wrapper.getTraceId()), record);
            return RepeaterResult.builder().success(true).message("operate success").data("-/-").build();
        } catch (Throwable throwable) {
            return RepeaterResult.builder().success(false).message(throwable.getMessage()).build();
        }
    }

    @Override
    public RepeaterResult<String> saveRepeat(String body) {
        try {
            RepeatModel rm = SerializerWrapper.hessianDeserialize(body, RepeatModel.class);
            Record record = repeatCache.remove(rm.getRepeatId());
            if (record == null) {
                return RepeaterResult.builder().success(false).message("invalid repeatId:" + rm.getRepeatId()).build();
            }
            RecordWrapper wrapper = SerializerWrapper.hessianDeserialize(record.getWrapperRecord(), RecordWrapper.class);
            rm.setOriginResponse(SerializerWrapper.hessianDeserialize(wrapper.getEntranceInvocation().getResponseSerialized()));
            repeatModelCache.put(rm.getRepeatId(), rm);
        } catch (Throwable throwable) {
            return RepeaterResult.builder().success(false).message(throwable.getMessage()).build();
        }
        return RepeaterResult.builder().success(true).message("operate success").data("-/-").build();
    }

    @Override
    public RepeaterResult<String> get(String appName, String traceId) {
        Record record = recordCache.get(buildUniqueKey(appName, traceId));
        if (record == null) {
            return RepeaterResult.builder().success(false).message("data not exits").build();
        }
        return RepeaterResult.builder().success(true).message("operate success").data(record.getWrapperRecord()).build();
    }

    @Override
    public RepeaterResult<String> repeat(String appName, String traceId, String repeatId) {
        final Record record = recordCache.get(buildUniqueKey(appName, traceId));
        if (record == null) {
            return RepeaterResult.builder().success(false).message("data does not exist").build();
        }
        RepeaterResult<String> pr = repeat(record, repeatId);
        if (pr.isSuccess()) {
            repeatCache.put(pr.getData(), record);
            recordRepeatMap.put(pr.getData(), record.getTraceId());
        }
        return pr;
    }

    @Override
    public RepeaterResult<List<RepeaterResult>> batchRepeat(String appName) {
        List<Record> records = getRecordByAppName(appName);

        if (records.isEmpty()) {
            return RepeaterResult.builder().success(false).message("data does not exist").build();
        }
        List<RepeaterResult<String>> results = new ArrayList<RepeaterResult<String>>();

        records.forEach(record -> {
            RepeaterResult<String> pr = repeat(record, null);
            if (pr.isSuccess()) {
                repeatCache.put(pr.getData(), record);
                recordRepeatMap.put(pr.getData(), record.getTraceId());
            }
            results.add(pr);
        });

        return RepeaterResult.builder().success(true).message("operate success").data(results).build();
    }

    @Override
    public RepeaterResult<RepeatModel> callback(String repeatId) {
        if (repeatCache.containsKey(repeatId)) {
            return RepeaterResult.builder().success(true).message("operate is going on").build();
        }
        RepeatModel rm = repeatModelCache.get(repeatId);
        // 进行Diff
        if (rm == null) {
            return RepeaterResult.builder().success(false).message("invalid repeatId:" + repeatId).build();
        }
        return RepeaterResult.builder().success(true).message("operate success").data(rm).build();
    }

    @Override
    public RepeaterResult<List<RepeatModel>> batchCallback(String appName) {
        // 根据appName获取对应的traceId
        List<Record> records = getRecordByAppName(appName);
        List<String> traceIds = records.stream().map(record -> record.getTraceId()).collect(Collectors.toList());

        // 根据traceId过滤获取的执行结果记录中获取其repeatId
        List<String> repeatIds = recordRepeatMap.keySet().stream().filter(key -> traceIds.contains(recordRepeatMap.get(key))).collect(Collectors.toList());

        // 根据获取到的repeatId列表获取对应的执行结果记录
        List<RepeatModel> repeatModels = repeatIds.stream().map(key -> repeatModelCache.get(key)).collect(Collectors.toList());

        // 当执行中缓存中存在所需要获取的执行结果记录的repeatId时，则认为这次批量录取回放还在执行中
        for(String repeatId:repeatIds){
            if (repeatCache.containsKey(repeatId)) {
                return RepeaterResult.builder().success(true).message("operate is going on").build();
            }
        }
        return RepeaterResult.builder().success(true).message("operate success").data(repeatModels).build();
    }

    private String buildUniqueKey(String appName, String traceId) {
        return appName + "-" + traceId;
    }

    /**
     * 根据应用名称从内存中获取对应的record
     * @param appName 应用名称
     * @return
     */
    private List<Record> getRecordByAppName(String appName){
        // 遍历recordCache的key，把key中包含了appName的对象放到records中
        List<String> recordKeys = recordCache.keySet().stream().filter(key -> key.contains(appName)).collect(Collectors.toList());
        List<Record> records = recordKeys.stream().map(key -> recordCache.get(key)).collect(Collectors.toList());
        return records;

    }
}
