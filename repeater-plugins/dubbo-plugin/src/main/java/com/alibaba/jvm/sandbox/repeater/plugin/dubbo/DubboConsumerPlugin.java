package com.alibaba.jvm.sandbox.repeater.plugin.dubbo;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.jvm.sandbox.api.event.Event;
import com.alibaba.jvm.sandbox.repeater.plugin.api.InvocationProcessor;
import com.alibaba.jvm.sandbox.repeater.plugin.core.impl.AbstractInvokePluginAdapter;
import com.alibaba.jvm.sandbox.repeater.plugin.core.model.EnhanceModel;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.InvokeType;
import com.alibaba.jvm.sandbox.repeater.plugin.spi.InvokePlugin;
import com.google.common.collect.Lists;
import org.kohsuke.MetaInfServices;

import java.util.List;

/**
 * {@link DubboConsumerPlugin} Apache dubbo consumer 插件
 * <p>
 * 拦截ConsumerContextFilte$ConsumerContextListenerr#onResponse行录制
 * 拦截ConsumerContextFilter#invoke进行MOCK
 * </p>
 *
 * @author zhaoyb1990
 */
@MetaInfServices(InvokePlugin.class)
public class DubboConsumerPlugin extends AbstractInvokePluginAdapter {

    @Override
    protected List<EnhanceModel> getEnhanceModels() {
        EnhanceModel invoke = EnhanceModel.builder().classPattern("com.alibaba.dubbo.rpc.filter.ConsumerContextFilter")
                .methodPatterns(EnhanceModel.MethodPattern.transform("invoke"))
                .watchTypes(Event.Type.BEFORE,Event.Type.RETURN, Event.Type.THROWS)
                .build();
        // 添加AbstractClusterInvoker的监听，mock到zk注册中心获取服务的步骤
        EnhanceModel abstractClusterInvoker = EnhanceModel.builder().classPattern("com.alibaba.dubbo.rpc.cluster.support.AbstractClusterInvoker")
                .methodPatterns(EnhanceModel.MethodPattern.transform("invoke"))
                .watchTypes(Event.Type.BEFORE,Event.Type.RETURN, Event.Type.THROWS)
                .build();
        return Lists.newArrayList(invoke, abstractClusterInvoker);
    }

    @Override
    protected InvocationProcessor getInvocationProcessor() {
        return new DubboProcessor(getType());
    }

    @Override
    public InvokeType getType() {
        return InvokeType.DUBBO;
    }

    @Override
    public String identity() {
        return "dubbo-consumer";
    }

    @Override
    public boolean isEntrance() {
        return false;
    }
}
