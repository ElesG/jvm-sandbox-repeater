package com.alibaba.jvm.sandbox.repeater.plugin.dubbo;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.jvm.sandbox.api.event.BeforeEvent;
import com.alibaba.jvm.sandbox.api.event.Event;
import com.alibaba.jvm.sandbox.api.event.ReturnEvent;
import com.alibaba.jvm.sandbox.repeater.plugin.core.impl.api.DefaultInvocationProcessor;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.LogUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.Identity;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.Invocation;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.InvokeType;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * {@link DubboProcessor}
 * <p>
 * dubbo consumer调用处理器，需要重写组装identity 和 组装request
 * </p>
 *
 * @author zhaoyb1990
 */
class DubboProcessor extends DefaultInvocationProcessor {

    private static final String ON_RESPONSE = "onResponse";

    private static final String INVOKE = "invoke";

    private static final String CLUSTER_INVOKER = "com.alibaba.dubbo.rpc.cluster.support.AbstractClusterInvoker";



    private Set<Integer> ignoreInvokeSet = new HashSet<Integer>(128);

    DubboProcessor(InvokeType type) {
        super(type);
    }

    @Override
    public Identity assembleIdentity(BeforeEvent event) {
        if(CLUSTER_INVOKER.equals(event.javaClassName)){
            return super.assembleIdentity(event);
        }

        // interfaceName
        Object invocation = ON_RESPONSE.equals(event.javaMethodName)?event.argumentArray[2]:event.argumentArray[1];
        Object invoker = ON_RESPONSE.equals(event.javaMethodName)?event.argumentArray[1]:event.argumentArray[0];
        Map<String,String> extra= new HashMap<String, String>();

        try {
            // methodName
            String methodName = (String) MethodUtils.invokeMethod(invocation, "getMethodName");
            Class<?>[] parameterTypes = (Class<?>[]) MethodUtils.invokeMethod(invocation, "getParameterTypes");

            String interfaceName = ((Class)MethodUtils.invokeMethod(invoker, "getInterface")).getName();
            Object dubboUrl= MethodUtils.invokeMethod(invoker, "getUrl");

            Map<String, String> dubboParamters= (Map<String, String>) MethodUtils.invokeMethod(dubboUrl, "getParameters");

            String protocol= (String) MethodUtils.invokeMethod(dubboUrl, "getProtocol");
            extra.put("protocol", protocol);
            extra.putAll(dubboParamters);

            extra = removeIpAndPortAndTimestamp(extra);

            log.debug("assembleIdentity.interfaceName={}", interfaceName);
            return new Identity(InvokeType.DUBBO.name(), interfaceName, getMethodDesc(methodName, parameterTypes), extra);
        } catch (Exception e) {
            // ignore
            LogUtil.error("error occurred when assemble dubbo request", e);
        }
        return new Identity(InvokeType.DUBBO.name(), "unknown", "unknown", extra);
    }

    @Override
    public Object[] assembleRequest(BeforeEvent event) {
        Object invocation;
        try {
            if(CLUSTER_INVOKER.equals(event.javaClassName)){
                invocation = event.argumentArray[0];
            }else {
                invocation = ON_RESPONSE.equals(event.javaMethodName) ? event.argumentArray[2] : event.argumentArray[1];
            }
            return (Object[]) MethodUtils.invokeMethod(invocation, "getArguments");
        } catch (Exception e) {
            // ignore
            LogUtil.error("error occurred when assemble dubbo request", e);
        }
        return null;
    }

    @Override
    public Object assembleMockResponse(BeforeEvent event, Invocation invocation) {

        // Result invoke(Invoker<?> invoker, Invocation invocation)
        try {
            Object dubboInvocation = CLUSTER_INVOKER.equals(event.javaClassName)?event.argumentArray[0]:event.argumentArray[1];
            Object response = invocation.getResponse();
            Class<?> dubboClass = event.javaClassLoader.loadClass("com.alibaba.dubbo.rpc.RpcResult");
            log.debug("DubboProcessor#assembleMockResponse.dubboInvocation={}", JSONObject.toJSONString(dubboInvocation, SerializerFeature.IgnoreNonFieldGetter));

            Object result = ConstructorUtils.invokeConstructor(dubboClass);
            Object attachments = MethodUtils.invokeMethod(dubboInvocation,"getAttachments");
            MethodUtils.invokeMethod(result, "setResult", response);
            MethodUtils.invokeMethod(result, "setAttachments", attachments);
            log.debug("DubboProcessor#assembleMockResponse.result={}", JSONObject.toJSONString(result, SerializerFeature.IgnoreNonFieldGetter));
            // 调用AsyncRpcResult#newDefaultAsyncResult返回;
            return result;
        } catch (ClassNotFoundException e) {
            LogUtil.error("no valid AsyncRpcResult class fount in classloader {}", event.javaClassLoader, e);
            return null;
        } catch (Exception e) {
            LogUtil.error("error occurred when assemble dubbo mock response", e);
            return null;
        }
    }

    @Override
    public Object assembleResponse(Event event) {
        // 在onResponse的before事件中组装response
        if (event.type == Event.Type.BEFORE && ON_RESPONSE.equals(((BeforeEvent)event).javaMethodName)) {
            Object appResponse = ((BeforeEvent) event).argumentArray[0];
            try {
                return MethodUtils.invokeMethod(appResponse, "getValue");
            } catch (Exception e) {
                // ignore
                LogUtil.error("error occurred when assemble dubbo response", e);
            }
        }

        if(event.type == Event.Type.RETURN){
            Object result = ((ReturnEvent) event).object;
            try {
                return MethodUtils.invokeMethod(result, "getValue");
            } catch (Exception e) {
                // ignore
                LogUtil.error("error occurred when assemble dubbo response", e);
            }
        }


        return null;
    }

    /**
     * 去掉ip、port、timesamp、pid等标识，防止跨环境回放失败
     * @param extra
     * @return
     */
    private Map<String, String> removeIpAndPortAndTimestamp(Map<String, String> extra){
        Map<String,String> result = new HashMap<>();

        for(String key : extra.keySet()){
            if(key.contains("ip") || key.contains("port") || key.contains("timestamp") || key.contains("pid")){
                continue;
            }
            result.put(key, extra.get(key));
        }

        return result;


    }
}
