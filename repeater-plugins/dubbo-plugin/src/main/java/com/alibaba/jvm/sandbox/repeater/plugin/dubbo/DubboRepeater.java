package com.alibaba.jvm.sandbox.repeater.plugin.dubbo;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.jvm.sandbox.repeater.plugin.core.bridge.ClassloaderBridge;
import com.alibaba.jvm.sandbox.repeater.plugin.core.impl.AbstractRepeater;
import com.alibaba.jvm.sandbox.repeater.plugin.core.spring.SpringContextAdapter;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.ClassUtils;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.Identity;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.Invocation;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.InvokeType;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeatContext;
import com.alibaba.jvm.sandbox.repeater.plugin.exception.RepeatException;
import com.alibaba.jvm.sandbox.repeater.plugin.spi.Repeater;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.kohsuke.MetaInfServices;

import java.util.HashMap;
import java.util.Map;


/**
 * {@link DubboRepeater} dubbo回放器
 * <p>
 *
 * @author zhaoyb1990
 */
@MetaInfServices(Repeater.class)
public class DubboRepeater extends AbstractRepeater {

    private final static String REFERECE_CONFIG_CLASS_NAME = "com.alibaba.dubbo.config.ReferenceConfig";
    private final static String URL_CLASS_NAME = "com.alibaba.dubbo.common.URL";
    private final static String PROTOCAL_CONFIG_CLASS_NAME = "com.alibaba.dubbo.config.ProtocolConfig";
    private final static String PROTOCAL_CONFIG_METHOD_GET_PORT = "getPort";
    private final static String APPLIATION_CONFIG_CLASS_NAME = "com.alibaba.dubbo.config.ApplicationConfig";
    private final static String REFERECE_CONFIG_METHOD_SET_URL = "setUrl";
    private final static String REFERECE_CONFIG_METHOD_SET_INTERFACE = "setInterface";
    private final static String REFERECE_CONFIG_METHOD_SET_TIMEOUT = "setTimeout";
    private final static String REFERECE_CONFIG_METHOD_SET_GENERIC = "setGeneric";
    private final static String REFERECE_CONFIG_METHOD_SET_APPLICATION = "setApplication";
    private final static String REFERECE_CONFIG_METHOD_GET = "get";
    private final static String PROXY_METHOD_INVOKE = "$invoke";
    private final static String URL_CLASSLOADER_NAME = "org.springframework.boot.loader.LaunchedURLClassLoader";
    private final static Integer DEFAULT_PORT = 20880;


    @Override
    protected Object executeRepeat(RepeatContext context) throws Exception {
        log.debug("DubboRepeater#executeRepeat start");

        Invocation invocation = context.getRecordModel().getEntranceInvocation();
        if (invocation.getType().name() != getType().name()) {
            throw new RepeatException("invoke type miss match, required invoke type is: " + invocation.getType());
        }


        Identity identity = invocation.getIdentity();
        log.debug("DubboRepeater#executeRepeat.identity={}", JSONObject.toJSONString(identity,SerializerFeature.IgnoreNonFieldGetter));

        Map<String,String> dubboUrlParams = getMapFromIdentityUri(identity.getUri());
        log.debug("DubboRepeater#executeRepeat.identity.uri={}", identity.getUri());

        String host = "127.0.0.1";
        String protocol = dubboUrlParams.get("protocol");
        dubboUrlParams.remove("protocol");
        Object protocolConfig = SpringContextAdapter.getBeanByType(PROTOCAL_CONFIG_CLASS_NAME);
        log.debug("DubboRepeater#executeRepeat.protocolConfig={}", JSONObject.toJSONString(protocolConfig, SerializerFeature.IgnoreNonFieldGetter));

        Object applicationConfig = SpringContextAdapter.getBeanByType(APPLIATION_CONFIG_CLASS_NAME);
        log.debug("DubboRepeater#executeRepeat.applicationConfig={}", JSONObject.toJSONString(applicationConfig, SerializerFeature.IgnoreNonFieldGetter));
        Integer port = DEFAULT_PORT;
        if(protocolConfig != null){
            port = (Integer) MethodUtils.invokeMethod(protocolConfig, PROTOCAL_CONFIG_METHOD_GET_PORT);

        }

        Object[] args = invocation.getRequest();
        // 根据入参转换parameterTypes，入参是基本类型(int.class)则会找到不方法，原因是toClass只会返回包装类型
        Class<?>[] parameterTypes =  ClassUtils.toClass(args);
        String[] array = identity.getEndpoint().split("~");
        // array[0]=/methodName
        String methodName = array[0].substring(1);

        String[] parameterTypesName = new String[parameterTypes.length];
        for(int i = 0;i<parameterTypes.length;i++){
            parameterTypesName[i] = parameterTypes[i].getName();
        }

        ClassLoader pre = Thread.currentThread().getContextClassLoader();
        log.debug("DubboRepeater#executeRepeat.pre={}", pre.getClass().getName());
        ClassLoader newClassLoader = ClassloaderBridge.instance().decode( URL_CLASSLOADER_NAME);
        Thread.currentThread().setContextClassLoader(newClassLoader);

        Class<?> urlClazz = ClassloaderBridge.instance().findClassInstance(URL_CLASS_NAME);

        Object repeatUrl = ConstructorUtils.invokeConstructor(urlClazz, protocol, host, port.equals(-1)? 20880 :port, dubboUrlParams);
        log.debug("DubboRepeater#executeRepeat.repeatUrl={}", repeatUrl.toString());

        Class<?> clazz = ClassloaderBridge.instance().findClassInstance(REFERECE_CONFIG_CLASS_NAME);
        log.debug("DubboRepeater#executeRepeat.clazz={}", clazz.getClassLoader().getClass().getName());


        String interfaceName = identity.getLocation();

        Object ref= ConstructorUtils.invokeConstructor(clazz);
        MethodUtils.invokeMethod(ref,REFERECE_CONFIG_METHOD_SET_URL, repeatUrl.toString());
        MethodUtils.invokeMethod(ref,REFERECE_CONFIG_METHOD_SET_INTERFACE, interfaceName);
        MethodUtils.invokeMethod(ref,REFERECE_CONFIG_METHOD_SET_TIMEOUT, 600000);
        MethodUtils.invokeMethod(ref,REFERECE_CONFIG_METHOD_SET_APPLICATION, applicationConfig);

        MethodUtils.invokeMethod(ref,REFERECE_CONFIG_METHOD_SET_GENERIC, true);

        Object proxyBean = MethodUtils.invokeMethod(ref, REFERECE_CONFIG_METHOD_GET);

        log.debug("DubboRepeater#executeRepeat.proxyBean={}", proxyBean.getClass().getName());

        Thread.currentThread().setContextClassLoader(pre);
        Object result =  MethodUtils.invokeMethod(proxyBean, PROXY_METHOD_INVOKE, methodName, parameterTypesName, args);

        return result;

    }

    @Override
    public InvokeType getType() {
        return InvokeType.DUBBO;
    }

    @Override
    public String identity() {
        return "dubbo";
    }

    private Map<String,String> getMapFromIdentityUri(String uri){
        Map<String,String> result = new HashMap<String, String>();
        String paramString = uri.split("\\?")[1];
        log.debug("DubboRepeater#getMapFromIdentityUri.paramString={}",paramString);

        String[] params = paramString.split("&");
        for(String param: params){
            log.debug("DubboRepeater#getMapFromIdentityUri.param={}",param);
            String key = param.split("=")[0];
            String value = param.split("=")[1];

            result.put(key, value);
        }
        return result;
    }

}

