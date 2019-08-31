package com.alibaba.jvm.sandbox.repeater.module;

import com.alibaba.jvm.sandbox.api.Information;
import com.alibaba.jvm.sandbox.api.Information.Mode;
import com.alibaba.jvm.sandbox.api.Module;
import com.alibaba.jvm.sandbox.api.ModuleLifecycle;
import com.alibaba.jvm.sandbox.api.annotation.Command;
import com.alibaba.jvm.sandbox.api.resource.ConfigInfo;
import com.alibaba.jvm.sandbox.api.resource.LoadedClassDataSource;
import com.alibaba.jvm.sandbox.api.resource.ModuleController;
import com.alibaba.jvm.sandbox.api.resource.ModuleEventWatcher;
import com.alibaba.jvm.sandbox.repeater.module.advice.SpringInstantiateAdvice;
import com.alibaba.jvm.sandbox.repeater.module.classloader.PluginClassLoader;
import com.alibaba.jvm.sandbox.repeater.module.impl.JarFileLifeCycleManager;
import com.alibaba.jvm.sandbox.repeater.module.util.LogbackUtils;
import com.alibaba.jvm.sandbox.repeater.plugin.api.ConfigManager;
import com.alibaba.jvm.sandbox.repeater.plugin.core.StandaloneSwitch;
import com.alibaba.jvm.sandbox.repeater.plugin.core.impl.api.DefaultInvocationListener;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.PathUtils;
import com.alibaba.jvm.sandbox.repeater.plugin.Constants;
import com.alibaba.jvm.sandbox.repeater.plugin.api.Broadcaster;
import com.alibaba.jvm.sandbox.repeater.plugin.api.InvocationListener;
import com.alibaba.jvm.sandbox.repeater.plugin.api.LifecycleManager;
import com.alibaba.jvm.sandbox.repeater.plugin.core.bridge.ClassloaderBridge;
import com.alibaba.jvm.sandbox.repeater.plugin.core.bridge.RepeaterBridge;
import com.alibaba.jvm.sandbox.repeater.plugin.core.eventbus.EventBusInner;
import com.alibaba.jvm.sandbox.repeater.plugin.core.eventbus.RepeatEvent;
import com.alibaba.jvm.sandbox.repeater.plugin.core.model.ApplicationModel;
import com.alibaba.jvm.sandbox.repeater.plugin.core.serialize.SerializeException;
import com.alibaba.jvm.sandbox.repeater.plugin.core.spring.SpringContextInnerContainer;
import com.alibaba.jvm.sandbox.repeater.plugin.core.trace.TtlConcurrentAdvice;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.ExecutorInner;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.SerializerWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.InvokeType;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeaterConfig;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeaterResult;
import com.alibaba.jvm.sandbox.repeater.plugin.exception.PluginLifeCycleException;
import com.alibaba.jvm.sandbox.repeater.plugin.spi.InvokePlugin;
import com.alibaba.jvm.sandbox.repeater.plugin.spi.Repeater;
import com.alibaba.jvm.sandbox.repeater.plugin.spi.SubscribeSupporter;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 *
 * @author zhaoyb1990
 */
@MetaInfServices(Module.class)
@Information(id = com.alibaba.jvm.sandbox.repeater.module.Constants.MODULE_ID, author = "zhaoyb1990", version = com.alibaba.jvm.sandbox.repeater.module.Constants.VERSION)
public class RepeaterModule implements Module, ModuleLifecycle {

    private final static Logger log = LoggerFactory.getLogger(RepeaterModule.class);

    // 事件观察者
    @Resource
    private ModuleEventWatcher eventWatcher;

    // 模块控制接口,控制模块的激活和冻结
    @Resource
    private ModuleController moduleController;

    // sandbox启动配置，这里只用来判断启动模式
    @Resource
    private ConfigInfo configInfo;

    //已加载类数据源,可以获取到所有已加载类的集合
    @Resource
    private LoadedClassDataSource loadedClassDataSource;

    //消息广播服务；用于采集流量之后的消息分发（保存录制记录，保存回放结果、拉取录制记录）
    private Broadcaster broadcaster;

    // 调用监听器
    private InvocationListener invocationListener;

    // 配置管理器，实现拉取配置
    private ConfigManager configManager;

    // 插件加载器
    private LifecycleManager lifecycleManager;

    //插件列表
    private List<InvokePlugin> invokePlugins;

    // 是否完成初始化
    private AtomicBoolean initialized = new AtomicBoolean(false);

    public RepeaterModule() {
    }

    @Override
    public void onLoad() throws Throwable {
        //模块加载，模块开始加载之前
        // 初始化日志框架
        LogbackUtils.init(PathUtils.getConfigPath() + "/repeater-logback.xml");
        // 获取启动模式
        Mode mode = configInfo.getMode();
        log.info("module on loaded,id={},version={},mode={}", com.alibaba.jvm.sandbox.repeater.module.Constants.MODULE_ID, com.alibaba.jvm.sandbox.repeater.module.Constants.VERSION, mode);
        /* agent方式启动 */
        if (mode == Mode.AGENT) {
            log.info("agent launch mode,use Spring Instantiate Advice to register bean.");
            //  SpringContext内部容器的是否agent模式设置为真
            SpringContextInnerContainer.setAgentLaunch(true);
            // spring初始化拦截器，agent启动模式下拦截记录beanName和bean
            SpringInstantiateAdvice.watcher(this.eventWatcher).watch();
            // 模块激活
            moduleController.active();
        }
    }

    @Override
    public void onUnload() throws Throwable {
        // 模块卸载，模块开始卸载之前调用
        // 释放插件加载资源，尽可能关闭pluginClassLoader
        if (lifecycleManager != null) {
            lifecycleManager.release();
        }
    }

    @Override
    public void onActive() throws Throwable {
        // 模块激活 就打印一个日志
        log.info("onActive");
    }

    @Override
    public void onFrozen() throws Throwable {
        // 模块冻结 就打印一个日志
        log.info("onFrozen");
    }

    @Override
    public void loadCompleted() {
        // 模块加载完成，模块完成加载后调用！
        // 这里不是很懂，因为这个虽然是一个多线程，但是只执行一次，run中内容还是顺序进行的。
        ExecutorInner.execute(new Runnable() {
            @Override
            public void run() {
                // 根据使用模式是单机版还是服务端板来获取拉取配置的实现方法
                configManager = StandaloneSwitch.instance().getConfigManager();
                // 根据使用模式是单机版还是服务端板来获取消息广播服务
                broadcaster = StandaloneSwitch.instance().getBroadcaster();
                // 调用监听器实例化
                invocationListener = new DefaultInvocationListener(broadcaster);
                // 拉取配置
                RepeaterResult<RepeaterConfig> pr = configManager.pullConfig();
                if (pr.isSuccess()) {
                    // 如果配置拉取成功
                    log.info("pull repeater config success,config={}", pr.getData());
                    // 根据已加载类数据源初始化类加载器连接桥
                    ClassloaderBridge.init(loadedClassDataSource);
                    // 根据配置进行插件初始化
                    initialize(pr.getData());
                }
            }
        });
    }

    /**
     * 初始化插件
     *
     * @param config 配置文件
     */
    private synchronized void initialize(RepeaterConfig config) {
        // 如果插件没有被初始化，才开始初始化
        if (initialized.compareAndSet(false, true)) {
            try {
                // http需要特殊路由操作，使用到容器里面的servlet-api
                PluginClassLoader.Routing[] routingArray = null;
                if (config.getPluginIdentities().contains(InvokeType.HTTP.name())) {
                    int retryTime = 60;
                    // Agent启动方式下类可能未加载完
                    // 所以这里不断重试60次进行查询查询
                    while (configInfo.getMode() == Mode.AGENT && --retryTime > 0
                            && ClassloaderBridge.instance().findClassInstances(Constants.SERVLET_API_NAME).size() == 0) {
                        try {
                            log.info("http plugin required servlet-api class router,waiting for class loading");
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                    // 重试结束后获取httpServlet的类实例列表
                    // 如列表中超过一个httpServlet或者没有httpServlet都会导致http插件使用失败
                    List<Class<?>> instances = ClassloaderBridge.instance().findClassInstances(Constants.SERVLET_API_NAME);
                    if (instances.size() > 1) {
                        throw new RuntimeException("found multiple servlet-api loaded in container, can't use http plugin");
                    }
                    // 如列表中只有一个httpServlet时将这个httpServlet加到插件加载器的路由中
                    if (instances.size() == 1){
                        Class<?> aClass = instances.get(0);
                        routingArray = new PluginClassLoader.Routing[]{new PluginClassLoader.Routing(aClass.getClassLoader(), "^javax.servlet..*")};
                    } else {
                        config.getPluginIdentities().remove(InvokeType.HTTP.name());
                        log.info("http plugin required servlet-api class router, but found no valid class in classloader, ignore http plugin");
                    }
                }
                // 读取插件路径，如无则驱默认
                String pluginsPath;
                if (StringUtils.isEmpty(config.getPluginsPath())) {
                    pluginsPath = PathUtils.getPluginPath();
                } else {
                    pluginsPath = config.getPluginsPath();
                }
                lifecycleManager = new JarFileLifeCycleManager(pluginsPath, routingArray);
                // 装载插件
                invokePlugins = lifecycleManager.loadInvokePlugins();
                // 在全局的应用模型中插入当前配置
                ApplicationModel.instance().setConfig(config);
                // 加载好插件知乎，遍历列表运行插件初始化
                for (InvokePlugin invokePlugin : invokePlugins) {
                    try {
                        // 设置插件配置
                        if (invokePlugin.enable(config)) {
                            log.info("enable plugin {} success", invokePlugin.identity());
                            // 配置插件观察时事件
                            invokePlugin.watch(eventWatcher, invocationListener);
                        }
                    } catch (PluginLifeCycleException e) {
                        log.info("watch plugin occurred error", e);
                    }
                }
                // 装载回放器
                List<Repeater> repeaters = lifecycleManager.loadRepeaters();
                for (Repeater repeater : repeaters) {
                    if (repeater.enable(config)) {
                        repeater.setBroadcast(broadcaster);
                    }
                }
                RepeaterBridge.instance().build(repeaters);
                // 装载消息订阅器
                List<SubscribeSupporter> subscribes = lifecycleManager.loadSubscribes();
                for (SubscribeSupporter subscribe : subscribes) {
                    subscribe.register();
                }
                TtlConcurrentAdvice.watcher(eventWatcher).watch(config);
            } catch (Throwable throwable) {
                initialized.compareAndSet(true, false);
                log.error("error occurred when initialize module", throwable);
            }
        }
    }

    /**
     * 回放http接口
     * 接口路径/sandbox/default/module/http/repeater/repeat
     *
     * @param req    请求参数
     * @param writer printWriter
     */
    @Command("repeat")
    public void repeat(final Map<String, String> req, final PrintWriter writer) {
        try {
            // 判断是否有"_data"参数，如果没有则返回报错
            String data = req.get(Constants.DATA_TRANSPORT_IDENTIFY);
            if (StringUtils.isEmpty(data)) {
                writer.write("invalid request, cause parameter {" + Constants.DATA_TRANSPORT_IDENTIFY + "} is required");
                return;
            }
            // 将回放请求的参数保存到RepeatEvent对象，并将这个对象推送到回放事件总线
            RepeatEvent event = new RepeatEvent();
            Map<String, String> requestParams = new HashMap<String, String>(16);
            for (Map.Entry<String, String> entry : req.entrySet()) {
                requestParams.put(entry.getKey(), entry.getValue());
            }
            event.setRequestParams(requestParams);
            EventBusInner.post(event);
            writer.write("submit success");
        } catch (Throwable e) {
            writer.write(e.getMessage());
        }
    }

    /**
     * 配置推送接口
     * 接口路径/sandbox/default/module/http/repeater/pushConfig
     *
     * @param req    请求参数
     * @param writer printWriter
     */
    @Command("pushConfig")
    public void pushConfig(final Map<String, String> req, final PrintWriter writer) {
        // 判断是否有"_data"参数，如果没有则返回报错
        String data = req.get(Constants.DATA_TRANSPORT_IDENTIFY);
        if (StringUtils.isEmpty(data)) {
            writer.write("invalid request, cause parameter {" + Constants.DATA_TRANSPORT_IDENTIFY + "} is required");
            return;
        }
        try {
            // 将请求参数序列化之后获取RepeaterConfig，并且通知插件更新配置
            RepeaterConfig config = SerializerWrapper.hessianDeserialize(data, RepeaterConfig.class);
            noticeConfigChange(config);
            writer.write("config push success");
        } catch (SerializeException e) {
            writer.write("invalid request, cause deserialize config failed, reason = {" + e.getMessage() + "}");
        }
    }

    /**
     * 通知插件配置变更
     *
     * @param config 配置文件
     */
    private void noticeConfigChange(final RepeaterConfig config) {
        // 如果模块初始化已成功，逐个插件进行更新通知
        if (initialized.get()) {
            for (InvokePlugin invokePlugin : invokePlugins) {
                try {
                    invokePlugin.onConfigChange(config);
                } catch (PluginLifeCycleException e) {
                    log.error("error occurred when notice config, plugin ={}", invokePlugin.getType().name(), e);
                }
            }
        }
    }
}
