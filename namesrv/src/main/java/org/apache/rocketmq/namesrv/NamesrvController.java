/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.namesrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;


public class NamesrvController {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final NamesrvConfig namesrvConfig;

    private final NettyServerConfig nettyServerConfig;

    /**
     * 定时线程池，单个线程
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "NSScheduledThread"));
    private final KVConfigManager kvConfigManager;
    private final RouteInfoManager routeInfoManager;

    private RemotingServer remotingServer;

    private BrokerHousekeepingService brokerHousekeepingService;

    private ExecutorService remotingExecutor;

    private Configuration configuration;
    private FileWatchService fileWatchService;

    /**
     * 构造初始化参数
     * @param namesrvConfig NameServer的基础配置
     * @param nettyServerConfig NameServer 作为服务端的一部分netty参数
     */
    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.namesrvConfig = namesrvConfig;//nameServer基础配置
        this.nettyServerConfig = nettyServerConfig;//服务端netty基础配置
        this.kvConfigManager = new KVConfigManager(this);//kv配置管理器
        this.routeInfoManager = new RouteInfoManager();//路由信息管理器
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.configuration = new Configuration(log, this.namesrvConfig, this.nettyServerConfig);//配置对象
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");//配置文件路径
    }

    public boolean initialize() {

        this.kvConfigManager.load();// 加载K/V配置管理器

        //构造NettyRemotingServer，根据是否用epoll模型，初始化了IO boss线程池和work线程池、public公共线程池等
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);

        //初始化线程池
        this.remotingExecutor = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));

        this.registerProcessor();

        //添加每10秒扫描存活broker的定时任务，移除不存活的broker
        this.scheduledExecutorService.scheduleAtFixedRate(NamesrvController.this.routeInfoManager::scanNotActiveBroker, 5, 10, TimeUnit.SECONDS);
        //添加每10分钟打印k/v配置的定时任务
        this.scheduledExecutorService.scheduleAtFixedRate(NamesrvController.this.kvConfigManager::printAllPeriodically, 1, 10, TimeUnit.MINUTES);

        if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
            // 构造文件监听服务，其实就是个线程，监听构造数组中的文件，在线程启动之后循环扫描这些文件，读取文件内容并对内容进行MD5和缓存的MD5串进行比对，
            // 数据有变动则MD5串必定不相同；如果文件内容发生变动，则回调listener的onChanged方法，重新加载这部分文件
            try {
                fileWatchService = new FileWatchService(
                    new String[] {
                        TlsSystemConfig.tlsServerCertPath,
                        TlsSystemConfig.tlsServerKeyPath,
                        TlsSystemConfig.tlsServerTrustCertPath
                    },
                    new FileWatchService.Listener() {
                        boolean certChanged, keyChanged = false;
                        @Override
                        public void onChanged(String path) {
                            if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                reloadServerSslContext();
                            }
                            if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                certChanged = true;
                            }
                            if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                keyChanged = true;
                            }
                            if (certChanged && keyChanged) {
                                certChanged = keyChanged = false;
                                reloadServerSslContext();
                            }
                        }
                        private void reloadServerSslContext() {
                            ((NettyRemotingServer) remotingServer).loadSslContext();
                        }
                    });
            } catch (Exception e) {
                log.warn("FileWatchService created error, can't load the certificate dynamically");
            }
        }

        return true;
    }

    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {

            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()),
                this.remotingExecutor);
        } else {

            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
        }
    }

    public void start() throws Exception {
        this.remotingServer.start();//启动netty服务网络监听

        if (this.fileWatchService != null) {
            this.fileWatchService.start();//启动文件监听线程
        }
    }

    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingExecutor.shutdown();
        this.scheduledExecutorService.shutdown();

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
