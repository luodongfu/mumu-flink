package com.lovecws.mumu.flink.common.elasticsearch;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 索引管理
 * @date 2018-06-02 21:18
 */
public class ElasticsearchClient {

    public static final Logger log = Logger.getLogger(ElasticsearchClient.class);

    private TransportClient client;
    private EsConfig esConfig;

    public ElasticsearchClient(EsConfig esConfig) {
        this.esConfig = esConfig;
        client = client();
    }

    public TransportClient client() {
        if (client != null) {
            return client;
        }

        Settings settings = Settings.builder()
                .put("cluster.name", esConfig.getClusterName())
                .put("transport.tcp.compress", esConfig.isTcpCompress())
                .put("client.transport.sniff", false)
                .put("client.transport.ping_timeout", esConfig.getPingTimeout())
                .build();
        String[] hostnames = esConfig.getHost().split(",");
        String[] ports = esConfig.getPort().split(",");
        if (hostnames.length != ports.length) {
            throw new IllegalArgumentException("host [" + StringUtils.join(ports, ",") + "],port[" + StringUtils.join(ports, ",") + "]配置错误");
        }
        try {
            TransportAddress[] transportAddresses = new TransportAddress[hostnames.length];
            for (int i = 0; i < hostnames.length; i++) {
                try {
                    TransportAddress transportAddress = null;
                    //elasticsearch5.6 TransportAddress是一个接口
                    if (TransportAddress.class.isInterface()) {
                        transportAddress = (TransportAddress) ConstructorUtils.invokeConstructor(Class.forName("org.elasticsearch.common.transport.InetSocketTransportAddress"), InetAddress.getByName(hostnames[i]), Integer.parseInt(ports[i]));
                    }
                    //elasticsearch6.2 TransportAddress是一个类
                    else {
                        transportAddress = ConstructorUtils.invokeConstructor(TransportAddress.class, InetAddress.getByName(hostnames[i]), Integer.parseInt(ports[i]));
                    }
                    transportAddresses[i] = transportAddress;
                } catch (Exception ex) {
                    log.error(ex.getLocalizedMessage());
                }
            }
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(transportAddresses);
            log.info("初始化elasticsearch连接 host[" + StringUtils.join(hostnames, ",") + "],port[" + StringUtils.join(ports, ",") + "]....");
            return client;
        } catch (Exception e) {
            log.error(e.getLocalizedMessage(), e);
            throw new IllegalArgumentException("非法的hostname:" + StringUtils.join(hostnames, ",") + ",port:" + StringUtils.join(ports, ","));
        }
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
