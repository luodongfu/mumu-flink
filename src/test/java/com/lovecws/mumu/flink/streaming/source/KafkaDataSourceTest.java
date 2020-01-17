package com.lovecws.mumu.flink.streaming.source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.lovecws.mumu.flink.common.model.atd.AtdEventModel;
import com.lovecws.mumu.flink.common.model.attack.AttackEventModel;
import com.lovecws.mumu.flink.common.model.gynetres.GynetresModel;
import com.lovecws.mumu.flink.common.util.AnnotationUtil;
import com.lovecws.mumu.flink.common.util.AvroUtil;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @program: mumu-flink
 * @description: kafka数据来源测试
 * @author: 甘亮
 * @create: 2019-05-31 09:27
 **/
@Slf4j
public class KafkaDataSourceTest {

    //    private String brokers = "172.31.134.215:9092";
//    private String brokers = "172.31.134.225:9092";
//    private String brokers = "172.31.132.20:9092";
    private String brokers = "172.31.134.214:9092";
//    private String brokers = "172.31.134.51:6667";
//    private String brokers = "172.31.134.51:6667,172.31.134.52:6667,172.31.134.53:6667";


    @Test
    public void sendMessage() {
        String topic = "industry_atd_topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "KafkaQuickStartProceducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 180000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 200000);
        props.put("retries", 3);//重试次数
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        try {
            RecordMetadata metadata = producer.send(new ProducerRecord<Integer, String>(topic, null, 1, "123456789")).get();
            System.out.println(DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss") + " : " + metadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void sendAtdMessage() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                String topic = "industry_atd_topic";

                Properties props = new Properties();
                props.put("bootstrap.servers", brokers);
                props.put("client.id", "KafkaQuickStartProceducer");
                props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("retries", 3);//重试次数
                props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 180000);
                props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 200000);
                KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

                BufferedReader bufferedReader = null;
                try {
                    //bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("E:/data/atd_test.txt")));
                    bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("E:\\data\\dataprocessing\\atd\\localfile\\test20190909.txt")));

                    String line = null;
                    while ((line = bufferedReader.readLine()) != null) {
                        RecordMetadata metadata = producer.send(new ProducerRecord<Integer, String>(topic, line)).get();
                        log.info("topic:" + metadata.topic() + " : " + " partition:" + metadata.partition() + " offset:" + metadata.offset());
                    }

                    RecordMetadata metadata = producer.send(new ProducerRecord<Integer, String>(topic, "123")).get();
                    log.info("topic:" + metadata.topic() + " : " + " partition:" + metadata.partition() + " offset:" + metadata.offset());
                    producer.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                } finally {
                    try {
                        if (bufferedReader != null) bufferedReader.close();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }, 10, 100, TimeUnit.SECONDS);
        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getAtdData() throws InterruptedException {
        String topic = "industry_atd_topic";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaQuickStartConsumer123456");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(true));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer<Integer, String>(props);
        consumer.subscribe(Arrays.asList(topic));

        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        for (ConsumerRecord<Integer, String> record : records) {
            log.info(record.topic() + " " + record.value());
        }
        Thread.sleep(10000);
        consumer.close();
    }

    @Test
    public void sendGynetresMessage() {
        String topic = "industry_gynetres_topic";

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "KafkaGynetresProceducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("retries", 3);//重试次数
        KafkaProducer<Integer, byte[]> producer = new KafkaProducer<Integer, byte[]>(props);

        try {
            Schema schema = AvroUtil.getSchema(GynetresModel.class);
            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

            for (int i = 0; i < 100; i++) {
                GenericRecord genericRecord = new GenericData.Record(schema);
                genericRecord.put("ip", "218.17.169.2" + i);
                genericRecord.put("port", "80");
                genericRecord.put("nmapProduct", "WordPress 1.3.7");
                genericRecord.put("createTime", System.currentTimeMillis());
                genericRecord.put("deviceName", "资产信息");

                byte[] bytes = recordInjection.apply(genericRecord);
                RecordMetadata metadata = producer.send(new ProducerRecord<Integer, byte[]>(topic, i, bytes)).get();
                log.info("topic:" + metadata.topic() + " : " + " partition:" + metadata.partition() + " offset:" + metadata.offset() + " " + bytes.length);
            }
            producer.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void getGynetresData() throws InterruptedException {
        String topic = "industry_gynetres_topic";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaGynetresConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaConsumer consumer = new KafkaConsumer<Integer, String>(props);
        consumer.subscribe(Arrays.asList(topic));

        Schema schema = AvroUtil.getSchema(GynetresModel.class);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        Set<TopicPartition> assignment = consumer.poll(0).partitions();
        if (assignment.size() > 0) {
            assignment.forEach(topicPartition -> consumer.seek(topicPartition, 0));
            log.info("kafka seek offset " + 0);
        }

        ConsumerRecords<Integer, byte[]> records = consumer.poll(1000);
        try {
            for (ConsumerRecord<Integer, byte[]> record : records) {
                log.info(record.topic() + " " + record.partition() + " " + record.offset());

                GenericRecord genericRecord = recordInjection.invert(record.value()).get();
                Map<String, Object> resultMap = new HashMap<>();
                genericRecord.getSchema().getFields().forEach(field -> {
                    Object o = genericRecord.get(field.pos());
                    if (o != null) {
                        resultMap.put(field.name(), String.valueOf(o));
                    }
                });

                String s = JSON.toJSONString(resultMap, SerializerFeature.IgnoreErrorGetter, SerializerFeature.WriteMapNullValue);
                GynetresModel gynetresModel = JSON.parseObject(s, GynetresModel.class);
                System.out.println(JSON.toJSONString(gynetresModel));
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
        consumer.close();
    }


    @Test
    public void sendAttackMessage() {
        String topic = "AttackTopic";

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "KafkaAttackTopicProceducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("retries", 3);//重试次数
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        try {
            for (int i = 0; i < 1000; i++) {
                AttackEventModel attackEventModel = new AttackEventModel();
                attackEventModel.setSourceIP("222.242.230.226");
                attackEventModel.setSourcePort(80 + i);
                attackEventModel.setSourceCompany("湘阴县人民政府");
                attackEventModel.setAttackLevel(i);

                attackEventModel.setDestIP("61.240.239.24");
                attackEventModel.setDestPort(8080);
                attackEventModel.setDestCompany("徐师师亲友集团");
                RecordMetadata metadata = producer.send(new ProducerRecord<Integer, String>(topic, JSON.toJSONString(attackEventModel))).get();
                log.info("topic:" + metadata.topic() + " : " + " partition:" + metadata.partition() + " offset:" + metadata.offset());
            }

            producer.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void timestamp() throws UnsupportedEncodingException {
        TimeStamp timeStamp = new TimeStamp(new Date().getTime());
        System.out.println(timeStamp.getTime());
        String s = "skksks\n\r\tasakak我愛你";
        System.out.println(URLEncoder.encode(s, "UTF-8"));
        System.out.println(File.separator);

        System.out.println(AnnotationUtil.getModularAnnotation("storage", "ftp1"));
        String ss = "name|sllsls|";
        for (String str : ss.split("\\|")) {
            System.out.println(str);
        }
        System.out.println(StringUtils.join(ss.split("\\|"), "|"));

        Field[] allFields = FieldUtils.getAllFields(AtdEventModel.class);
        for (Field field : allFields) {
            String type = field.getType().getSimpleName();
            System.out.println(type);
        }
    }

    @Test
    public void sendFlowMessage() throws IOException, ExecutionException, InterruptedException {
        String topic = "industry_flow_topic";

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "KafkaAttackTopicProceducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("retries", 3);//重试次数
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        for (int i = 0; i < 1; i++) {
            BufferedReader bufferedReader = new BufferedReader(new FileReader("E:\\data\\flow.json"));
            String readLine = null;
            while ((readLine = bufferedReader.readLine()) != null) {
                RecordMetadata metadata = producer.send(new ProducerRecord<Integer, String>(topic, JSON.toJSONString(JSON.parse(readLine)))).get();
                log.info("topic:" + metadata.topic() + " : " + " partition:" + metadata.partition() + " offset:" + metadata.offset());
            }
            bufferedReader.close();

            Thread.sleep(40000);
        }

        producer.close();
    }

    @Test
    public void json() {
        String str = "{\"AllCount\":95351,\"AllFlow\":65447202,\"ActualAllCount\":779672,\"ActualAllFlow\":536157640,\"ListFlowCount\":[{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"101.28.132.235\",\"DestIP\":\"58.252.15.177\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:49\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"101.28.132.234\",\"DestIP\":\"58.252.15.177\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":110,\"MinSize\":110,\"AverageSize\":110,\"Size\":110,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"221.4.0.57\",\"DestIP\":\"58.252.58.141\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD No.2 Xin Cheng Road, Room R6,Songshan Lake Technology Park Dongguan 523808 CN\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":74,\"MinSize\":74,\"AverageSize\":74,\"Size\":74,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"58.252.12.193\",\"DestIP\":\"120.80.153.29\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD No.2 Xin Cheng Road, Room R6,Songshan Lake Technology Park Dongguan 523808 CN\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"218.104.190.167\",\"DestIP\":\"119.167.152.254\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD No.2 Xin Cheng Road, Room R6,Songshan Lake Technology Park Dongguan 523808 CN\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"106.120.159.68\",\"DestIP\":\"120.86.65.108\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"106.120.159.68\",\"DestIP\":\"58.252.15.177\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:04:00\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"118.121.192.16\",\"DestIP\":\"218.104.189.137\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"59.49.41.7\",\"DestIP\":\"218.104.189.137\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"129.211.129.164\",\"DestIP\":\"218.104.189.137\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000}],\"Uuid\":\"e7d2c211-804f-4d1a-a94f-b05715cb2129\",\"CountTime\":\"2019-07-11 11:04:14\"}";
        str = "{\"AllCount\":95351,\"AllFlow\":65447202,\"ActualAllCount\":779672,\"ActualAllFlow\":536157640,\"ListFlowCount\":[{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"101.28.132.235\",\"DestIP\":\"58.252.15.177\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:49\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"101.28.132.234\",\"DestIP\":\"58.252.15.177\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":110,\"MinSize\":110,\"AverageSize\":110,\"Size\":110,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"221.4.0.57\",\"DestIP\":\"58.252.58.141\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD No.2 Xin Cheng Road, Room R6,Songshan Lake Technology Park Dongguan 523808 CN\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":74,\"MinSize\":74,\"AverageSize\":74,\"Size\":74,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"58.252.12.193\",\"DestIP\":\"120.80.153.29\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD No.2 Xin Cheng Road, Room R6,Songshan Lake Technology Park Dongguan 523808 CN\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"218.104.190.167\",\"DestIP\":\"119.167.152.254\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD No.2 Xin Cheng Road, Room R6,Songshan Lake Technology Park Dongguan 523808 CN\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"106.120.159.68\",\"DestIP\":\"120.86.65.108\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"106.120.159.68\",\"DestIP\":\"58.252.15.177\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:04:00\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"118.121.192.16\",\"DestIP\":\"218.104.189.137\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"59.49.41.7\",\"DestIP\":\"218.104.189.137\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"129.211.129.164\",\"DestIP\":\"218.104.189.137\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000}],\"Uuid\":\"e7d2c211-804f-4d1a-a94f-b05715cb2129\",\"CountTime\":\"2019-07-11 11:04:14\"}";
        str = "{\"AllCount\":95351,\"AllFlow\":65447202,\"ActualAllCount\":779672,\"ActualAllFlow\":536157640,\"ListFlowCount\":[{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"101.28.132.235\",\"DestIP\":\"58.252.15.177\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:49\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"101.28.132.234\",\"DestIP\":\"58.252.15.177\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":110,\"MinSize\":110,\"AverageSize\":110,\"Size\":110,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"221.4.0.57\",\"DestIP\":\"58.252.58.141\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD No.2 Xin Cheng Road, Room R6,Songshan Lake Technology Park Dongguan 523808 CN\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":74,\"MinSize\":74,\"AverageSize\":74,\"Size\":74,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"58.252.12.193\",\"DestIP\":\"120.80.153.29\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD No.2 Xin Cheng Road, Room R6,Songshan Lake Technology Park Dongguan 523808 CN\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"218.104.190.167\",\"DestIP\":\"119.167.152.254\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD No.2 Xin Cheng Road, Room R6,Songshan Lake Technology Park Dongguan 523808 CN\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"106.120.159.68\",\"DestIP\":\"120.86.65.108\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"106.120.159.68\",\"DestIP\":\"58.252.15.177\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:04:00\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"118.121.192.16\",\"DestIP\":\"218.104.189.137\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":60,\"MinSize\":60,\"AverageSize\":60,\"Size\":60,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"59.49.41.7\",\"DestIP\":\"218.104.189.137\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000},{\"PayloadDataList\":null,\"MaxSize\":98,\"MinSize\":98,\"AverageSize\":98,\"Size\":98,\"Count\":1,\"ProtocolName\":\"ICMP4\",\"DetailProtocol\":null,\"SourceBusiness\":null,\"DestBusiness\":null,\"SourceCompany\":null,\"DestCompany\":null,\"FuncCode\":null,\"Descript\":null,\"IsWrite\":false,\"DataType\":0,\"SourceIP\":\"129.211.129.164\",\"DestIP\":\"218.104.189.137\",\"SourcePort\":0,\"DestPort\":0,\"CountTime\":\"2019-07-11 11:03:50\",\"SourceBrand\":\"Cisco Systems, Inc 80 West Tasman Drive San Jose CA 94568 US\",\"DestBrand\":\"HUAWEI TECHNOLOGIES CO.,LTD D1-4,Huawei Industrial Base,Bantian,Longgang CN\",\"MaxInterverTime\":10000,\"MinInterverTime\":10000,\"AverageIntervalTime\":10000}],\"Uuid\":\"e7d2c211-804f-4d1a-a94f-b05715cb2129\",\"CountTime\":\"2019-07-11 11:04:14\"}";
        Map map = JSON.parseObject(str, Map.class);
        System.out.println(map);
        System.out.println("\"".length());
    }
}
