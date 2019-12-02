package com.lovecws.mumu.flink.common.elasticsearch;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author 甘亮
 * @Description: 基础查询
 * @date 2018/7/12 12:50
 */
public class ElasticsearchBaseQuery implements Serializable {

    public static final Logger log = Logger.getLogger(ElasticsearchBaseQuery.class);

    private ElasticsearchPool pool;
    private EsConfig esConfig;

    public String[] indexNames;
    public String typeName;
    public int pageCount;
    public int beginIndex;

    public ElasticsearchBaseQuery(EsConfig esConfig, ElasticsearchPool elasticsearchPool, String[] indexNames, String typeName) {
        this.indexNames = indexNames;
        this.typeName = typeName;
        if (indexNames == null || indexNames.length == 0) {
            throw new IllegalArgumentException("索引名称不能为空");
        }
        this.pageCount = 5;
        this.beginIndex = 0;

        this.esConfig = esConfig;
        pool = elasticsearchPool;
        if (pool == null) pool = new ElasticsearchPool(esConfig);
    }

    public ElasticsearchBaseQuery(EsConfig esConfig, String[] indexNames, String typeName) {
        this(esConfig, null, indexNames, typeName);
    }


    public ElasticsearchPool getPool() {
        if (pool == null) pool = new ElasticsearchPool(esConfig);
        return pool;
    }

    /**
     * 基本查询
     *
     * @param queryBuilder
     * @return
     */
    public List<Map<String, Object>> query(QueryBuilder queryBuilder) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            SearchResponse searchResponse = transportClient.prepareSearch(indexNames)
                    .setSearchType(SearchType.DEFAULT)
                    .setTypes(typeName)
                    .setQuery(queryBuilder)
                    .setFrom(beginIndex)
                    .setSize(pageCount)
                    .get();
            log.info("查询总数:" + searchResponse.getHits().totalHits);
            List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
            for (SearchHit searchHit : searchResponse.getHits()) {
                datas.add(searchHit.getSource());
            }
            return datas;
        } catch (Exception e) {
            log.error(e);
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return null;
    }


    /**
     * scroll查询
     *
     * @param fieldName  字段名称
     * @param fieldValue 字段值
     * @param batchSize  批量大小
     * @param sortField  排序字段，一般为入库时间
     * @param sortOrder  sort排序 asc、desc
     * @return
     */
    public String getScrollId(String fieldName, Object fieldValue, int batchSize, String sortField, String sortOrder, Object sortValue) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        String scrollId = null;
        try {
            //查询获取到scrollId
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexNames)
                    .setTypes(typeName)
                    .setScroll("5m")
                    .setFrom(0)
                    .setSearchType(SearchType.DEFAULT)
                    .setSize(batchSize);
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            if (StringUtils.isNotEmpty(fieldName) && fieldValue != null && StringUtils.isNotEmpty(fieldValue.toString())) {
                if (fieldName.startsWith("!")) {
                    boolQueryBuilder.mustNot(QueryBuilders.termsQuery(fieldName, fieldValue.toString().substring(1).split(",")));
                } else {
                    boolQueryBuilder.must(QueryBuilders.termsQuery(fieldName, fieldValue.toString().split(",")));
                }
            }
            if (StringUtils.isNotEmpty(sortField)) {
                searchRequestBuilder.addSort(sortField, SortOrder.fromString(sortOrder));
            }
            if (sortValue != null) {
                if ("asc".equalsIgnoreCase(sortOrder)) {
                    boolQueryBuilder.must(QueryBuilders.rangeQuery(sortField).gt(sortValue));
                } else {
                    boolQueryBuilder.must(QueryBuilders.rangeQuery(sortField).lt(sortValue));
                }
            }
            if (boolQueryBuilder.hasClauses()) searchRequestBuilder.setQuery(boolQueryBuilder);

            SearchResponse searchResponse = searchRequestBuilder.get();
            scrollId = searchResponse.getScrollId();
            long totalHits = searchResponse.getHits().getTotalHits();
            log.info("scrollId:" + scrollId);
            log.info("totalHits:" + totalHits);
        } catch (Exception e) {
            log.error(e);
            e.printStackTrace();
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return scrollId;
    }

    /**
     * scroll查询
     *
     * @param scrollId 滚屏id
     * @return
     */
    public Map<String, Object> getScrollData(String scrollId) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();

        Map<String, Object> scrollMap = new HashMap<>();
        try {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scrollId(scrollId);
            searchScrollRequest.scroll("5m");
            SearchResponse response = transportClient.searchScroll(searchScrollRequest).get();
            scrollId = response.getScrollId();
            List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
            for (SearchHit searchHit : response.getHits()) {
                Map<String, Object> source = searchHit.getSource();
                source.put("_uid", searchHit.getId());
                source.put("id", searchHit.getId());
                datas.add(source);
            }
            scrollMap.put("scrollId", scrollId);
            scrollMap.put("data", datas);
        } catch (InterruptedException | ExecutionException e) {
            log.error(e);
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return scrollMap;
    }

    /**
     * scroll查询
     *
     * @param fieldName  字段名称
     * @param fieldValue 字段值
     * @param batchSize  批量大小
     * @param sortField  排序字段，一般为入库时间
     * @param sortOrder  sort排序 asc、desc
     * @return
     */
    public List<Map<String, Object>> scroll(String fieldName, Object fieldValue, int batchSize, String sortField, String sortOrder, Object sortValue) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        String scrollId = null;
        int hits = batchSize;
        List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
        try {
            //查询获取到scrollId
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexNames)
                    .setTypes(typeName)
                    .setScroll("5m")
                    .setFrom(0)
                    .setSearchType(SearchType.DEFAULT)
                    .setSize(batchSize);
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            if (fieldName != null && fieldValue != null) {
                boolQueryBuilder.must(QueryBuilders.termQuery(fieldName, fieldValue));
            }
            if (StringUtils.isNotEmpty(sortField)) {
                searchRequestBuilder.addSort(sortField, SortOrder.fromString(sortOrder));
            }
            if (sortValue != null) {
                if ("asc".equalsIgnoreCase(sortOrder)) {
                    boolQueryBuilder.must(QueryBuilders.rangeQuery(sortField).gt(sortValue));
                } else {
                    boolQueryBuilder.must(QueryBuilders.rangeQuery(sortField).lt(sortValue));
                }
            }
            if (boolQueryBuilder.hasClauses()) searchRequestBuilder.setQuery(boolQueryBuilder);

            SearchResponse searchResponse = searchRequestBuilder.get();
            scrollId = searchResponse.getScrollId();
            long totalHits = searchResponse.getHits().getTotalHits();
            log.info("scrollId:" + scrollId);
            log.info("totalHits:" + searchResponse.getHits().getTotalHits());
            //scroll查询 当获取的数据量小于批处理数量 则退出scroll查询
            while (totalHits > 0 && hits == batchSize) {
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scrollId(scrollId);
                searchScrollRequest.scroll("5m");
                SearchResponse response = transportClient.searchScroll(searchScrollRequest).get();
                hits = response.getHits().getHits().length;
                scrollId = response.getScrollId();
                for (SearchHit searchHit : response.getHits()) {
                    datas.add(searchHit.getSource());
                }
                log.info("hits:" + hits);
                log.info("scrollId:" + scrollId);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error(e);
            e.printStackTrace();
        } finally {
            //清除scrollId
            if (scrollId != null) {
                transportClient.prepareClearScroll().addScrollId(scrollId).get();
            }
            getPool().removeClient(elasticsearchClient);
        }
        return datas;
    }

    /**
     * 滚屏分页查询
     *
     * @param queryBuilder 查询条件
     * @param currentPage  当前页数
     * @param pageSize     一页的大小
     * @return
     */
    public List<Map<String, Object>> getPageByScroll(QueryBuilder queryBuilder, int currentPage, int pageSize) {
        int scrollPageSize = 1000;
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        String scrollId = null;
        if (currentPage == 0) {
            currentPage = 1;
        }
        List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
        try {
            //查询获取到scrollId
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexNames)
                    .setTypes(typeName)
                    .setScroll("5m")
                    .setFrom(0)
                    .setSearchType(SearchType.DEFAULT)
                    .setSize(scrollPageSize);
            if (queryBuilder != null) {
                searchRequestBuilder.setQuery(queryBuilder);
            }
            SearchResponse searchResponse = searchRequestBuilder.get();
            scrollId = searchResponse.getScrollId();
            //获取第currentPage页的数据
            int current_page = 1;

            while (true) {
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scrollId(scrollId);
                searchScrollRequest.scroll("5m");
                SearchResponse response = transportClient.searchScroll(searchScrollRequest).get();
                scrollId = response.getScrollId();
                if (current_page * scrollPageSize >= currentPage * pageSize) {
                    int beginIndex = (currentPage - 1) * pageSize - (current_page - 1) * scrollPageSize;
                    for (SearchHit searchHit : response.getHits()) {
                        datas.add(searchHit.getSource());
                    }
                    datas = datas.subList(beginIndex, beginIndex + pageSize);
                    break;
                }
                current_page++;
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error(e);
            e.printStackTrace();
        } finally {
            //清除scrollId
            if (scrollId != null) {
                transportClient.prepareClearScroll().addScrollId(scrollId).get();
            }
            getPool().removeClient(elasticsearchClient);
        }
        return datas;
    }

    /**
     * 根据事件类型 算出来需要的时间数据
     *
     * @param dateType         统计的时间类型：1、本日(day)，2、本周(week)，3、本月(month)，4、本年(year)，5、24小时(24hour),6、固定的年月(fixed)，7、nearmonth 近一个月
     * @param boolQueryBuilder boolQuery
     */
    public void setDateTypeQueryBuilder(String dateType, String dateField, BoolQueryBuilder boolQueryBuilder) {
        if (dateType == null || "".equals(dateType)) {
            return;
        }
        Calendar calendar = Calendar.getInstance(Locale.CHINA);
        calendar.setTime(new Date());
        //匹配时间
        switch (dateType.toLowerCase()) {
            case "nearday":
                calendar.add(Calendar.DAY_OF_MONTH, -1);
                calendar.add(Calendar.HOUR, 1);
                boolQueryBuilder.must(QueryBuilders.rangeQuery(dateField).gte(DateUtils.truncate(calendar, Calendar.HOUR).getTime().getTime()));
                break;
            case "day":
                calendar.setTime(DateUtils.truncate(calendar.getTime(), Calendar.DAY_OF_MONTH));
                boolQueryBuilder.must(QueryBuilders.rangeQuery(dateField).gte(calendar.getTime().getTime()));
                break;
            case "week":
                calendar.setFirstDayOfWeek(Calendar.MONDAY);
                int dayWeek = calendar.get(Calendar.DAY_OF_WEEK);// 获得当前日期是一个星期的第几天
                if (dayWeek == 1) {
                    dayWeek = 8;
                }
                calendar.add(Calendar.DATE, calendar.getFirstDayOfWeek() - dayWeek);
                calendar.setTime(DateUtils.truncate(calendar.getTime(), Calendar.DAY_OF_MONTH));
                boolQueryBuilder.must(QueryBuilders.rangeQuery(dateField).gte(calendar.getTime().getTime()));
                break;
            case "month":
                calendar.setTime(DateUtils.truncate(calendar.getTime(), Calendar.MONTH));
                boolQueryBuilder.must(QueryBuilders.rangeQuery(dateField).gte(calendar.getTime().getTime()));
                break;
            //近一个月
            case "nearmonth":
                calendar.add(Calendar.MONTH, -1);//减去一个月
                boolQueryBuilder.must(QueryBuilders.rangeQuery(dateField).gte(calendar.getTime().getTime()));
                break;
            case "year":
                calendar.setTime(DateUtils.truncate(calendar.getTime(), Calendar.YEAR));
                boolQueryBuilder.must(QueryBuilders.rangeQuery("event_time").gte(calendar.getTime().getTime()));
                break;
            default:
                //固定的时间 如yyyy、yyyy-MM、yyyy-MM-dd
                if (org.apache.http.client.utils.DateUtils.parseDate(dateType, new String[]{"yyyy"}) != null) {
                    setDateTypeQueryBuilder("year", dateField, boolQueryBuilder);
                }
                if (org.apache.http.client.utils.DateUtils.parseDate(dateType, new String[]{"yyyy-MM", "yyyyMM", "yyyy_MM"}) != null) {
                    setDateTypeQueryBuilder("month", dateField, boolQueryBuilder);
                }
                if (org.apache.http.client.utils.DateUtils.parseDate(dateType, new String[]{"yyyy-MM-dd", "yyyyMMdd", "yyyy_MM_dd"}) != null) {
                    setDateTypeQueryBuilder("day", dateField, boolQueryBuilder);
                }
                break;
        }
    }
}
