package com.lovecws.mumu.flink.common.service.loophole;

import com.lovecws.mumu.flink.common.mapper.BaseServiceImpl;
import com.lovecws.mumu.flink.common.mapper.loophole.CnvdMapper;
import com.lovecws.mumu.flink.common.model.loophole.CnvdModel;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * @program: mumu-flink
 * @description: cnvd服务
 * @author: 甘亮
 * @create: 2019-06-06 13:07
 **/
public class CnvdService extends BaseServiceImpl<CnvdMapper, CnvdModel> {

    public List<CnvdModel> matchCnvdLoophole(List<CnvdModel> cnvdModels, String reflectProduct, String productVersion) {
        if (reflectProduct.contains("/") && StringUtils.isEmpty(productVersion)) {
            String[] productVersions = reflectProduct.split("/");
            reflectProduct = productVersions[0];
            if (productVersions.length >= 2) {
                productVersion = productVersions[1];
            }
        }
        if (productVersion == null || "".equals(productVersion)) productVersion = "0.0";
        productVersion = productVersion.replaceAll("\\D+", "");
        double product_version = 0.0;
        if (StringUtils.isNotEmpty(productVersion)) {
            product_version = Double.parseDouble(productVersion);
        }
        Set<CnvdModel> matchCnvdModes = new HashSet<>();
        String finalReflectProduct = reflectProduct;
        double finalProduct_version = product_version;
        cnvdModels.forEach(cnvdModel -> {
            String product = cnvdModel.getReflectProduct();
            if (product.contains(finalReflectProduct)) {
                double lower_version = 0.0, upper_version = 0.0;
                boolean lower_eq = false, upper_eq = false;
                for (String sub_product : product.split(",")) {
                    if (sub_product.contains("/")) {
                        int lower_index = 0;
                        if (sub_product.contains(">=")) {
                            lower_eq = true;
                            lower_index = sub_product.indexOf(">=") + 2;
                        } else {
                            lower_index = sub_product.indexOf(">") + 1;
                        }
                        String lower_version_str = sub_product.substring(lower_index);
                        if (lower_version_str.contains(",")) {
                            lower_version_str = lower_version_str.substring(0, lower_version_str.indexOf(","));
                        }
                        if ("".equals(lower_version_str)) lower_version_str = "0";
                        String lower_version_number = lower_version_str.replaceAll("\\D+", "");
                        if (StringUtils.isNotEmpty(lower_version_number))
                            lower_version = Double.parseDouble(lower_version_number);
                    }
                    if (sub_product.contains("<")) {
                        int upper_index = 0;
                        if (sub_product.contains("<=")) {
                            upper_eq = true;
                            upper_index = sub_product.indexOf("<=") + 2;
                        } else {
                            upper_index = sub_product.indexOf("<") + 1;
                        }
                        String upper_version_str = sub_product.substring(upper_index);
                        if (upper_version_str.contains(",")) {
                            upper_version_str = upper_version_str.substring(0, upper_version_str.indexOf(","));
                        }
                        if ("".equals(upper_version_str)) upper_version_str = "0";
                        String upper_version_number = upper_version_str.replaceAll("\\D+", "");
                        if (StringUtils.isNotEmpty(upper_version_number))
                            upper_version = Double.parseDouble(upper_version_number);
                    }
                    if (!sub_product.contains("<") && !sub_product.contains(">")) {
                        upper_eq = lower_eq = true;
                        String version_number = sub_product.replaceAll("\\D+", "");
                        if (StringUtils.isNotEmpty(version_number))
                            upper_version = lower_version = Double.parseDouble(version_number);
                    }

                    if (((upper_eq && finalProduct_version <= upper_version) || (!upper_eq && finalProduct_version < upper_version)) && ((lower_eq && finalProduct_version >= lower_version) || (!lower_eq && finalProduct_version > lower_version))) {
                        matchCnvdModes.add(cnvdModel);
                    }
                }
            }
        });
        List<CnvdModel> cnvdModelList = new ArrayList<>();
        matchCnvdModes.forEach(cnvdModel -> cnvdModelList.add(cnvdModel));
        return cnvdModelList;
    }

    /**
     * 根据受影响的产品获取漏洞列表
     *
     * @param reflectProduct 受影响的产品
     * @return
     */
    public List<CnvdModel> queryCnvdRefectProduct(String reflectProduct) {
        List<CnvdModel> cnvdModels = new ArrayList<>();
        if (StringUtils.isEmpty(reflectProduct)) return cnvdModels;

        for (String subReflectProduct : reflectProduct.split("/")) {
            QueryWrapper<CnvdModel> queryWrapper = new QueryWrapper<>();
            queryWrapper.like("reflect_product", subReflectProduct);
            cnvdModels.addAll(baseMapper.selectList(queryWrapper));
        }

        Map<String, CnvdModel> uniqueMap = new HashMap<>();
        cnvdModels.forEach(cnvdModel -> uniqueMap.putIfAbsent(cnvdModel.getCnvdNo(), cnvdModel));

        return new ArrayList<>(uniqueMap.values());
    }

    /**
     * 根据受影响的产品和版本号 计算匹配的漏洞
     *
     * @param reflectProduct 受影响的产品
     * @param productVersion 受影响的产品版本号
     * @return
     */
    public List<CnvdModel> queryCnvdRefectProduct(String reflectProduct, String productVersion) {
        List<CnvdModel> cnvdModels = new ArrayList<>();
        if (StringUtils.isEmpty(reflectProduct)) return cnvdModels;

        for (String subReflectProduct : reflectProduct.split("/")) {
            QueryWrapper<CnvdModel> queryWrapper = new QueryWrapper<>();
            queryWrapper.like("reflect_product", subReflectProduct);
            cnvdModels.addAll(matchCnvdLoophole(baseMapper.selectList(queryWrapper), subReflectProduct, productVersion));
        }


        Map<String, CnvdModel> uniqueMap = new HashMap<>();
        cnvdModels.forEach(cnvdModel -> uniqueMap.putIfAbsent(cnvdModel.getCnvdNo(), cnvdModel));

        return new ArrayList<>(uniqueMap.values());
    }


    /**
     * 根据受影响的产品和版本号 计算匹配的漏洞
     *
     * @param productVersions 受影响的产品
     * @return
     */
    public List<CnvdModel> queryCnvdRefectProduct(List<Map<String, String>> productVersions) {
        List<CnvdModel> cnvdModels = new ArrayList<>();
        if (productVersions == null || productVersions.size() == 0) return cnvdModels;

        //批量根据产品名称匹配cnvd漏洞库
        Set<String> products = new HashSet<String>();
        productVersions.forEach(map -> {
            products.add(map.getOrDefault("product", ""));
        });
        QueryWrapper<CnvdModel> queryWrapper = new QueryWrapper<>();
        queryWrapper.lambda().and(obj -> {
            Arrays.asList(products.toArray()).forEach(product -> {
                obj.or().like(CnvdModel::getReflectProduct, product);
            });
            return obj;
        });

        cnvdModels = baseMapper.selectList(queryWrapper);

        List<CnvdModel> cnvdModelList = new ArrayList<>();
        List<CnvdModel> finalCnvdModels = cnvdModels;
        productVersions.forEach(map -> {
            String product = map.getOrDefault("product", "");
            String version = map.getOrDefault("version", "");
            cnvdModelList.addAll(matchCnvdLoophole(finalCnvdModels, product, version));
        });

        return cnvdModelList;
    }
}
