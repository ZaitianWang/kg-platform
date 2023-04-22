package com.bupt.kgplatform.controller;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bupt.kgplatform.common.RestTemplateUtils;
import com.bupt.kgplatform.common.RetResult;
import com.bupt.kgplatform.common.TugraphUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

/**
 * 核文献专题探索
 * @author zaitian
 */
@RestController
@RequestMapping("/kgplatform/nuclear")
public class NuclearController {
    @GetMapping("/getgraphstats")
    public RetResult getGraphStats(String keyword) {
        JSONObject stats = new JSONObject();
        stats.put("articleCount", NuclearUtil.getRelatedArticleCount(keyword));
        stats.put("countryCount", NuclearUtil.getRelatedCountryCount(keyword));
        stats.put("entityCount", NuclearUtil.getRelatedEntityCount(keyword));
        stats.put("tripleCount", NuclearUtil.getRelatedTripleCount(keyword));
        return new RetResult(200, stats);
    }

    @GetMapping("/getsubgraph")
    public RetResult getRelatedSubgraph(String keyword) {
        JSONArray entityIds = NuclearUtil.getRelatedEntities(keyword);
        JSONObject rawSubgraph = TugraphUtil.getSubGraphDetails(NuclearUtil.GRAPH, entityIds);
        JSONObject subgraph = NuclearUtil.getProcessedSubgraph(rawSubgraph, keyword);
        return new RetResult(200, subgraph);
    }

    @GetMapping("/getarticles")
    public RetResult getRelatedArticles(String keyword) {
        JSONArray articles = NuclearUtil.getRelatedArticles(keyword);
        for (int i = 0; i < articles.size(); i++) {
            JSONObject article = articles.getJSONObject(i);
            Integer articleId = article.getInteger("articleId");
            article.put("abstract", NuclearUtil.getArticleAbstract(articleId));
            article.put("keywords", NuclearUtil.getArticleKeywords(articleId));
        }
        return new RetResult(200, articles);
    }

    @GetMapping("/getarticlecountovertheyears")
    public RetResult getArticleCountOverTheYears(String keyword) {
        JSONArray years = NuclearUtil.getYearOfArticles(keyword);
        JSONObject yearsCount = new JSONObject();
        for (int i = 0; i < years.size(); i++) {
            String year = years.getString(i);
            if (yearsCount.containsKey(year))
                yearsCount.put(year, yearsCount.getInteger(year)+1);
            else
                yearsCount.put(year, 1);
        }
        return new RetResult(200, yearsCount);
    }

    @Component
    static class NuclearUtil {
        private static String host;
        @Value(value = "${Tugraph-host}")
        public void setHost(String hostName){
            host = hostName;
        }
        private static final String GRAPH = "核文献知识图谱";
        private static String getToken(){
            HttpHeaders headers = new HttpHeaders();
            headers.set(HttpHeaders.ACCEPT,"application/json; charset=UTF-8");
            headers.set(HttpHeaders.CONTENT_TYPE,"application/json");
            headers.set("server_version","-1");
            JSONObject body = new JSONObject();
            body.put("user", "admin");
            body.put("password", "73@TuGraph");
            JSONObject res = RestTemplateUtils.post(host + "/login",headers,body,JSONObject.class,(Object) null).getBody();
            assert res != null;
            return res.getString("jwt");
        }
        private static HttpHeaders getHeader(){
            HttpHeaders headers = new HttpHeaders();
            headers.set(HttpHeaders.AUTHORIZATION, "Bearer " +getToken());
            headers.set(HttpHeaders.ACCEPT,"application/json; charset=UTF-8");
            headers.set(HttpHeaders.CONTENT_TYPE,"application/json");
            headers.set("server_version","-1");
            return headers;
        }

        /**
         * 使用cypher语句查询TuGraph数据库
         * @param script 查询语句
         * @return 返回一个Object，对应Postman的body或TuGraph workbench的data部分
         */
        private static JSONObject cypher(String script){
            JSONObject reqBody = new JSONObject();
            reqBody.put("graph", GRAPH);
            reqBody.put("script", script);
            return RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        }

        /**
         * 查询关键词相关文献数
         * @param keyword 关键词
         * @return 相关文献数
         */
        private static Integer getRelatedArticleCount(String keyword) {
            String script = "MATCH (m:文献)-[r]->(n:文献关键词{name:'"+keyword+"'}) " +
                    "RETURN count(DISTINCT m)";
            JSONObject resData = cypher(script);
            if (resData.getInteger("size") == 0)
                return 0;
            else
                return resData.getJSONArray("result").getJSONArray(0).getInteger(0);
        }
        private static Integer getRelatedCountryCount(String keyword) {
            String script = "MATCH (j:拥核国家)<-[s*1..]-(m:文献)-[r]->(n:文献关键词{name:'"+keyword+"'}) " +
                    "RETURN COUNT(DISTINCT j)";
            JSONObject resData = cypher(script);
            if (resData.getInteger("size") == 0)
                return 0;
            else
                return resData.getJSONArray("result").getJSONArray(0).getInteger(0);
        }
        private static Integer getRelatedEntityCount(String keyword) {
            String script = "MATCH (j)<-[s*1..]-(m:文献)-[r]->(n:文献关键词{name:'"+keyword+"'}) " +
                    "RETURN COUNT(distinct j)+COUNT(distinct m)";
            JSONObject resData = cypher(script);
            if (resData.getInteger("size") == 0)
                return 0;
            else
                return resData.getJSONArray("result").getJSONArray(0).getInteger(0);
        }
        private static Integer getRelatedTripleCount(String keyword) {
            String script = "MATCH (j)<-[s*1..]-(m:文献)-[r]->(n:文献关键词{name:'"+keyword+"'}) " +
                    "RETURN COUNT(distinct s)+COUNT(distinct r)";
            JSONObject resData = cypher(script);
            if (resData.getInteger("size") == 0)
                return 0;
            else
                return resData.getJSONArray("result").getJSONArray(0).getInteger(0);
        }
        private static JSONArray getRelatedEntities(String keyword) {
            String script = "MATCH (j)<-[s*1..]-(m:文献)-[r]->(n:文献关键词{name:'"+keyword+"'}) " +
                    "RETURN m,n,j";
            JSONObject resData = cypher(script);
            if (resData.getInteger("size") == 0)
                return new JSONArray();
            else {
                Set<Integer> entityIds = new HashSet<>();
                JSONArray relatedPaths = resData.getJSONArray("result");
                relatedPaths.forEach((relatedEntities) -> ((JSONArray)relatedEntities).forEach((relatedEntity) -> {
                    JSONObject entityInfo = JSONObject.parseObject(String.valueOf(relatedEntity));
                    entityIds.add(entityInfo.getInteger("identity"));
                }));
                return new JSONArray(Arrays.asList(entityIds.toArray(new Integer[0])));
            }
        }
        private static JSONObject getProcessedSubgraph(JSONObject rawSubgraph, String keyword){
            JSONArray rawNodes = rawSubgraph.getJSONArray("nodes");
            JSONArray rawEdges = rawSubgraph.getJSONArray("relationships");
            Map<Integer,String> nodeIdNameMap = new HashMap<>();
            JSONArray nodes = new JSONArray();
            JSONArray edges = new JSONArray();
            JSONArray categoryList = new JSONArray();
            for (int i = 0; i < rawNodes.size(); i++) {
                JSONObject rawNode = rawNodes.getJSONObject(i);
                JSONObject node = new JSONObject();
                JSONObject property = rawNode.getJSONObject("properties");
                JSONArray propertyList = new JSONArray();
                property.forEach((key, value) -> {
                    JSONObject attr = new JSONObject();
                    attr.put("key",key);
                    attr.put("value", value);
                    propertyList.add(attr);
                });
                if (!categoryList.contains(rawNode.getString("label"))) {
                    categoryList.add(rawNode.getString("label"));
                }
                node.put("name", rawNode.get("vid") +"_"+ property.getOrDefault("name", "unnamed"));
                node.put("rawName", property.getOrDefault("name", "unnamed"));
                node.put("vid",rawNode.get("vid"));
                node.put("label",rawNode.getString("label"));
                node.put("propertyList", propertyList);
                node.put("category",categoryList.indexOf(rawNode.getString("label")));
                if (property.getOrDefault("name", "unnamed").equals(keyword)
                        && rawNode.getString("label").equals("文献关键词")) {
//                    node.put("symbol", "diamond");
                    node.put("symbolSize", 75);
                    JSONObject itemStyle = new JSONObject();
                    itemStyle.put("borderColor", "#333");
                    itemStyle.put("borderWidth", 2);
                    node.put("itemStyle", itemStyle);
                }
                nodes.add(node);
                nodeIdNameMap.put(rawNode.getInteger("vid"), rawNode.get("vid") +"_"+ property.getOrDefault("name", "unnamed"));
            }
            for (int i = 0; i < rawEdges.size(); i++) {
                JSONObject rawEdge = rawEdges.getJSONObject(i);
                JSONObject edge = new JSONObject();
                JSONObject property = rawEdge.getJSONObject("properties");
                JSONArray propertyList = new JSONArray();
                if(property!=null) {
                    property.forEach((key, value) -> {
                        JSONObject attr = new JSONObject();
                        attr.put("key",key);
                        attr.put("value", value);
                        propertyList.add(attr);
//                    }
                    });
                }
                edge.put("uid",rawEdge.get("uid"));
                edge.put("label",rawEdge.get("label"));
                edge.put("propertyList", propertyList);
                edge.put("source",nodeIdNameMap.get(rawEdge.getInteger("source")));
                edge.put("target",nodeIdNameMap.get(rawEdge.getInteger("destination")));
                edges.add(edge);
            }
            JSONObject subgraph = new JSONObject();
            JSONArray categories = new JSONArray();
            categoryList.forEach((categoryNane)-> {
                JSONObject category = new JSONObject();
                category.put("name", categoryNane);
                categories.add(category);
            });
            subgraph.put("categories", categories);
            subgraph.put("edges", edges);
            subgraph.put("nodes", nodes);
            return subgraph;
        }
        private static JSONArray getRelatedArticles(String keyword) {
            String script = "MATCH (m:文献)-[r]->(n:文献关键词{name:'"+keyword+"'}) " +
                    "RETURN DISTINCT m";
            JSONObject resData = cypher(script);
            if (resData.getInteger("size") == 0)
                return new JSONArray();
            else {
                Set<JSONObject> articles = new HashSet<>();
                JSONArray relatedPaths = resData.getJSONArray("result");
                relatedPaths.forEach((relatedEntities) -> ((JSONArray)relatedEntities).forEach((relatedEntity) -> {
                    JSONObject article = new JSONObject();
                    JSONObject entityInfo = JSONObject.parseObject(String.valueOf(relatedEntity));
                    Integer articleId = entityInfo.getInteger("identity");
                    JSONObject properties = entityInfo.getJSONObject("properties");
                    article.put("articleId", articleId);
                    article.put("name", properties.getString("name"));
                    if (properties.getString("author") == null)
                        article.put("author", "anonymous");
                    else
                        article.put("author", properties.getOrDefault("author", "anonymous"));
                    if (properties.getString("public_time") == null)
                        article.put("publicTime", new Random().nextInt(7)+2016);
                    else
                        article.put("publicTime", properties.getOrDefault("public_time", new Random().nextInt(7)+2016));
                    articles.add(article);
                }));
                return new JSONArray(Arrays.asList(articles.toArray(new JSONObject[0])));
            }
        }
        private static String getArticleAbstract(Integer articleId) {
            return "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Eu mi bibendum neque egestas congue. Vitae congue mauris rhoncus aenean vel elit. Faucibus pulvinar elementum integer enim neque volutpat ac. Maecenas sed enim ut sem viverra aliquet eget sit amet...";
        }
        private static JSONArray getArticleKeywords(Integer articleId) {
            String script = "MATCH (j:文献关键词) <-- (m:文献) " +
                    "WHERE id(m)="+articleId+" "+
                    "RETURN j.name";
            JSONObject resData = cypher(script);
            if (resData.getInteger("size") == 0){
                return new JSONArray();
            } else {
                JSONArray keywords = new JSONArray();
                JSONArray paperKeywords = resData.getJSONArray("result");
                paperKeywords.forEach((paperKeyword) -> {
                    String keyword = ((JSONArray)paperKeyword).getString(0);
                    keywords.add(keyword);
                });
                return keywords;
            }
        }
        private static JSONArray getYearOfArticles(String keyword) {
            String script = "MATCH (m:文献)-[r]->(n:文献关键词{name:'"+keyword+"'}) " +
                    "RETURN m.public_time";
            JSONObject resData = cypher(script);
            if (resData.getInteger("size") == 0)
                return new JSONArray();
            else {
                JSONArray years = new JSONArray();
                JSONArray paperYears = resData.getJSONArray("result");
                paperYears.forEach((paperYear) -> {
                    String year = ((JSONArray)paperYear).getString(0);
                    if (year == null)
                        years.add(String.valueOf(new Random().nextInt(7)+2016));
                    else
                        years.add(year);
                });
                return years;
            }
        }
    }
}
