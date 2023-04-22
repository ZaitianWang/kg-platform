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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * 用于获取图谱和本体的统计特征与图特征
 * @author zaitian
 */
@RestController
@RequestMapping("/kgplatform/features")
public class KgFeaturesController {
    /**
     * 用五个不同的url来处理同时发送的不同数据项的请求，避免请求被stall
     * @param item 请求的统计指标
     * @return 指标的值
     */
    @GetMapping({"/getbasicstats-1", "/getbasicstats-2", "/getbasicstats-3", "/getbasicstats-4", "/getbasicstats-5"})
    public RetResult getBasicStats(String item) {
        JSONObject stats = new JSONObject();
        JSONObject allGraphs = TugraphUtil.listSubgraph();
        JSONArray graphNames = new JSONArray();
        graphNames.addAll(allGraphs.keySet());
        stats.put("item", item);
        int val;
        JSONObject labelCount;
        switch (item) {
            case "graph":
                val = graphNames.size();
                stats.put("val", val);
                break;
            case "entityLabel":
                labelCount = FeaturesUtil.getLabelCount(graphNames);
                val = labelCount.getIntValue("entityLabelCount");
                stats.put("val", val);
                break;
            case "relationshipLabel":
                labelCount = FeaturesUtil.getLabelCount(graphNames);
                val = labelCount.getIntValue("relationshipLabelCount");
                stats.put("val", val);
                break;
            case "entity":
                val = FeaturesUtil.getTotalEntityCount(graphNames);
                stats.put("val", val);
                break;
            case "relationship":
                val = FeaturesUtil.getTotalRelationshipCount(graphNames);
                stats.put("val", val);
                break;
        }
        return new RetResult(200, stats);
    }
    @GetMapping("/getstatsbygraph")
    public RetResult getStatsByGraph() throws InterruptedException, BrokenBarrierException {
        JSONObject stats = new JSONObject();
        JSONObject allGraphs = TugraphUtil.listSubgraph();
        JSONArray graphNames = new JSONArray();
        graphNames.addAll(allGraphs.keySet());
        stats.put("graphNames", graphNames);
        // 除获取图谱列表以外分三个线程获取三种信息，把响应时间从5秒压缩到2秒
        // 与getBasicStats的5个url一个效果
        final CyclicBarrier barrier = new CyclicBarrier(4);
        new Thread(() -> {
            JSONArray entityCount = FeaturesUtil.getEntityCountOfEachGraph(graphNames);
            stats.put("entityCount", entityCount);
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }).start();
        new Thread(() -> {
            JSONArray relationshipCount = FeaturesUtil.getRelationshipCountOfEachGraph(graphNames);
            stats.put("relationshipCount", relationshipCount);
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }).start();
        new Thread(() -> {
            JSONObject labelCount = FeaturesUtil.getLabelCountOfEachGraph(graphNames);
            JSONArray entityLabelCount = labelCount.getJSONArray("entityLabelCount");
            stats.put("entityLabelCount", entityLabelCount);
            JSONArray relationshipLabelCount = labelCount.getJSONArray("relationshipLabelCount");
            stats.put("relationshipLabelCount", relationshipLabelCount);
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }).start();
        barrier.await();
        return new RetResult(200, stats);
    }
    @GetMapping("/getinstancecount")
    public RetResult getInstanceCount(String graph) {
        JSONArray graphNames = new JSONArray();
        graphNames.add(graph);
        Integer entityCount = FeaturesUtil.getTotalEntityCount(graphNames);
        Integer relationshipCount = FeaturesUtil.getTotalRelationshipCount(graphNames);
        JSONObject instanceCount = new JSONObject();
        instanceCount.put("entityCount", entityCount);
        instanceCount.put("tripleCount", relationshipCount);
        return new RetResult(200, instanceCount);
    }
    @GetMapping("/getlabeldistribution")
    public RetResult getLabelDistribution(String graph) {
        JSONObject labels = TugraphUtil.listGraphLabel(graph);
        JSONArray edgeLabels = labels.getJSONArray("edge");
        JSONArray nodeLabels = labels.getJSONArray("vertex");
        JSONArray edgeDistribution = FeaturesUtil.getLabelDistribution(graph, "edge", edgeLabels);
        JSONArray nodeDistribution = FeaturesUtil.getLabelDistribution(graph, "node", nodeLabels);
        JSONObject distribution = new JSONObject();
        distribution.put("nodeDistribution", nodeDistribution);
        distribution.put("edgeDistribution", edgeDistribution);
        return new RetResult(200, distribution);
    }
    @GetMapping("/getnoderank")
    public RetResult getNodeRank(String graph) {
        JSONArray outRank = FeaturesUtil.getNodeRank(graph, "out");
        JSONArray inRank = FeaturesUtil.getNodeRank(graph, "in");
        JSONArray biRank = FeaturesUtil.getNodeRank(graph, "bi");
        JSONObject nodeRank = new JSONObject();
        nodeRank.put("outRank", outRank);
        nodeRank.put("inRank", inRank);
        nodeRank.put("biRank", biRank);
        return new RetResult(200, nodeRank);
    }
    @Component
    static class FeaturesUtil {
        private static String host;

        @Value(value = "${Tugraph-host}")
        public void setHost(String hostName) {
            host = hostName;
        }

        private static String getToken() {
            HttpHeaders headers = new HttpHeaders();
            headers.set(HttpHeaders.ACCEPT, "application/json; charset=UTF-8");
            headers.set(HttpHeaders.CONTENT_TYPE, "application/json");
            headers.set("server_version", "-1");
            JSONObject body = new JSONObject();
            body.put("user", "admin");
            body.put("password", "73@TuGraph");
            JSONObject res = RestTemplateUtils.post(host + "/login", headers, body, JSONObject.class, (Object) null).getBody();
            assert res != null;
            return res.getString("jwt");
        }

        private static HttpHeaders getHeader() {
            HttpHeaders headers = new HttpHeaders();
            headers.set(HttpHeaders.AUTHORIZATION, "Bearer " + getToken());
            headers.set(HttpHeaders.ACCEPT, "application/json; charset=UTF-8");
            headers.set(HttpHeaders.CONTENT_TYPE, "application/json");
            headers.set("server_version", "-1");
            return headers;
        }

        /**
         * 使用cypher语句查询TuGraph数据库
         * @param script 查询语句
         * @return 返回一个Object，对应Postman的body或TuGraph workbench的data部分
         */
        private static JSONObject cypher(String graph, String script) {
            JSONObject reqBody = new JSONObject();
            reqBody.put("graph", graph);
            reqBody.put("script", script);
            return RestTemplateUtils.post(host + "/cypher", getHeader(), reqBody, JSONObject.class).getBody();
        }

        private static JSONObject getLabelCount(JSONArray graphNames) {
            int entityLabelCount = 0, relationshipLabelCount = 0;
            for (int i = 0; i < graphNames.size(); i++) {
                JSONObject labels = TugraphUtil.listGraphLabel(graphNames.getString(i));
                entityLabelCount += labels.getJSONArray("vertex").size();
                relationshipLabelCount += labels.getJSONArray("edge").size();
            }
            JSONObject labelCount = new JSONObject();
            labelCount.put("entityLabelCount", entityLabelCount);
            labelCount.put("relationshipLabelCount", relationshipLabelCount);
            return labelCount;
        }

        private static int getTotalEntityCount(JSONArray graphNames) {
            int total = 0;
            for (int i = 0; i < graphNames.size(); i++) {
                int count = TugraphUtil.getGraphNodeRoughCount(graphNames.getString(i)).getIntValue("num_vertex");
                total += count;
            }
            return total;
        }

        private static int getTotalRelationshipCount(JSONArray graphNames) {
            int total = 0;
            for (int i = 0; i < graphNames.size(); i++) {
                int count = TugraphUtil.getGraphEdgeCount(graphNames.getString(i), null);
                total += count;
            }
            return total;
        }

        private static JSONObject getLabelCountOfEachGraph(JSONArray graphNames) {
            JSONObject graphLabelCounts = new JSONObject();
            JSONArray entityLabelCount = new JSONArray();
            JSONArray relationshipLabelCount = new JSONArray();
            for (int i = 0; i < graphNames.size(); i++) {
                JSONObject labels = TugraphUtil.listGraphLabel(graphNames.getString(i));
                entityLabelCount.add(labels.getJSONArray("vertex").size());
                relationshipLabelCount.add(labels.getJSONArray("edge").size());
            }
            graphLabelCounts.put("entityLabelCount", entityLabelCount);
            graphLabelCounts.put("relationshipLabelCount", relationshipLabelCount);
            return graphLabelCounts;
        }

        private static JSONArray getEntityCountOfEachGraph(JSONArray graphNames) {
            JSONArray graphEntityCount = new JSONArray();
            for (int i = 0; i < graphNames.size(); i++) {
                int count = TugraphUtil.getGraphNodeRoughCount(graphNames.getString(i)).getIntValue("num_vertex");
                graphEntityCount.add(count);
            }
            return graphEntityCount;
        }

        private static JSONArray getRelationshipCountOfEachGraph(JSONArray graphNames) {
            JSONArray graphRelationshipCount = new JSONArray();
            for (int i = 0; i < graphNames.size(); i++) {
                int count = TugraphUtil.getGraphEdgeCount(graphNames.getString(i), null);
                graphRelationshipCount.add(count);
            }
            return graphRelationshipCount;
        }

        private static JSONArray getLabelDistribution(String graph, String labelType, JSONArray labels) {
            JSONArray distribution = new JSONArray();
            for (int i = 0; i < labels.size(); i++) {
                JSONObject labelOccur = new JSONObject();
                String stmt;
                if (labelType.equals("node")) {
                    stmt = "MATCH (n:" + labels.getString(i) + ") RETURN COUNT(n)";
                } else  { // else if (labelType.equals("edge"))
                    stmt = "MATCH (n) -[r:" + labels.getString(i) + "]- (m) RETURN COUNT(r)";
                }
                JSONObject queryResult = cypher(graph, stmt);
                if (queryResult.getInteger("size") > 0) {
                    labelOccur.put(labels.getString(i),
                            queryResult.getJSONArray("result").getJSONArray(0).getInteger(0));
                } else {
                    labelOccur.put(labels.getString(i), 0);
                }
                distribution.add(labelOccur);
            }
            return distribution;
        }
        private static JSONArray getNodeRank(String graph, String direction) {
            String stmt;
            if (direction.equals("out")) {
                stmt = "MATCH (n) -[r]-> (m) " +
                        "RETURN id(n), n.name, COUNT(distinct r) AS degree ORDER BY degree DESC LIMIT 10";
            } else if (direction.equals("in")) {
                stmt = "MATCH (n) <-[r]- (m) " +
                        "RETURN id(n), n.name, COUNT(distinct r) AS degree ORDER BY degree DESC LIMIT 10";
            } else {
                stmt = "MATCH (n) -[r]- (m) " +
                        "RETURN id(n), n.name, COUNT(distinct r) AS degree ORDER BY degree DESC LIMIT 20";
            }
            JSONObject queryResult = cypher(graph, stmt);
            if (queryResult.getInteger("size") == 0) {
                return new JSONArray();
            } else {
                JSONArray nodeRank = new JSONArray();
                JSONArray result = queryResult.getJSONArray("result");
                for (int i = 0; i < result.size(); i++) {
                    JSONObject nodeDegree = new JSONObject();
                    nodeDegree.put("id", result.getJSONArray(i).get(0));
                    nodeDegree.put("name", result.getJSONArray(i).get(1));
                    nodeDegree.put("degree", result.getJSONArray(i).get(2));
                    nodeRank.add(nodeDegree);
                }
                return nodeRank;
            }
        }
    }
}
