package com.bupt.kgplatform.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bupt.kgplatform.common.RetResult;
import com.bupt.kgplatform.common.TugraphUtil;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
@RequestMapping("/kgplatform/kgvisualize")
public class KgVisualizeController {
    @GetMapping("/listsubgraph")
    public RetResult listSubgraph(){
        JSONObject graphList = TugraphUtil.listSubgraph();
        JSONArray result = new JSONArray();
        if(graphList!=null)
            graphList.forEach((key, value) -> {
                //名字和描述
                JSONObject graphInfo = new JSONObject();
                graphInfo.put("name", key);
                Object desc = ((HashMap<?, ?>) value).get("description");
                graphInfo.put("description", desc);
                //节点数
                graphInfo.put("vertexNum", TugraphUtil.getGraphNodeRoughCount(key).getIntValue("num_vertex"));
                result.add(graphInfo);
            });
        return new RetResult(200,result);
    }

    /**
     * 获取图标签列表 暂用于加载查询时的关系列表
     * @param graphName 图名
     * @return 图标签列表
     */
    @GetMapping("/listgraphlabel")
    public RetResult listGraphLabel(String graphName) {

        return new RetResult(200,TugraphUtil.listGraphLabel(graphName));
    }

    /** 把子图转换成echarts可视化的格式
     * @param IDNameMap 节点ID和name的映射
     * @param categoryArray 节点类型数组
     * @param nodes 子图节点列表
     * @param edges 子图边列表
     * @param nodeRes 转换后的节点列表
     * @param edgeRes 转换后的边列表
     */
    private static void prepareForVisualization(Map<Integer, String> IDNameMap, JSONArray categoryArray, JSONArray nodes, JSONArray edges, JSONArray nodeRes, JSONArray edgeRes) {
        for (int i = 0; i < nodes.size(); i++) {
            JSONObject info = nodes.getJSONObject(i);
            JSONObject node = new JSONObject();
            JSONObject property = info.getJSONObject("properties");
            JSONArray propertyList = new JSONArray();
            property.forEach((key, value) -> {
                JSONObject attr = new JSONObject();
                attr.put("key",key);
                attr.put("value", value);
                propertyList.add(attr);
            });
            node.put("rawName", property.getOrDefault("name", "unnamed"));
            node.put("name", info.get("vid") +"_"+ property.getOrDefault("name", "unnamed"));
            node.put("property",property);
            node.put("propertyList", propertyList);
            node.put("category", categoryArray.indexOf(info.getString("label")));
            node.put("label",info.getString("label"));
            node.put("vid",info.get("vid"));
            nodeRes.add(node);
        }
        for (int i = 0; i < edges.size(); i++) {
            JSONObject info = edges.getJSONObject(i);
            JSONObject edge = new JSONObject();
            if(info.getJSONObject("properties")!=null) {
                JSONObject property = info.getJSONObject("properties");
                JSONArray propertyList = new JSONArray();
                property.forEach((key, value) -> {
                    JSONObject attr = new JSONObject();
                    attr.put("key",key);
                    attr.put("value", value);
                    propertyList.add(attr);
                });
                edge.put("property", property);
                edge.put("propertyList", propertyList);
            } else {
                edge.put("property", new JSONObject());
                edge.put("propertyList", new JSONArray());
            }
            edge.put("label",info.get("label"));
            edge.put("uid",info.get("uid"));
            edge.put("source", IDNameMap.get(Integer.parseInt(info.get("source").toString())));
            edge.put("target", IDNameMap.get(Integer.parseInt(info.get("destination").toString())));
            edgeRes.add(edge);
        }
    }

    /** 获取图的schema信息，用于可视化
     * @param graphName 图名
     * @return 图的schema信息
     */
    @GetMapping("/getschemaforvis")
    public RetResult getSchemaForVis(String graphName) {
        JSONObject res = new JSONObject();
        JSONArray nodeRes = new JSONArray();
        JSONArray edgeRes = new JSONArray();
        JSONObject LabelList = TugraphUtil.listGraphLabel(graphName);
        JSONArray nodeLabelList = LabelList.getJSONArray("vertex");
        JSONArray edgeLabelList = LabelList.getJSONArray("edge");
        nodeLabelList.forEach((nodeLabel)->{
            String labelName = nodeLabel.toString();
            JSONObject labelInfo = JSONObject.parseObject(TugraphUtil.getVertexSchema(
                    graphName,labelName).getJSONArray(0).getString(0));
            JSONObject node = new JSONObject();
            node.put("name", labelName);
            JSONArray properties = labelInfo.getJSONArray("properties");
            JSONArray labelProps = new JSONArray();
            properties.forEach((property)->{
                JSONObject labelProp = new JSONObject();
                labelProp.put("name", ((JSONObject)property).getString("name"));
                labelProp.put("type", ((JSONObject)property).getString("type"));
                labelProps.add(labelProp);
            });
            node.put("properties", labelProps);
            node.put("labelType", "node");
            node.put("category", nodeLabelList.indexOf(nodeLabel));
            nodeRes.add(node);
        });
        edgeLabelList.forEach((edgeLabel)->{
            String labelName = edgeLabel.toString();
            JSONObject labelInfo = JSONObject.parseObject(TugraphUtil.getEdgeSchema(
                    graphName,labelName).getJSONArray(0).getString(0));
            JSONObject edge = new JSONObject();
            edge.put("labelName", labelName);
            JSONObject label = new JSONObject();
            label.put("show",true);
            label.put("formatter", labelName);
            edge.put("label", label);
//            edge.put("formatter", labelName);
            JSONArray properties = labelInfo.getJSONArray("properties");
            JSONArray labelProps = new JSONArray();
            properties.forEach((property)->{
                if (!((JSONObject)property).getString("type").equalsIgnoreCase("int8")
                        && !((JSONObject)property).getString("type").equalsIgnoreCase("int16")) {
                    JSONObject labelProp = new JSONObject();
                    labelProp.put("name", ((JSONObject)property).getString("name"));
                    labelProp.put("type", ((JSONObject)property).getString("type"));
                    labelProps.add(labelProp);
                }
            });
            edge.put("properties", labelProps);
            JSONArray constraints = labelInfo.getJSONArray("constraints");
            if (constraints.size()==0){
                edge.put("source", "unconstrained/any");
                edge.put("target", "unconstrained/any");
            } else {
                edge.put("source", constraints.getJSONArray(0).getString(0));
                edge.put("target", constraints.getJSONArray(0).getString(1));
            }
            edge.put("labelType", "edge");
//            edge.put("value", new Random().nextInt(10));
            edgeRes.add(edge);
        });
        res.put("nodes",nodeRes);
        res.put("edges",edgeRes);
        return new RetResult(200, res);
    }

    /**
     * 常规可视化接口
     * echarts可视化逻辑：节点有name、category属性
     * name就是显示的字段，category是用于图例筛选，
     * 只能是数字，需要用category数组的index
     * 边则是起点终点都用节点name表示
     * 程序逻辑：先获取所有节点，然后获取子图，然后把子图转换成echarts可视化的格式
     * @param graphName 图名
     * @return 图的可视化数据
     */
    @GetMapping("/getgraphdetail")
    public RetResult getGraphDetail(String graphName) {
        int nodeCount = TugraphUtil.getGraphNodeCount(graphName, "", "");
        if (nodeCount>2048) {
            return new RetResult(400, "节点数超过2048，无法可视化");
        }
        // 获取节点列表
        JSONObject nodeRaw = TugraphUtil.getGraphNodesAllWithCypher(graphName);
        JSONArray nodeResJsonArray = nodeRaw.getJSONArray("result");
        // 准备所有节点ID的集合
        JSONArray nodeIDArray = new JSONArray();
        // 准备节点的name和ID映射集，用于两者转换
        Map<Integer,String> IDNameMap = new HashMap<>();
        // 准备节点类型数组，用于图例
        JSONArray categoryArray = new JSONArray();
        JSONArray categoryRes = new JSONArray();
        for (Object node : nodeResJsonArray) {
            JSONObject nodeInfo = JSONObject.parseObject(JSONArray.parseArray(node.toString()).get(0).toString());
            // 汇总所有ID
            nodeIDArray.add(nodeInfo.get("identity"));
            JSONObject properties = (JSONObject) nodeInfo.get("properties");
            // 建立ID和name的映射
            IDNameMap.put(Integer.parseInt(nodeInfo.get("identity").toString()),nodeInfo.get("identity").toString()+"_"+properties.getOrDefault("name", "unnamed").toString());
            // 图例数组构建 categoryArray
            if(!categoryArray.contains(nodeInfo.get("label").toString())){
                categoryArray.add(nodeInfo.get("label").toString());
                JSONObject item = new JSONObject();
                item.put("name",nodeInfo.get("label").toString());
                categoryRes.add(item);
            }
        }
        // 通过节点ID集合获取子图
        JSONObject details = TugraphUtil.getSubGraphDetails(graphName,nodeIDArray);
        JSONArray nodes = details.getJSONArray("nodes");
        JSONArray edges = details.getJSONArray("relationships");
        // 把子图转换成echarts可视化的格式
        JSONArray nodeRes = new JSONArray();
        JSONArray edgeRes = new JSONArray();
        prepareForVisualization(IDNameMap, categoryArray, nodes, edges, nodeRes, edgeRes);
        // 返回结果
        JSONObject res = new JSONObject();
        res.put("categories",categoryRes);
        res.put("links",edgeRes);
        res.put("nodes",nodeRes);

        return new RetResult(200, res);
    }

    /**
     * 旧的按实体查询和按关系查询接口
     * 查询用包括单实体查询（返回该节点一跳的所有节点）  三元组查询（头尾空一个）
     * 参数只穿graphName、head是单实体
     * 参数全传是三元组
     * 注意getsubGraphNodesByHeadNode与getsubGraphNodesByTailNode中解析顺序数组的0和1  因为俩函数返回的是头尾节点对
     * @param graphName 图名
     * @param head 头节点
     * @param relation 关系
     * @param tail 尾节点
     * @return 图数据
     * @deprecated 不同标签的同名实体会漏；实体查询不够强大；关系查询太太方便
     * @see #queryGraph(String, String, String, int)
     */
    @GetMapping("/querygraphbytriple")
    public RetResult queryGraphByTriple(String graphName,String head,String relation,String tail) {
        // 准备所有节点ID的集合
        JSONArray nodeIDArray = new JSONArray();
        Map<Integer,String> IDNameMap = new HashMap<>();//节点的name和ID映射集，用于两者转换
        // 准备节点类型数组，用于图例
        JSONArray categoryArray = new JSONArray();
        JSONArray categoryRes = new JSONArray();

        //单实体查询 第二个循环记得去重
        if(relation==null){
            JSONArray noderaw1 = TugraphUtil.getsubGraphNodesByHeadNode(graphName,head,relation).getJSONArray("result");
            if(noderaw1.size()!=0) {
                //获取被查询实例本身id
                JSONObject selfinfo = JSONObject.parseObject(JSONArray.parseArray(noderaw1.get(0).toString()).get(0).toString());
                nodeIDArray.add(selfinfo.get("identity"));
                JSONObject selfproperties = (JSONObject) selfinfo.get("properties");
                IDNameMap.put(Integer.parseInt(selfinfo.get("identity").toString()), selfinfo.get("identity").toString() + "_" + selfproperties.get("name").toString());
                if (!categoryArray.contains(selfinfo.get("label").toString())) {
                    categoryArray.add(selfinfo.get("label").toString());
                    JSONObject item = new JSONObject();
                    item.put("name", selfinfo.get("label").toString());
                    categoryRes.add(item);
                }

                for (Object node : noderaw1) {
                    JSONObject nodeInfo = JSONObject.parseObject(JSONArray.parseArray(node.toString()).get(1).toString());
                    nodeIDArray.add(nodeInfo.get("identity"));
                    JSONObject properties = (JSONObject) nodeInfo.get("properties");
                    IDNameMap.put(Integer.parseInt(nodeInfo.get("identity").toString()), nodeInfo.get("identity").toString() + "_" + properties.get("name").toString());
                    //图例数组构建 categoryArray
                    if (!categoryArray.contains(nodeInfo.get("label").toString())) {
                        categoryArray.add(nodeInfo.get("label").toString());
                        JSONObject item = new JSONObject();
                        item.put("name", nodeInfo.get("label").toString());
                        categoryRes.add(item);
                    }
                }
            }

            JSONArray noderaw2 = TugraphUtil.getsubGraphNodesByTailNode(graphName,head,relation).getJSONArray("result");
            if(noderaw2.size()!=0) {
                if(noderaw1.size()==0){
                    JSONObject selfinfo = JSONObject.parseObject(JSONArray.parseArray(noderaw2.get(0).toString()).get(1).toString());
                    nodeIDArray.add(selfinfo.get("identity"));
                    JSONObject selfproperties = (JSONObject) selfinfo.get("properties");
                    IDNameMap.put(Integer.parseInt(selfinfo.get("identity").toString()), selfinfo.get("identity").toString() + "_" + selfproperties.get("name").toString());
                    if (!categoryArray.contains(selfinfo.get("label").toString())) {
                        categoryArray.add(selfinfo.get("label").toString());
                        JSONObject item = new JSONObject();
                        item.put("name", selfinfo.get("label").toString());
                        categoryRes.add(item);
                    }
                }
                for (Object node : noderaw2) {
                    JSONObject nodeInfo = JSONObject.parseObject(JSONArray.parseArray(node.toString()).get(0).toString());
                    if (!nodeIDArray.contains(nodeInfo.get("identity")))//防止重复
                        nodeIDArray.add(nodeInfo.get("identity"));
                    JSONObject properties = (JSONObject) nodeInfo.get("properties");
                    IDNameMap.put(Integer.parseInt(nodeInfo.get("identity").toString()), nodeInfo.get("identity").toString() + "_" + properties.get("name").toString());
                    //图例数组构建 categoryArray
                    if (!categoryArray.contains(nodeInfo.get("label").toString())) {
                        categoryArray.add(nodeInfo.get("label").toString());
                        JSONObject item = new JSONObject();
                        item.put("name", nodeInfo.get("label").toString());
                        categoryRes.add(item);
                    }
                }
            }
        }
        else {
            //头实体为空
            if(Objects.equals(head, "")) {
                JSONArray noderaw1 = TugraphUtil.getsubGraphNodesByTailNode(graphName,tail,relation).getJSONArray("result");
                //获取被查询实例本身id
                JSONObject selfinfo = JSONObject.parseObject(JSONArray.parseArray(noderaw1.get(0).toString()).get(1).toString());
                nodeIDArray.add(selfinfo.get("identity"));
                JSONObject selfproperties = (JSONObject) selfinfo.get("properties");
                IDNameMap.put(Integer.parseInt(selfinfo.get("identity").toString()),selfinfo.get("identity").toString()+"_"+selfproperties.get("name").toString());
                if(!categoryArray.contains(selfinfo.get("label").toString())){
                    categoryArray.add(selfinfo.get("label").toString());
                    JSONObject item = new JSONObject();
                    item.put("name",selfinfo.get("label").toString());
                    categoryRes.add(item);
                }

                for (Object node : noderaw1) {
                    JSONObject nodeInfo = JSONObject.parseObject(JSONArray.parseArray(node.toString()).get(0).toString());
                    nodeIDArray.add(nodeInfo.get("identity"));
                    JSONObject properties = (JSONObject) nodeInfo.get("properties");
                    IDNameMap.put(Integer.parseInt(nodeInfo.get("identity").toString()),nodeInfo.get("identity").toString()+"_"+properties.get("name").toString());
                    //图例数组构建 categoryArray
                    if(!categoryArray.contains(nodeInfo.get("label").toString())){
                        categoryArray.add(nodeInfo.get("label").toString());
                        JSONObject item = new JSONObject();
                        item.put("name",nodeInfo.get("label").toString());
                        categoryRes.add(item);
                    }
                }
            }
            else {
                JSONArray noderaw1 = TugraphUtil.getsubGraphNodesByHeadNode(graphName,head,relation).getJSONArray("result");
                //获取被查询实例本身id
                JSONObject selfinfo = JSONObject.parseObject(JSONArray.parseArray(noderaw1.get(0).toString()).get(0).toString());
                nodeIDArray.add(selfinfo.get("identity"));
                JSONObject selfproperties = (JSONObject) selfinfo.get("properties");
                IDNameMap.put(Integer.parseInt(selfinfo.get("identity").toString()),selfinfo.get("identity").toString()+"_"+selfproperties.get("name").toString());
                if(!categoryArray.contains(selfinfo.get("label").toString())){
                    categoryArray.add(selfinfo.get("label").toString());
                    JSONObject item = new JSONObject();
                    item.put("name",selfinfo.get("label").toString());
                    categoryRes.add(item);
                }

                for (Object node : noderaw1) {
                    JSONObject nodeInfo = JSONObject.parseObject(JSONArray.parseArray(node.toString()).get(1).toString());
                    nodeIDArray.add(nodeInfo.get("identity"));
                    JSONObject properties = (JSONObject) nodeInfo.get("properties");
                    IDNameMap.put(Integer.parseInt(nodeInfo.get("identity").toString()),nodeInfo.get("identity").toString()+"_"+properties.get("name").toString());
                    //图例数组构建 categoryArray
                    if(!categoryArray.contains(nodeInfo.get("label").toString())){
                        categoryArray.add(nodeInfo.get("label").toString());
                        JSONObject item = new JSONObject();
                        item.put("name",nodeInfo.get("label").toString());
                        categoryRes.add(item);
                    }
                }
            }
        }

        JSONObject details = TugraphUtil.getSubGraphDetails(graphName,nodeIDArray);
        JSONArray nodes = details.getJSONArray("nodes");
        JSONArray edges = details.getJSONArray("relationships");

        JSONArray nodeRes = new JSONArray();
        JSONArray edgeRes = new JSONArray();

        prepareForVisualization(IDNameMap, categoryArray, nodes, edges, nodeRes, edgeRes);

        JSONObject res = new JSONObject();
        res.put("categories",categoryRes);
        res.put("links",edgeRes);
        res.put("nodes",nodeRes);

        return new RetResult(200, res);
    }

    /** 新的查询图谱接口，支持多跳查询，可选首跳关系类型
     * 比旧接口多了一个maxHop参数，用于控制最大跳数，以提供更广的查询范围
     * 有机融合了按节点和按关系，由util类中的方法实现
     * 简化了起点/终点、去重的逻辑
     * @author zaitian
     * @param graphName 图谱名称
     * @param nodeName 节点名称，必填
     * @param relation 首跳关系名称，可选（为null或empty时查询所有关系）
     * @param maxHop 最大跳数，可选（范围1到5）
     * @return 图谱可视化数据
     */
    @GetMapping("/query-graph")
    public RetResult queryGraph(String graphName, String nodeName, String relation, int maxHop) {
        // 准备所有节点ID的集合
        Set<Integer> nodeIdSet = new HashSet<>();
        // 准备节点的name和ID映射集，用于两者转换
        Map<Integer, String> IDNameMap = new HashMap<>();
        // 准备节点类型数组，用于图例
        JSONArray categorySet = new JSONArray();
        JSONArray categoryRes = new JSONArray();
        // 获取节点自身和一跳邻居
        JSONArray nodesRaw = TugraphUtil.getNodeNeighbours(graphName, nodeName, relation, maxHop).getJSONArray("result");
        nodesRaw.forEach((nodes) -> ((JSONArray) nodes).forEach((node) -> {
            JSONObject nodeInfo = JSONObject.parseObject(node.toString());
            nodeIdSet.add(nodeInfo.getInteger("identity"));
            JSONObject properties = nodeInfo.getJSONObject("properties");
            IDNameMap.put(nodeInfo.getInteger("identity"), nodeInfo.getInteger("identity") + "_" + properties.getString("name"));
            if (!categorySet.contains(nodeInfo.getString("label"))) {
                categorySet.add(nodeInfo.getString("label"));
                JSONObject item = new JSONObject();
                item.put("name", nodeInfo.getString("label"));
                categoryRes.add(item);
            }
        }));
        JSONObject details = TugraphUtil.getSubGraphDetails(graphName, new JSONArray(Arrays.asList(nodeIdSet.toArray(new Integer[0]))));
        JSONArray nodes = details.getJSONArray("nodes");
        JSONArray edges = details.getJSONArray("relationships");
        JSONArray nodeRes = new JSONArray();
        JSONArray edgeRes = new JSONArray();
        prepareForVisualization(IDNameMap, new JSONArray(Arrays.asList(categorySet.toArray())), nodes, edges, nodeRes, edgeRes);
        JSONObject res = new JSONObject();
        res.put("categories", categoryRes);
        res.put("links", edgeRes);
        res.put("nodes", nodeRes);
        return new RetResult(200, res);
    }
}
