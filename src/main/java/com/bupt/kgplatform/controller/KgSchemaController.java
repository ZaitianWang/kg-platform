package com.bupt.kgplatform.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bupt.kgplatform.common.RetResult;
import com.bupt.kgplatform.common.TugraphUtil;


import org.apache.commons.io.FilenameUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/kgplatform/kgschema")
public class KgSchemaController {

    @GetMapping("/listsubgraph")
    public RetResult listSubgraph() {
        JSONObject graphList = TugraphUtil.listSubgraph();
        JSONArray result = new JSONArray();
        if (graphList != null)
            graphList.forEach((k, v) -> {
                JSONObject graphInfo = new JSONObject();
                graphInfo.put("name", k);
                JSONObject info = graphList.getJSONObject(k);
                graphInfo.put("description", info.get("description"));
                //标签数
                JSONObject labelInfo = TugraphUtil.listGraphLabel(k);
                JSONArray vertex = labelInfo.getJSONArray("vertex");
                JSONArray edge = labelInfo.getJSONArray("edge");
                graphInfo.put("labelNum", vertex.size() + edge.size());
                graphInfo.put("entityNum", vertex.size());
                graphInfo.put("relationNum", edge.size());
                result.add(graphInfo);
            });

        return new RetResult(200, result);
    }

    @PostMapping("/addsubgraph")
    public RetResult addSubgraph(@RequestParam() String name, @RequestParam() String description) {
        try {
            HttpStatus httpStatus = TugraphUtil.addSubGraph(name, description);
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "添加成功");
        } catch (Exception e) {
            return new RetResult(400, "图谱已存在，请检查");
        }
        return new RetResult(400, "图谱已存在，请检查");
    }

    @DeleteMapping("/deletesubgraph")
    public RetResult deleteSubgraph(@RequestParam() String name) {
        HttpStatus httpStatus = TugraphUtil.deleteSubGraph(name);
        if (httpStatus.is2xxSuccessful())
            return new RetResult(200, "删除成功");
        else
            return new RetResult(400, "要删除图谱不存在，请检查");
    }

    @PostMapping("/addsinglenodelabel")
    public RetResult addSingleNodeLabel(@RequestParam() String graphName, @RequestParam() String nodeLabel, @RequestParam() String properties) {
        try {
            JSONArray fields = (JSONArray) JSONArray.parse(properties);
            HttpStatus httpStatus = TugraphUtil.addNodeLabel(graphName, nodeLabel, fields, "name");
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "节点标签添加成功");
        } catch (Exception e) {
            return new RetResult(400, "节点标签添加失败，请检查");
        }
        return new RetResult(400, "节点标签添加失败，请检查");
    }

    /**
     * add single node方法的request param可能报错，貌似前端必须qs.stringify才能用
     * 既然是post，这里还是用body了（add single只要stringify也能用）
     *
     * @param requestBody body
     * @return 200
     * @author zaitian
     */
    @PostMapping("/addonenodelabel")
    public RetResult addOneNodeLabel(@RequestBody String requestBody) {
        JSONObject body = JSONObject.parseObject(requestBody);
        String graphName = body.getString("graphName");
        String nodeLabel = body.getString("nodeLabel");
        JSONArray properties = body.getJSONArray("properties");
        try {
            HttpStatus httpStatus = TugraphUtil.addNodeLabel(graphName, nodeLabel, properties, "name");
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "节点标签添加成功");
        } catch (Exception e) {
            return new RetResult(400, e.getMessage());
        }
        return new RetResult(400, "节点标签添加失败，请检查");
    }

    @PostMapping("/addnodelabel")
    public RetResult addNodeLabel(@RequestParam() String graphName, @RequestParam() String labelsInfo) {
        JSONArray info = JSON.parseArray(labelsInfo);

        try {
            for (int i = 0; i < info.size(); i++) {
                JSONObject nodesInfo = info.getJSONObject(i);
                // String id = nodesInfo.getString("id");
                String label = nodesInfo.getString("label");
                String state = nodesInfo.getString("state");

                if (state.equalsIgnoreCase("changed")) {
                    // 对于发生修改的节点标签，先删除原标签再重新添加
                    TugraphUtil.deleteNodeLabel(graphName, label);
                } else if (state.equalsIgnoreCase("old")) {
                    // 对于未修改的旧有节点标签，不做添加处理
                    continue;
                }

                // 添加新节点标签和修改后的节点标签
                JSONArray fields = nodesInfo.getJSONArray("properties");
                // JSONObject idProperties = new JSONObject();
                // idProperties.put("name", "name");
                // idProperties.put("type", "string");
                // fields.add(idProperties);
//                System.out.println(label);

                HttpStatus httpStatus = TugraphUtil.addNodeLabel(graphName, label, fields, "name");
//                System.out.println(httpStatus);
                if (httpStatus.is4xxClientError())
                    return new RetResult(400, "节点标签添加失败，请检查");
            }
        } catch (Exception e) {
            return new RetResult(400, "节点标签添加失败，请检查");
        }
        return new RetResult(200, "所有节点标签添加成功");
    }

    /**
     * @param requestBody body
     * @return 200
     * @author zaitian
     */
    @PutMapping("/updatenodelabel")
    public RetResult updateNodeLabel(@RequestBody String requestBody) {
        JSONObject body = JSONObject.parseObject(requestBody);
        String graphName = body.getString("graphName");
        String nodeLabel = body.getString("nodeLabel");
        JSONArray properties = body.getJSONArray("property");
        try {
            HttpStatus deleteStatus = TugraphUtil.deleteNodeLabel(graphName, nodeLabel);
            if (deleteStatus.is2xxSuccessful()) {
                HttpStatus addStatus = TugraphUtil.addNodeLabel(graphName, nodeLabel, properties, "name");
                if (addStatus.is2xxSuccessful()) {
                    return new RetResult(200, "node label updated");
                }
            }
        } catch (Exception e) {
            return new RetResult(400, e.getMessage());
        }
        return new RetResult(400, "update failed");
    }

    @GetMapping("/listsinglenodelabel")
    public RetResult listSingleNodeLabel(@RequestParam() String graphName, @RequestParam() String nodeLabel) {
//        System.out.println(graphName);
        JSONObject nodeInfo = TugraphUtil.listNodeLabelInfo(graphName, nodeLabel);
        JSONObject nodeResult = new JSONObject();
        nodeResult.put("label", nodeLabel);
        JSONArray properties = new JSONArray();
        if (nodeInfo != null)
            for (Map.Entry<String, Object> entry : nodeInfo.entrySet()) {
                String key = entry.getKey();
                HashMap value = (HashMap) entry.getValue();
                String type = String.valueOf(value.get("type"));
                JSONObject property = new JSONObject();
                property.put("name", key);
                property.put("type", type.toLowerCase());
                properties.add(property);
            }

        nodeResult.put("properties", properties);

        return new RetResult(200, nodeResult);
    }

    @GetMapping("/listnodelabel")
    public RetResult listNodeLabel(@RequestParam() String graphName) {
        JSONObject LabelList = TugraphUtil.listGraphLabel(graphName);
        ArrayList<String> nodeLabelList = (ArrayList<String>) LabelList.get("vertex");
        JSONArray result = new JSONArray();
        for (int i = 0; i < nodeLabelList.size(); i++) {
            String nodeName = nodeLabelList.get(i);
            if (nodeName.startsWith("hyperRelationship_"))
                continue;
            JSONObject nodeInfo = TugraphUtil.listNodeLabelInfo(graphName, nodeName);
            JSONObject nodeResult = new JSONObject();
            nodeResult.put("id", i + 1);
            nodeResult.put("label", nodeName);
            nodeResult.put("type", "leaf");
            nodeResult.put("state", "old");
            ArrayList empty = new ArrayList();
            nodeResult.put("children", empty);
            JSONArray properties = new JSONArray();

            if (nodeInfo != null)
                for (Map.Entry<String, Object> entry : nodeInfo.entrySet()) {
                    String key = entry.getKey();
                    HashMap value = (HashMap) entry.getValue();
                    String type = String.valueOf(value.get("type"));
                    JSONObject property = new JSONObject();
                    property.put("name", key);
                    property.put("type", type.toLowerCase());
                    // if (type.equalsIgnoreCase("string"))
                    //     property.put("type", "文本");                  
                    // else if (type.equalsIgnoreCase("date"))
                    //     property.put("type", "时间");
                    // else if (type.equalsIgnoreCase("int32"))
                    //     property.put("type", "整型");
                    // else if (type.equalsIgnoreCase("double"))
                    //     property.put("type", "双精度浮点型");

                    properties.add(property);
                }

            nodeResult.put("properties", properties);
            // System.out.println(nodeResult);
            result.add(nodeResult);
        }

        return new RetResult(200, result);
    }

    @GetMapping("/listnodelabelname")
    public RetResult listNodeLabelName(@RequestParam() String graphName) {
        JSONObject LabelList = TugraphUtil.listGraphLabel(graphName);
        ArrayList<String> nodeLabelList = (ArrayList<String>) LabelList.get("vertex");
        JSONArray result = new JSONArray();
        result.add(nodeLabelList);
        return new RetResult(200, result);
    }

    @PostMapping("/deletenodelabel")
    public RetResult deleteNodeLabel(@RequestParam() String graphName, @RequestParam() String nodeName) {
        try {
            HttpStatus httpStatus = TugraphUtil.deleteNodeLabel(graphName, nodeName);
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "节点标签删除成功");
        } catch (Exception e) {
            return new RetResult(400, "节点标签删除失败，请检查");
        }
        return new RetResult(400, "节点标签删除失败，请检查");
    }

    @PostMapping("/addedgelabel")
    public RetResult addEdgeLabel(@RequestParam() String graphName, @RequestParam() String edgeLabel, @RequestParam() String startNode,
                                  @RequestParam() String endNode, @RequestParam() String properties) {
        try {
            JSONArray fields = (JSONArray) JSONArray.parse(properties);
            // properties.getJSONArray("properties");
            // 添加头尾节点标签作为额外的可选属性字段
            JSONObject startNodeObject = new JSONObject();
            startNodeObject.put("name", startNode);
            startNodeObject.put("type", "int8");
            startNodeObject.put("optional", true);
            JSONObject endNodeObject = new JSONObject();
            endNodeObject.put("name", endNode);
            endNodeObject.put("type", "int16");
            endNodeObject.put("optional", true);
            fields.add(startNodeObject);
            fields.add(endNodeObject);

            HttpStatus httpStatus = TugraphUtil.addEdgeLabel(graphName, edgeLabel, fields, startNode, endNode);
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "边标签添加成功");
        } catch (Exception e) {
            return new RetResult(400, "边标签添加失败，请检查");
        }
        return new RetResult(400, "边标签添加失败，请检查");
    }

    /**
     * @param requestBody body
     * @return 200
     * @author zaitian
     */
    @PutMapping("/updateedgelabel")
    public RetResult updateEdgeLabel(@RequestBody String requestBody) {
        JSONObject body = JSONObject.parseObject(requestBody);
        String graphName = body.getString("graphName");
        String nodeLabel = body.getString("edgeLabel");
        JSONArray properties = body.getJSONArray("property");
        String src = body.getString("src");
        String dst = body.getString("dst");
        try {
            HttpStatus deleteStatus = TugraphUtil.deleteEdgeLabel(graphName, nodeLabel);
            if (deleteStatus.is2xxSuccessful()) {
                HttpStatus addStatus = TugraphUtil.addEdgeLabel(graphName, nodeLabel, properties, src, dst);
                if (addStatus.is2xxSuccessful()) {
                    return new RetResult(200, "edge label updated");
                }
            }
        } catch (Exception e) {
            return new RetResult(400, e.getMessage());
        }
        return new RetResult(400, "update failed");
    }

    @PostMapping("/addhyperrelationship")
    public RetResult addHyperRelationship(@RequestParam() String graphName, @RequestParam() String edgeLabel, @RequestParam() String startNode,
                                          @RequestParam() String endNode, @RequestParam() String properties) {
        try {
            // 创建超关系节点
            String hyperLabel = "hyperRelationship_" + edgeLabel;
            JSONArray hyperFields = new JSONArray();

            JSONObject hyperName = new JSONObject();
            hyperName.put("name", edgeLabel);
            hyperName.put("type", "string");
            hyperFields.add(hyperName);

            JSONObject head = new JSONObject();
            head.put("name", startNode);
            head.put("type", "int8");
            hyperFields.add(head);

            JSONObject tail = new JSONObject();
            tail.put("name", endNode);
            tail.put("type", "int16");
            hyperFields.add(tail);

            JSONArray keyValues = (JSONArray) JSONArray.parse(properties);
            for (Object objectPair : keyValues) {
                JSONObject jsonObjectPair = (JSONObject) JSON.toJSON(objectPair);
                JSONObject value = new JSONObject();
                value.put("name", jsonObjectPair.getString("key") + "_" + jsonObjectPair.getString("value"));
                value.put("type", "int64");
                hyperFields.add(value);
            }

            HttpStatus httpStatus = TugraphUtil.addNodeLabel(graphName, hyperLabel, hyperFields, edgeLabel);
            if (httpStatus.is4xxClientError())
                return new RetResult(400, "超关系节点标签添加失败，请检查");

            // 创建头实体与超关系之间的边
            String headHyperLabel = startNode + "_" + edgeLabel;
            JSONArray headHyperFields = new JSONArray();

            JSONObject headHyperName = new JSONObject();
            headHyperName.put("name", headHyperLabel);
            headHyperName.put("type", "string");
            headHyperFields.add(headHyperName);

            httpStatus = TugraphUtil.addEdgeLabel(graphName, headHyperLabel, headHyperFields, startNode, hyperLabel);
            if (httpStatus.is4xxClientError())
                return new RetResult(400, "头实体-超关系边标签添加失败，请检查");


            // 创建超关系与尾实体之间的边
            String hyperTailLabel = edgeLabel + "_" + endNode;
            JSONArray hyperTailFields = new JSONArray();

            JSONObject hyperTailName = new JSONObject();
            hyperTailName.put("name", hyperTailLabel);
            hyperTailName.put("type", "string");
            hyperTailFields.add(hyperTailName);

            httpStatus = TugraphUtil.addEdgeLabel(graphName, hyperTailLabel, hyperTailFields, hyperLabel, endNode);
            if (httpStatus.is4xxClientError())
                return new RetResult(400, "超关系-尾实体边标签添加失败，请检查");

            // 创建超关系与属性实体之间的边
            for (Object objectPair : keyValues) {
                JSONObject jsonObjectPair = (JSONObject) JSON.toJSON(objectPair);
                String key = jsonObjectPair.getString("key");
                String value = jsonObjectPair.getString("value");

                String hyperAttributeLabel = edgeLabel + "_" + value + "_" + key;
                JSONArray hyperAttributeFields = new JSONArray();

                JSONObject hyperAttributeName = new JSONObject();
                hyperAttributeName.put("name", key);
                hyperAttributeName.put("type", "string");
                hyperAttributeFields.add(hyperAttributeName);

                httpStatus = TugraphUtil.addEdgeLabel(graphName, hyperAttributeLabel, hyperAttributeFields, hyperLabel, value);
                if (httpStatus.is4xxClientError())
                    return new RetResult(400, "超关系-属性边标签添加失败，请检查");
            }

        } catch (Exception e) {
            return new RetResult(400, "超关系标签添加失败，请检查");
        }
        return new RetResult(200, "超关系标签添加成功");
    }


    @GetMapping("/listsinglehyperrelationship")
    public RetResult listSingleHyperRelationship(@RequestParam() String graphName, @RequestParam() String hyperName) {
        String hyperLabel = "hyperRelationship_" + hyperName;
        JSONObject hyperInfo = TugraphUtil.listNodeLabelInfo(graphName, hyperLabel);
        JSONObject hyperResult = new JSONObject();
        hyperResult.put("name", hyperName);
        JSONArray properties = new JSONArray();

        if (hyperInfo != null)
            for (Map.Entry<String, Object> entry : hyperInfo.entrySet()) {
                String key = entry.getKey();
                HashMap value = (HashMap) entry.getValue();

                JSONObject property = new JSONObject();
                String type = String.valueOf(value.get("type"));
                if (type.equalsIgnoreCase("INT8")) {
                    hyperResult.put("startLabel", key);

                } else if (type.equalsIgnoreCase("INT16")) {
                    hyperResult.put("endLabel", key);
                } else if (type.equalsIgnoreCase("INT64")) {
                    property.put("key", key.split("_")[0]);
                    property.put("value", key.split("_")[1]);
                    properties.add(property);
                }
            }

        hyperResult.put("properties", properties);
//        System.out.println(hyperResult);
        return new RetResult(200, hyperResult);
    }

    @GetMapping("/listhyperrelationship")
    public RetResult listHyperRelationship(@RequestParam() String graphName) {
        JSONObject LabelList = TugraphUtil.listGraphLabel(graphName);
        ArrayList<String> nodeLabelList = (ArrayList<String>) LabelList.get("vertex");
        JSONArray result = new JSONArray();
        for (int i = 0; i < nodeLabelList.size(); i++) {
            String nodeName = nodeLabelList.get(i);
            if (nodeName.startsWith("hyperRelationship_")) {
                JSONObject hyperInfo = TugraphUtil.listNodeLabelInfo(graphName, nodeName);
                JSONObject hyperResult = new JSONObject();
                hyperResult.put("name", nodeName.substring("hyperRelationship_".length()));
                JSONArray properties = new JSONArray();

                if (hyperInfo != null)
                    for (Map.Entry<String, Object> entry : hyperInfo.entrySet()) {
                        String key = entry.getKey();
                        HashMap value = (HashMap) entry.getValue();

                        JSONObject property = new JSONObject();
                        String type = String.valueOf(value.get("type"));
                        if (type.equalsIgnoreCase("INT8")) {
                            hyperResult.put("startLabel", key);

                        } else if (type.equalsIgnoreCase("INT16")) {
                            hyperResult.put("endLabel", key);
                        } else if (type.equalsIgnoreCase("INT64")) {
                            property.put("key", key.split("_")[0]);
                            property.put("value", key.split("_")[1]);
                            properties.add(property);
                        }
                    }

                hyperResult.put("properties", properties);
//                System.out.println(hyperResult);
                result.add(hyperResult);
            }

        }
        return new RetResult(200, result);
    }

    @GetMapping("/listhypergraphinfo")
    public RetResult listHyperGraphInfo(@RequestParam() String graphName) {
        JSONObject result = new JSONObject();
        // 获取顶点信息和边信息
        JSONArray nodes = new JSONArray();
        JSONArray edges = new JSONArray();
        JSONObject LabelList = TugraphUtil.listGraphLabel(graphName);
        ArrayList<String> nodeLabelList = (ArrayList<String>) LabelList.get("vertex");
        for (int i = 0; i < nodeLabelList.size(); i++) {
            String nodeName = nodeLabelList.get(i);
            JSONObject node = new JSONObject();

            if (nodeName.startsWith("hyperRelationship_")) {
                JSONObject hyperInfo = TugraphUtil.listNodeLabelInfo(graphName, nodeName);
                String hyperName = nodeName.substring("hyperRelationship_".length());
                node.put("id", hyperName);
                node.put("label", hyperName);
                node.put("role", "hyperRelationship");
                // JSONObject hyperResult  = new JSONObject();
                // hyperResult.put("name", nodeName.substring("hyperRelationship_".length()));
                // JSONArray properties = new JSONArray();

                if (hyperInfo != null)
                    for (Map.Entry<String, Object> entry : hyperInfo.entrySet()) {
                        String key = entry.getKey();
                        HashMap value = (HashMap) entry.getValue();

                        JSONObject property = new JSONObject();
                        String type = String.valueOf(value.get("type"));
                        if (type.equalsIgnoreCase("INT8")) {
                            String head = key;
                            JSONObject edge = new JSONObject();
                            edge.put("source", head);
                            edge.put("target", hyperName);
                            edge.put("label", head + "-" + hyperName);
                            edge.put("role", "head-hyperRelation");
                            edges.add(edge);
                        } else if (type.equalsIgnoreCase("INT16")) {
                            String tail = key;
                            JSONObject edge = new JSONObject();
                            edge.put("source", hyperName);
                            edge.put("target", tail);
                            edge.put("label", hyperName + "-" + tail);
                            edge.put("role", "hyperRelation-tail");
                            edges.add(edge);
                        } else if (type.equalsIgnoreCase("INT64")) {
                            String attributeValue = key.split("_")[1];
                            String attributeKey = key.split("_")[0];
                            JSONObject edge = new JSONObject();
                            edge.put("source", hyperName);
                            edge.put("target", attributeValue);
                            edge.put("label", attributeKey);
                            edge.put("role", "key-value");
                            edges.add(edge);
                        }
                    }
            } else {
                node.put("id", nodeName);
                node.put("label", nodeName);
                node.put("role", "entity");
            }
            nodes.add(node);
        }
        result.put("nodes", nodes);
        result.put("edges", edges);

        return new RetResult(200, result);
    }


    @GetMapping("/listedgelabel")
    public RetResult listEdgeLabel(@RequestParam() String graphName) {
        JSONObject LabelList = TugraphUtil.listGraphLabel(graphName);
        JSONArray edgeLabelList = LabelList.getJSONArray("edge");
        JSONArray result = new JSONArray();
        // for each edge label
        for (int i = 0; i < edgeLabelList.size(); i++) {
            // edge name
            String edgeName = edgeLabelList.getString(i);
            // edge props and their data types
            JSONObject edgeInfo = TugraphUtil.listEdgeLabelInfo(graphName, edgeName);
            // edge constraints, and props
            JSONObject edgeSchema = JSONObject.parseObject(TugraphUtil.getEdgeSchema(graphName, edgeName).getJSONArray(0).getString(0));
            JSONObject edgeResult = new JSONObject();
            edgeResult.put("name", edgeName);
            JSONArray properties = new JSONArray();
            // int8->startlbl, int16->endlbl, others->property->properties
            if (edgeInfo != null)
                for (Map.Entry<String, Object> entry : edgeInfo.entrySet()) {
                    String key = entry.getKey();
                    // Object value = entry.getValue();
                    HashMap value = (HashMap) entry.getValue();

                    // JSONObject jsonValue = (JSONObject) JSON.toJSON(value);
                    JSONObject property = new JSONObject();
                    String type = String.valueOf(value.get("type"));
//                    System.out.println(type);
                    if (type.equalsIgnoreCase("INT8")) {
                        edgeResult.put("startLabel", key);
                    } else if (type.equalsIgnoreCase("INT16")) {
                        edgeResult.put("endLabel", key);
                    } else {
                        property.put("name", key);

                        if (type.equalsIgnoreCase("string"))
                            property.put("type", "文本");
                        else if (type.equalsIgnoreCase("date"))
                            property.put("type", "时间");
                        else if (type.equalsIgnoreCase("int32"))
                            property.put("type", "整型");
                        else if (type.equalsIgnoreCase("double"))
                            property.put("type", "双精度浮点型");
                        properties.add(property);
                    }
                }
            edgeResult.put("properties", properties);
            JSONArray constraints = edgeSchema.getJSONArray("constraints");
            // constraints:[["",""]] or []
            if (constraints.size() == 0) {
                edgeResult.put("srcLabel", "unconstrained/any");
                edgeResult.put("dstLabel", "unconstrained/any");
            } else {
                edgeResult.put("srcLabel", constraints.getJSONArray(0).getString(0));
                edgeResult.put("dstLabel", constraints.getJSONArray(0).getString(1));
            }
            result.add(edgeResult);
        }

        return new RetResult(200, result);
    }

    @PostMapping("/deletehyperrelationship")
    public RetResult deleteHyperRelationship(@RequestParam() String graphName, @RequestParam() String edgeName) {
        try {
            // 删除超关系节点和所有超关系边
            JSONObject LabelList = TugraphUtil.listGraphLabel(graphName);
            ArrayList<String> nodeLabelList = (ArrayList<String>) LabelList.get("vertex");
            String hyperLabel = "hyperRelationship_" + edgeName;
            for (int i = 0; i < nodeLabelList.size(); i++) {
                String nodeName = nodeLabelList.get(i);
                if (nodeName.equalsIgnoreCase(hyperLabel)) {
                    JSONObject hyperInfo = TugraphUtil.listNodeLabelInfo(graphName, nodeName);

                    if (hyperInfo != null)
                        for (Map.Entry<String, Object> entry : hyperInfo.entrySet()) {
                            String key = entry.getKey();
                            HashMap value = (HashMap) entry.getValue();

                            String type = String.valueOf(value.get("type"));
                            if (type.equalsIgnoreCase("INT8")) {
                                String head = key;
                                String headHyperLabel = head + "_" + edgeName;
                                HttpStatus httpStatus = TugraphUtil.deleteEdgeLabel(graphName, headHyperLabel);
                                if (httpStatus.is4xxClientError())
                                    return new RetResult(400, "头实体-超关系边标签删除失败，请检查");
                            } else if (type.equalsIgnoreCase("INT16")) {
                                String tail = key;
                                String hyperTailLabel = edgeName + "_" + tail;
                                HttpStatus httpStatus = TugraphUtil.deleteEdgeLabel(graphName, hyperTailLabel);
                                if (httpStatus.is4xxClientError())
                                    return new RetResult(400, "超关系-尾实体边标签删除失败，请检查");
                            } else if (type.equalsIgnoreCase("INT64")) {
                                String attributeKey = key.split("_")[0];
                                String attributeValue = key.split("_")[1];
                                String hyperAttributeLabel = edgeName + "_" + attributeValue + "_" + attributeKey;
                                HttpStatus httpStatus = TugraphUtil.deleteEdgeLabel(graphName, hyperAttributeLabel);
                                if (httpStatus.is4xxClientError())
                                    return new RetResult(400, "超关系-属性边标签删除失败，请检查");
                            }
                        }

                    HttpStatus httpStatus = TugraphUtil.deleteNodeLabel(graphName, nodeName);
                    if (httpStatus.is4xxClientError())
                        return new RetResult(400, "超关系节点标签删除失败，请检查");
                }
            }
        } catch (Exception e) {
            return new RetResult(400, "超关系标签删除失败，请检查");
        }
        return new RetResult(200, "超关系标签添加成功");
    }

    @PostMapping("/deleteedgelabel")
    public RetResult deleteEdgeLabel(@RequestParam() String graphName, @RequestParam() String edgeName) {
        try {
            HttpStatus httpStatus = TugraphUtil.deleteEdgeLabel(graphName, edgeName);
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "边标签删除成功");
        } catch (Exception e) {
            return new RetResult(400, "边标签删除失败，请检查");
        }
        return new RetResult(400, "边标签删除失败，请检查");
    }

    @DeleteMapping("/deletelabel")
    public RetResult deleteLabel(String graphName, String labelName, String labelType) {
        try {
            HttpStatus httpStatus;
            if (labelType.equalsIgnoreCase("node"))
                httpStatus = TugraphUtil.deleteNodeLabel(graphName, labelName);
            else
                httpStatus = TugraphUtil.deleteEdgeLabel(graphName, labelName);
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "标签删除成功");
        } catch (Exception e) {
            return new RetResult(400, e.getMessage());
        }
        return new RetResult(400, "标签删除失败，请检查");
    }

    @PostMapping("/import-schema")
    public RetResult importSchema(MultipartFile file) {
        try {
            String type = file.getContentType();
            assert type != null;
            if (!file.getContentType().equals("application/json")) {
                return new RetResult(400, "文件格式错误");
            }
            String name = file.getOriginalFilename();
            String graphName = FilenameUtils.removeExtension(name);
            InputStream in = file.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String text = reader.lines().collect(Collectors.joining());
            HttpStatus httpStatus = TugraphUtil.importSchema(graphName, JSONObject.parseObject(text).toJSONString());
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "导入成功");
            else
                return new RetResult(400, "导入失败");
        } catch (IOException e) {
            e.printStackTrace();
            return new RetResult(400, "导入失败");
        }
    }

    @GetMapping("/download-schema-template")
    public RetResult downloadSchemaTemplate(HttpServletResponse response) {
        OutputStream out;
        BufferedWriter bw = null;
        try {
            String fileName = "schema_template.json";
            response.setCharacterEncoding("UTF-8");
            response.setContentType("multipart/form-data");
            response.addHeader("Content-Disposition", "attachment;fileName=" + URLEncoder.encode(fileName, "UTF-8"));
            response.setHeader("filetype", "application/json");
            out = response.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(out));
            bw.write(
                    "{\n" +
                            "  \"schema\": [\n" +
                            "    {\n" +
                            "      \"label\": \"actor\",\n" +
                            "      \"type\": \"VERTEX\",\n" +
                            "      \"properties\": [\n" +
                            "        { \"name\": \"name\", \"type\": \"STRING\", \"optional\": false }\n" +
                            "        { \"name\": \"nationality\", \"type\": \"STRING\", \"optional\": true },\n" +
                            "      ],\n" +
                            "      \"primary\": \"name\"\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"label\": \"movie\",\n" +
                            "      \"type\": \"VERTEX\",\n" +
                            "      \"properties\": [\n" +
                            "        { \"name\": \"name\", \"type\": \"STRING\", \"optional\": false },\n" +
                            "        { \"name\": \"year\", \"type\": \"INT32\", \"optional\": true },\n" +
                            "        { \"name\": \"rate\", \"type\": \"DOUBLE\", \"optional\": true }\n" +
                            "      ],\n" +
                            "      \"primary\": \"name\"\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"label\": \"play_in\",\n" +
                            "      \"type\": \"EDGE\",\n" +
                            "      \"properties\": [{ \"name\": \"role\", \"type\": \"STRING\", \"optional\": true }],\n" +
                            "      \"constraints\": [[\"actor\", \"movie\"]]\n" +
                            "    }\n" +
                            "  ],\n" +
                            "}"
            );
            bw.flush();
            return new RetResult(200, "下载成功");
        } catch (IOException e) {
            e.printStackTrace();
            return new RetResult(400, "下载失败");
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @GetMapping("/download-template-readme")
    public void downloadTemplateReadme(HttpServletResponse response) {
        OutputStream out;
        BufferedWriter bw = null;
        try {
            response.setCharacterEncoding("UTF-8");
            response.setContentType("multipart/form-data");
            response.setHeader("Content-Disposition", "attachment;filename=" + URLEncoder.encode("README.txt", "UTF-8"));
            response.addHeader("filetype", "text/plain");
            out = response.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(out));
            bw.write("模板说明：\n" +
                    "1. 请将文件命名为graphName.json，graphName用要导入的图谱名称替代\n" +
                    "2. 请使用UTF-8编码打开和保存文件\n" +
                    "3. 模板中的schema为一个数组，其每一个项是一个标签，可以任意扩充\n" +
                    "4. 每个标签的label为标签名称，只能包含数字、字母、下划线，type为标签类型，可选VERTEX（实体）或EDGE（关系）\n" +
                    "5. 每个标签的properties为一个数组，其每一项是一个属性，可以任意扩充\n" +
                    "6. 每个属性的name为属性名称，name必填，只能包含数字、字母、下划线，type为属性类型，可选STRING、INT32、DOUBLE、DATE\n" +
                    "7. 对于实体标签，每个标签的primary为主键，必需填name\n" +
                    "8. 对于关系标签，每个标签的constraints为约束，约束为一个数组，可以任意扩充\n" +
                    "9. 约束的每一项也是一个数组，数组中的两个元素分别为关系的起点和终点标签\n" +
                    "10. 每个属性的optional为是否可选，name属性的必须填false，其余的必须填true\n"
            );
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
