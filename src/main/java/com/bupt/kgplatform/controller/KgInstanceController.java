package com.bupt.kgplatform.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bupt.kgplatform.common.RetResult;
import com.bupt.kgplatform.common.TugraphUtil;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;

@RestController
@RequestMapping("/kgplatform/kginstance")
public class KgInstanceController {
    @GetMapping("/listsubgraph")
    public RetResult listSubgraph() {
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
     * 反复查询单个节点并拼接结果
     * @param graphName kg
     * @param start first node
     * @param count total nodes
     * @return array of nodes
     * @deprecated
     */
    @GetMapping("/listgraphnodes")
    public RetResult listGraphNodes(String graphName, Integer start, Integer count) {
        JSONArray result = new JSONArray();
        for (int i = start; i < start + count; i++) {
            JSONObject nodeInfo = TugraphUtil.getGraphNodeInstanceById(graphName, i);
            if (nodeInfo.containsKey("error_message")) {
                break;
            }
            JSONObject nodeProperty  = nodeInfo.getJSONObject("property");
            JSONObject resultEntry = new JSONObject();
            JSONArray resultEntryAtrr = new JSONArray();
            resultEntry.put("label", nodeInfo.get("label"));
            if (nodeProperty.containsKey("name")) {
                resultEntry.put("name", nodeProperty.get("name"));
            }
            else {
                resultEntry.put("name", nodeProperty.get("id"));
            }
            nodeProperty.forEach((key, value) -> {
                if (!key.equals("name") && !key.equals("id")) {
                    JSONObject attr = new JSONObject();
                    attr.put("key",key);
                    attr.put("value", value);
                    resultEntryAtrr.add(attr);
                }
            });
            resultEntry.put("attributes", resultEntryAtrr);
            result.add(resultEntry);
        }
        return new RetResult(200, result);
    }

    /**
     * 获取所有节点，调用cypher接口
     * @param graphName kg
     * @return res:{
     *  code, msg, data:{
     *   result:[
     *    {label:"",identity:int,attributes:[{key:"",value:""},...],name:""},
     *    ...],
     *   resultSize:int
     *  }
     * }
     */
    @GetMapping("/listgraphnodesall")
    public RetResult listGraphNodesAll(String graphName) {
        JSONObject result = new JSONObject();
        JSONObject res = TugraphUtil.getGraphNodesAllWithCypher(graphName);
        JSONArray nodeList = res.getJSONArray("result");
        JSONArray newNodeList = new JSONArray();
        for (Object node : nodeList) {

            JSONObject nodeInfo = JSONObject.parseObject(JSONArray.parseArray(node.toString()).get(0).toString());
            JSONObject nodeProperty  = nodeInfo.getJSONObject("properties");
            JSONObject resultEntry = new JSONObject();
            JSONArray resultEntryAtrr = new JSONArray();
            resultEntry.put("identity", nodeInfo.get("identity"));
            resultEntry.put("label", nodeInfo.get("label"));
            // should all be names. add a constraint in schema.
            if (nodeProperty.containsKey("name")) {
                resultEntry.put("name", nodeProperty.get("name"));
            } else if (nodeProperty.containsKey("title")) {
                resultEntry.put("name", nodeProperty.get("title"));
            }
            else {
                resultEntry.put("name", nodeProperty.get("id"));
            }
            nodeProperty.forEach((key, value) -> {
                if (!key.equals("name") && !key.equals("id") && !key.equals("title")) {
                    JSONObject attr = new JSONObject();
                    attr.put("key",key);
                    attr.put("value", value);
                    resultEntryAtrr.add(attr);
                }
            });
            resultEntry.put("attributes", resultEntryAtrr);
            newNodeList.add(resultEntry);
        }
        result.put("resultSize", res.get("resultSize"));
        result.put("result", newNodeList);
        return new RetResult(200, result);
    }
    @GetMapping("/listgraphnodesbypage")
    public RetResult listGraphNodesByPage(String graphName, int page, int size, String label, String name) {
        JSONArray result = new JSONArray();
        JSONArray res = TugraphUtil.getGraphNodesAsPagedWithCypher(graphName,page,size,label,name);
        for (Object node : res) {
            JSONObject nodeInfo = JSONObject.parseObject(JSONArray.parseArray(node.toString()).get(0).toString());
            // node info: identity, label, properties
            JSONObject nodeProperty  = nodeInfo.getJSONObject("properties");
            JSONObject resultEntry = new JSONObject();
            resultEntry.put("identity", nodeInfo.get("identity"));
            resultEntry.put("label", nodeInfo.get("label"));
            // should all be names. add a constraint in schema.
            if (nodeProperty.containsKey("name")) {
                resultEntry.put("name", nodeProperty.get("name"));
            } else if (nodeProperty.containsKey("title")) {
                resultEntry.put("name", nodeProperty.get("title"));
            }
            else {
                resultEntry.put("name", nodeProperty.getOrDefault("id", "name?"));
            }
            JSONArray resultEntryAtrr = new JSONArray();
            nodeProperty.forEach((key, value) -> {
                if (!key.equals("name")) {
                    JSONObject attr = new JSONObject();
                    attr.put("key",key);
                    attr.put("value", value);
                    resultEntryAtrr.add(attr);
                }
            });
            resultEntry.put("attributes", resultEntryAtrr);
            result.add(resultEntry);
            // result entry: identity, label, name, attributes
        }
        return new RetResult(200, result);
    }
    @GetMapping("listgraphnodelabels")
    public RetResult listGraphNodeLabels(String graphName) {
        JSONObject labelInfo = TugraphUtil.listGraphLabel(graphName);
        JSONArray result = labelInfo.getJSONArray("vertex");
        return new RetResult(200, result);
    }
    @GetMapping("listgraphedgelabels")
    public RetResult listGraphEdgeLabels(String graphName) {
        JSONObject labelInfo = TugraphUtil.listGraphLabel(graphName);
        JSONArray result = labelInfo.getJSONArray("edge");
        return new RetResult(200, result);
    }
    @GetMapping("/countgraphvertex")
    public RetResult countGraphVertex(String graphName, String label, String name) {
        Integer result = TugraphUtil.getGraphNodeCount(graphName,label,name);
        return new RetResult(200, result);
    }
    @GetMapping("/countgraphedge")
    public RetResult countGraphEdge(String graphName, String label) {
        Integer result = TugraphUtil.getGraphEdgeCount(graphName,label);
        return new RetResult(200, result);
    }
    @GetMapping("/listgraphedgesall")
    public RetResult listGraphEdgesAll(String graphName) {
        //node list for name mapping
        RetResult nodesListResponse = listGraphNodesAll(graphName);
        JSONObject nodesListData = (JSONObject) nodesListResponse.getData();
        JSONArray nodesListResult = nodesListData.getJSONArray("result");
        HashMap<String, String> nodeMapping = new HashMap<>();
        for (Object node :
                nodesListResult) {
            nodeMapping.put(((JSONObject)node).get("identity").toString(), ((JSONObject)node).get("name").toString());
        }
        //edge label list for name mapping
        JSONArray edgeLabelList = TugraphUtil.listGraphLabel(graphName).getJSONArray("edge");
        //edge list
        JSONObject result = new JSONObject();
        JSONObject res = TugraphUtil.getGraphEdgesAll(graphName);
        JSONArray edgeList = res.getJSONArray("result");
        JSONArray newEdgeList = new JSONArray();
        for (Object edge : edgeList) {
            JSONObject edgeInfo = JSONObject.parseObject(JSONArray.parseArray(edge.toString()).get(0).toString());
            JSONArray resultEntryAtrr = new JSONArray();
            if (edgeInfo.containsKey("properties")) {
                JSONObject edgeProperty = edgeInfo.getJSONObject("properties");
                edgeProperty.forEach((key, value) -> {
                    JSONObject attr = new JSONObject();
                    attr.put("key",key);
                    attr.put("value", value);
                    resultEntryAtrr.add(attr);
                });
            }
            JSONObject resultEntry = new JSONObject();
            resultEntry.put("identity", edgeInfo.get("identity"));
            JSONObject edgeLabel = new JSONObject();
            edgeLabel.put("id", edgeInfo.get("label_id"));
            edgeLabel.put("name", edgeLabelList.get((Integer) edgeInfo.get("label_id")));
            resultEntry.put("label", edgeLabel);
            JSONObject startNode = new JSONObject();
            startNode.put("id", edgeInfo.get("src"));
            startNode.put("name", nodeMapping.get(edgeInfo.get("src").toString()));
            resultEntry.put("start", startNode);
            JSONObject endNode = new JSONObject();
            endNode.put("id", edgeInfo.get("dst"));
            endNode.put("name", nodeMapping.get(edgeInfo.get("dst").toString()));
            resultEntry.put("end", endNode);
            resultEntry.put("forward", edgeInfo.get("forward"));
            resultEntry.put("attributes", resultEntryAtrr);
            newEdgeList.add(resultEntry);
        }
        result.put("resultSize", res.get("resultSize"));
        result.put("result", newEdgeList);
        return new RetResult(200, result);
    }
    @GetMapping("/listgraphedgesbypage")
    public RetResult listGraphEdgesByPage(String graphName, int page, int size, String label) {
        // 需要获得node list，把edge的node id转成name
        // 小数据库（<5000）可以一次性list all，1.3秒
        // 对于大数据库，无法list all，只能针对10个edge的首尾分别查询node名字，2.3秒
        // 先判断大小再决定用list all + map还是分别查询，优化性能
        RetResult countResponse = countGraphVertex(graphName, null, null);
        int nodeCount = Integer.parseInt(countResponse.getData().toString());
        HashMap<String, String> nodeMapping = new HashMap<>();
        if (nodeCount<5000) {
            RetResult nodesListResponse = listGraphNodesAll(graphName);
            JSONObject nodesListData = (JSONObject) nodesListResponse.getData();
            JSONArray nodesListResult = nodesListData.getJSONArray("result");
            for (Object node :
                    nodesListResult) {
                nodeMapping.put(((JSONObject) node).get("identity").toString(), ((JSONObject) node).get("name").toString());
            }
        }
        //edge label list for name mapping
        JSONArray edgeLabelList = TugraphUtil.listGraphLabel(graphName).getJSONArray("edge");
        //edge list
        JSONArray result = new JSONArray();
        JSONArray res = TugraphUtil.getGraphEdgesAsPaged(graphName,page,size,label);
        for (Object edge : res) {
            JSONObject edgeInfo = JSONObject.parseObject(JSONArray.parseArray(edge.toString()).get(0).toString());
            // edge info: dst, src, identity, label_id, properties
            JSONObject resultEntry = new JSONObject();
            resultEntry.put("identity", edgeInfo.get("identity"));
            JSONObject edgeLabel = new JSONObject();
            edgeLabel.put("id", edgeInfo.get("label_id"));
            edgeLabel.put("name", edgeLabelList.get(edgeInfo.getInteger("label_id")));
            resultEntry.put("label", edgeLabel);
            if (nodeCount<5000) {
                JSONObject startNode = new JSONObject();
                startNode.put("id", edgeInfo.get("src"));
                startNode.put("name", nodeMapping.get(edgeInfo.get("src").toString()));
                resultEntry.put("start", startNode);
                JSONObject endNode = new JSONObject();
                endNode.put("id", edgeInfo.get("dst"));
                endNode.put("name", nodeMapping.get(edgeInfo.get("dst").toString()));
                resultEntry.put("end", endNode);
            } else {
                JSONObject startNodeInfo = TugraphUtil.getGraphNodeInstanceById(graphName,Integer.parseInt(edgeInfo.get("src").toString())).getJSONObject("property");
                JSONObject startNode = new JSONObject();
                startNode.put("id", edgeInfo.get("src"));
                startNode.put("name", startNodeInfo.getOrDefault("name", edgeInfo.get("src")));
                resultEntry.put("start", startNode);
                JSONObject endNodeInfo = TugraphUtil.getGraphNodeInstanceById(graphName,Integer.parseInt(edgeInfo.get("dst").toString())).getJSONObject("property");
                JSONObject endNode = new JSONObject();
                endNode.put("id", edgeInfo.get("dst"));
                endNode.put("name", endNodeInfo.getOrDefault("name", edgeInfo.get("dst")));
                resultEntry.put("end", endNode);
            }
            resultEntry.put("forward", edgeInfo.get("forward"));
            JSONArray resultEntryAtrr = new JSONArray();
            JSONObject labelInfo = TugraphUtil.listEdgeLabelInfo(graphName,edgeLabelList.getString(edgeInfo.getInteger("label_id")));
            if (edgeInfo.containsKey("properties")) {
                JSONObject edgeProperty = edgeInfo.getJSONObject("properties");
                edgeProperty.forEach((key, value) -> {
                    String dataType = labelInfo.getJSONObject(key).getString("type");
                    // ignore special fields reserved for dst(int16) and src(int8)
                    if (!dataType.equalsIgnoreCase("int8") && !dataType.equalsIgnoreCase("int16")) {
                        JSONObject attr = new JSONObject();
                        attr.put("key", key);
                        attr.put("value", value);
                        resultEntryAtrr.add(attr);
                    }
                });
            }
            resultEntry.put("attributes", resultEntryAtrr);
            result.add(resultEntry);
            // result entry: identity, id, name, label, start, end, attributes
        }
        return new RetResult(200, result);
    }
    @PostMapping ("/addnode")
    public RetResult addNode(@RequestBody String requestBody) {
        JSONObject body = JSONObject.parseObject(requestBody);
        String graphName = body.getString("graphName");
        String name = body.getString("name");
        String label = body.getString("label");
//        String description = body.getString("description");
        try {
            HttpStatus httpStatus = TugraphUtil.addNode(graphName, name, label);
            if(httpStatus.is2xxSuccessful())
                return new RetResult(200,"添加成功");
        }
        catch (Exception e){
            e.printStackTrace();
            return new RetResult(400,"add failed");
        }
        return new RetResult(400,"add failed");
    }
    @DeleteMapping("/deletenode")
    public RetResult deleteNode(String graphName, String nodeId) {
        try {
            JSONArray adjacentEdges = new JSONArray();
            adjacentEdges.addAll(TugraphUtil.getIncomingEdges(graphName, nodeId));
            adjacentEdges.addAll(TugraphUtil.getOutgoingEdges(graphName, nodeId));
            adjacentEdges.forEach(edge -> {
                HttpStatus httpStatus = TugraphUtil.deleteEdge(graphName, edge.toString());
                if (!httpStatus.is2xxSuccessful()) {
                    throw new RuntimeException();
                }
            });
            HttpStatus httpStatus = TugraphUtil.deleteNode(graphName, nodeId);
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "node deleted");
        } catch (Exception e) {
            e.printStackTrace();
            return new RetResult(400, "delete failed");
        }
        return new RetResult(400, "delete failed");
    }
    @PutMapping("/updatenode")
    public RetResult updateNode(@RequestBody String requestBody) {
        JSONObject body = JSONObject.parseObject(requestBody);
        String graphName = body.getString("graphName");
        String nodeId = body.getString("nodeId");
        String nodeLabel = body.getString("nodeLabel");
        JSONArray propertyList = body.getJSONArray("property");
        JSONObject property = new JSONObject();
        JSONObject labelInfo = TugraphUtil.listNodeLabelInfo(graphName,nodeLabel);
        for (Object prop :
                propertyList) {
            JSONObject p = JSONObject.parseObject(prop.toString());
            String dataType = labelInfo.getJSONObject(p.getString("key")).getString("type");
            if (dataType.equalsIgnoreCase("string")) {
                property.put(p.get("key").toString(), p.getString("value"));
            } else if (dataType.equalsIgnoreCase("int32")) {
                property.put(p.get("key").toString(), p.getInteger("value"));
            } else if (dataType.equalsIgnoreCase("float")) {
                property.put(p.get("key").toString(), p.getFloat("value"));
            } else if (dataType.equalsIgnoreCase("double")) {
                property.put(p.get("key").toString(), p.getDouble("value"));
            }
        }
        try {
            HttpStatus httpStatus = TugraphUtil.updateNode(graphName, nodeId, property);
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "node updated");
        } catch (Exception e) {
            e.printStackTrace();
            return new RetResult(400, "update failed");
        }
        return new RetResult(400, "update failed");
    }
    @PostMapping("/importnodes")
    public RetResult importNodes(String graphName, MultipartFile file) {
        try {
            String type = file.getContentType();
            assert type != null;
            if (!file.getContentType().equals("text/csv")) {
                return new RetResult(400, "wrong file type");
            }
            String name = file.getOriginalFilename();
            String label = FilenameUtils.removeExtension(name);
            InputStream in = file.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String[] f = br.readLine().split(",");
            JSONArray fields = new JSONArray();
            fields.addAll(Arrays.asList(f));
            JSONObject labelInfo = TugraphUtil.listNodeLabelInfo(graphName,label);
            JSONArray values = new JSONArray();
            String line;
            do {
                line = br.readLine();
                if (line != null) {
                    JSONArray value = new JSONArray();
                    String[] v = line.split(",");
                    for (int i = 0; i < v.length; i++) {
                        String dataType = labelInfo.getJSONObject(fields.getString(i)).getString("type"); 
                        if (dataType.equalsIgnoreCase("string")) {
                            value.add(v[i]);
                        } else if (dataType.equalsIgnoreCase("int32")) {
                            value.add(Integer.parseInt(v[i]));
                        } else if (dataType.equalsIgnoreCase("float")) {
                            value.add(Float.parseFloat(v[i]));
                        } else if (dataType.equalsIgnoreCase("double")) {
                            value.add(Double.parseDouble(v[i]));
                        }
                    }
                    values.add(value);
                }
            }
            while (line != null);
            br.close();
            HttpStatus httpStatus = TugraphUtil.addNodesBatch(graphName, label, fields, values);
            if (httpStatus.is2xxSuccessful()) {
                return new RetResult(200, "nodes imported");
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new RetResult(400, "nodes import failed");
        }
        return new RetResult(400, "nodes import failed");
    }
    @GetMapping("/downloadnodestemplate")
    public void downloadNodesTemplate(HttpServletResponse response) {
        OutputStream out;
        BufferedWriter bw = null;
        try {
            response.setCharacterEncoding("UTF-8");
            response.setContentType("multipart/form-data");
            response.setHeader("Content-Disposition", "attachment;filename="+URLEncoder.encode("node_template.csv","UTF-8"));
            response.addHeader("filetype", "text/csv");
            out = response.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(out));
            bw.write("name,attr1,attr2,...");
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert bw != null;
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    @DeleteMapping("/deleteedgebyid")
    public RetResult deleteEdgeById(String graphName, String edgeId) {
        try {
            HttpStatus httpStatus = TugraphUtil.deleteEdge(graphName, edgeId);
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "edge deleted");
        } catch (Exception e) {
            e.printStackTrace();
            return new RetResult(400, "delete failed");
        }
        return new RetResult(400, "delete failed");
    }
    /**
     * <b>原先的节点名字改成了节点ID</b>
     * 因为有多个constraints时，名字+constraints label可能找不到对应的节点
     * 前端需对应更改（getEdge方法提供了节点name和id，可用）
     * @param graphName 图名
     * @param start 起点<b>ID</b>，向旧兼容name但不推荐
     * @param relationship 关系名称
     * @param end 终点<b>ID</b>，向旧兼容name但不推荐
     * @return 200
     */
    @DeleteMapping("/deleteedge")
    public RetResult deleteEdge(String graphName, String start, String relationship, String end) {
        try {
            HttpStatus httpStatus;
            if (StringUtils.isNumeric(start) && StringUtils.isNumeric(end)) {
                // 新接口，传节点id
                httpStatus = TugraphUtil.deleteEdge(graphName, relationship, relationship, start, end);
            } else {
                // 旧接口，传节点name，猜节点label，不安全
                JSONObject edgeSchema = JSONObject.parseObject(TugraphUtil.getEdgeSchema(graphName,relationship).getJSONArray(0).getString(0));
                JSONArray constraints = edgeSchema.getJSONArray("constraints");
                httpStatus = TugraphUtil.deleteEdge(graphName, relationship, constraints.getJSONArray(0).getString(0), start, constraints.getJSONArray(0).getString(1), end);
            }
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "edge deleted");
        } catch (Exception e) {
            e.printStackTrace();
            return new RetResult(400, "delete failed");
        }
        return new RetResult(400, "delete failed");
    }

    /**
     * <b>原先的节点名字改成了节点ID</b>
     * 因为有多个constraints时，名字+constraints label可能找不到对应的节点
     * 前端需对应更改（getEdge方法提供了节点name和id，可用）
     * @param requestBody graphName,(edgeLabel)name,start,end（推荐nodeID，兼容name）
     * @return 200
     */
    @PostMapping("/addedge")
    public RetResult addEdge(@RequestBody String requestBody) {
        JSONObject body = JSONObject.parseObject(requestBody);
        String graphName = body.getString("graphName");
        String name = body.getString("name");
        String start = body.getString("start");
        String end = body.getString("end");
        try {
            HttpStatus httpStatus;
            if (StringUtils.isNumeric(start) && StringUtils.isNumeric(end)) {
                // 新接口，传节点id
                httpStatus = TugraphUtil.addEdge(graphName, name, null, start, null, end);
            } else {
                // 旧接口，传节点name，猜节点label，不安全
                JSONObject edgeSchema = JSONObject.parseObject(TugraphUtil.getEdgeSchema(graphName,name).getJSONArray(0).getString(0));
                JSONArray constraints = edgeSchema.getJSONArray("constraints");
                httpStatus = TugraphUtil.addEdge(graphName, name, constraints.getJSONArray(0).getString(0), start, constraints.getJSONArray(0).getString(1), end);
            }
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "edge added");
        } catch (Exception e) {
            e.printStackTrace();
            return new RetResult(400, "add failed");
        }
        return new RetResult(400, "add failed");
    }

    /**
     * @deprecated 本方法查询constraints，但会有多个，有可能匹配不到对应的起止节点，导致无法删除和新建
     * @see #updateEdgeProp(String)
     */
    @PutMapping("/updateedge")
    public RetResult updateEdge(@RequestBody String requestBody) {
        JSONObject body = JSONObject.parseObject(requestBody);
        String graphName = body.getString("graphName");
        String name = body.getString("name");
        String start = body.getString("start");
        String end = body.getString("end");
        JSONArray propertyList = body.getJSONArray("property");
        String oldName = body.getString("oldName");
        String oldStart = body.getString("oldStart");
        String oldEnd = body.getString("oldEnd");
        JSONObject oldEdgeSchema = JSONObject.parseObject(TugraphUtil.getEdgeSchema(graphName,oldName).getJSONArray(0).getString(0));
        JSONArray oldConstraints = oldEdgeSchema.getJSONArray("constraints");
        JSONObject edgeSchema = JSONObject.parseObject(TugraphUtil.getEdgeSchema(graphName,name).getJSONArray(0).getString(0));
        JSONArray constraints = edgeSchema.getJSONArray("constraints");
        try {
            // delete old edge
            HttpStatus deleteStatus = TugraphUtil.deleteEdge(graphName, oldName, oldConstraints.getJSONArray(0).getString(0), oldStart, oldConstraints.getJSONArray(0).getString(1), oldEnd);
            if (deleteStatus.is2xxSuccessful()) {
                try {
                    // delete succeeds, add new edge
                    HttpStatus addStatus = TugraphUtil.addEdge(graphName, name, constraints.getJSONArray(0).getString(0), start, constraints.getJSONArray(0).getString(1), end, propertyList);
                    if (addStatus.is2xxSuccessful()) {
                        return new RetResult(200, "edge updated");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return new RetResult(400, "update failed");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new RetResult(400, "update failed");
        }
        return new RetResult(400, "update failed");
    }

    /**
     * （1）可视化下传edge的euid，调用更改edge属性的方法
     * （2）表单编辑传edge的label和节点id，先删后加
     * 见“if vis”和“if form”注释，在判断参数和选择util两处都有
     * @param requestBody 兼容可视化的euid和表单的节点id+edgeLabel，另graphName+property
     * @return 200
     */
    @PutMapping("/updateedgeprop")
    public RetResult updateEdgeProp(@RequestBody String requestBody) {
        JSONObject body = JSONObject.parseObject(requestBody);
        String graphName = body.getString("graphName");
        String edgeId = "";
        if (body.containsKey("edgeId")) {
            // contains if vis edit, unless form edit
            edgeId = body.getString("edgeId");
        }
        String start = "", end = "", relationship = "";
        if (body.containsKey("start") && body.containsKey("end")) {
            // contains if form edit, unless vis edit
            start = body.getString("start");
            end = body.getString("end");
            relationship = body.getString("relationship");
        }
        String edgeLabel = body.getString("edgeLabel");
        JSONArray propertyList = body.getJSONArray("property");
        JSONObject property = new JSONObject();
        JSONObject labelInfo = TugraphUtil.listEdgeLabelInfo(graphName,edgeLabel);
        for (Object prop :
                propertyList) {
            JSONObject p = JSONObject.parseObject(prop.toString());
            String dataType = labelInfo.getJSONObject(p.getString("key")).getString("type");
            if (dataType.equalsIgnoreCase("string")) {
                property.put(p.get("key").toString(), p.getString("value"));
            } else if (dataType.equalsIgnoreCase("int32")) {
                property.put(p.get("key").toString(), p.getInteger("value"));
            } else if (dataType.equalsIgnoreCase("float")) {
                property.put(p.get("key").toString(), p.getFloat("value"));
            } else if (dataType.equalsIgnoreCase("double")) {
                property.put(p.get("key").toString(), p.getDouble("value"));
            }
        }
        try {
            HttpStatus httpStatus;
            if (edgeId.equalsIgnoreCase("")){
                // if form edit
                HttpStatus deleteStatus = TugraphUtil.deleteEdge(graphName, relationship, relationship, start, end);
                if (deleteStatus.is2xxSuccessful()) {
                    httpStatus = TugraphUtil.addEdge(graphName, relationship, null, start, null, end, propertyList);
                } else {
                    return new RetResult(400, "update failed");
                }
            } else {
                // if vis edit
                httpStatus = TugraphUtil.updateEdge(graphName, edgeId, property);
            }
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "edge updated");
        } catch (Exception e) {
            e.printStackTrace();
            return new RetResult(400, "update failed");
        }
        return new RetResult(400, "update failed");
    }
    @PostMapping("/importedges")
    public RetResult importEdges(String graphName, MultipartFile file) {
        try {
            String type = file.getContentType();
            assert type != null;
            if (!file.getContentType().equals("text/csv")) {
                return new RetResult(400, "wrong file type");
            }
            String name = file.getOriginalFilename();
            String label = FilenameUtils.removeExtension(name);
            InputStream in = file.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String[] f = br.readLine().split(",");
            JSONArray fields = new JSONArray();
            fields.addAll(Arrays.asList(f));
            // remove src and dst and their label fields
            fields.remove(0);
            fields.remove(0);
            fields.remove(0);
            fields.remove(0);
            JSONObject labelInfo = TugraphUtil.listEdgeLabelInfo(graphName,label);
            JSONArray edges = new JSONArray();
            String line;
            do {
                line = br.readLine();
                if (line != null) {
                    String[] e = line.split(",");
                    // e: source, destination, source_label, destination_label, description, is_concrete
                    JSONObject edge = new JSONObject();
                    JSONArray values = new JSONArray();
                    for (int fieldCount = 0; fieldCount < e.length; fieldCount++) {
//                        JSONObject edgeSchema = JSONObject.parseObject(TugraphUtil.getEdgeSchema(graphName,label).getJSONArray(0).getString(0)); JSONArray constraints = edgeSchema.getJSONArray("constraints");
                        if (fieldCount == 0) {
                            // constraints可能有多个，不对应则找不到
                            edge.put("source", TugraphUtil.getGraphNodeIdByName(graphName, e[fieldCount+2], e[fieldCount]));
                        } else if (fieldCount == 1) {
                            // 改成csv中指定的label
                            edge.put("destination", TugraphUtil.getGraphNodeIdByName(graphName, e[fieldCount+2], e[fieldCount]));
                        } else if (fieldCount > 3){
                            String dataType = labelInfo.getJSONObject(fields.getString(fieldCount-4)).getString("type");
                            if (dataType.equalsIgnoreCase("string")) {
                                values.add(e[fieldCount]);
                            } else if (dataType.equalsIgnoreCase("int32")) {
                                values.add(Integer.parseInt(e[fieldCount]));
                            } else if (dataType.equalsIgnoreCase("float")) {
                                values.add(Float.parseFloat(e[fieldCount]));
                            } else if (dataType.equalsIgnoreCase("double")) {
                                values.add(Double.parseDouble(e[fieldCount]));
                            }
                        }
                    }
                    edge.put("values",values);
                    edges.add(edge);
                }
            }
            while (line != null);
            br.close();
            HttpStatus httpStatus = TugraphUtil.addEdgesBatch(graphName, label, fields, edges);
            if (httpStatus.is2xxSuccessful()) {
                return new RetResult(200, "edges imported");
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new RetResult(400, "edges import failed");
        }
        return new RetResult(400, "edges import failed");
    }
    @GetMapping("/downloadedgestemplate")
    public void downloadEdgesTemplate(HttpServletResponse response) {
        OutputStream out;
        BufferedWriter bw = null;
        try {
            response.setCharacterEncoding("UTF-8");
            response.setContentType("multipart/form-data");
            response.setHeader("Content-Disposition", "attachment;filename="+URLEncoder.encode("edge_template.csv","UTF-8"));
            response.addHeader("filetype", "text/csv");
            out = response.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(out));
            bw.write("source,destination,source_label,destination_label,attr1,attr2...");
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert bw != null;
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    @GetMapping("/downloadtemplatereadme")
    public void downloadTemplateReadme(HttpServletResponse response) {
        OutputStream out;
        BufferedWriter bw = null;
        try {
            response.setCharacterEncoding("UTF-8");
            response.setContentType("multipart/form-data");
            response.setHeader("Content-Disposition", "attachment;filename="+URLEncoder.encode("README.txt","UTF-8"));
            response.addHeader("filetype", "text/plain");
            out = response.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(out));
            bw.write("模板说明：\n" +
                    "1. 请使用UTF-8编码格式\n" +
                    "2. 请使用逗号分隔\n" +
                    "3. 文件名为实体/关系的标签名，如\"学生.csv\"，或\"就读于.csv\"\n" +
                    "4. 对于实体，表头第一行里一列固定为\"name\"，第二列开始是这类实体的所有属性名称，如\"age\"，\"gender\"等\n" +
                    "5. 对于关系，表头第一行里一列固定为\"source\"，第二列固定为\"destination\"，第三列固定为\"source_label\"，第四列固定为\"destination_label\"，分别是起点、终点实体的名称和标签，第五列开始是这类关系的所有属性名称，如\"role\"，\"start_time\"等\n" +
                    "6. 从第二行开始，每一行是一个实体/关系实例，实体的属性值和关系的属性值按照表头中的属性名称顺序排列\n" +
                    "7. 实体/关系的属性值类型必须与图模型中定义的类型一致，否则导入会失败\n" +
                    "8. 数值类型的属性值不能为空，否则导入会失败，请使用0\n" +
                    "9. 可以使用空格，但不要使用引号和英文逗号" +
                    "模板样例：\n" +
                    "学生.csv\n" +
                    "name,age,gender\n" +
                    "张三,18,男\n" +
                    "李四,19,女\n" +
                    "就读于.csv\n" +
                    "source,destination,source_label,destination_label,role,start_time\n" +
                    "张三,北邮,学生,学校,本科生,2018-09-01\n" +
                    "李四,清华,学生,学校,研究生,2020-09-01");
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert bw != null;
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
