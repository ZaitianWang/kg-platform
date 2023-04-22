package com.bupt.kgplatform.common;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

//通过RestTemplateUtils类创建http请求，看请RestTemplateUtils参数即可，header由此工具类getHeader生成，包含了token获取
@Component
public class TugraphUtil {
    
    private static String host;
    @Value(value = "${Tugraph-host}")
    public  void setHost(String hostName){
        host = hostName;
    }

    private static HttpHeaders getHeader(){
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.AUTHORIZATION, "Bearer " +getToken());
        headers.set(HttpHeaders.ACCEPT,"application/json; charset=UTF-8");
        headers.set(HttpHeaders.CONTENT_TYPE,"application/json");
        headers.set("server_version","-1");
        return headers;
    }

    private static String getToken(){
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.ACCEPT,"application/json; charset=UTF-8");
        headers.set(HttpHeaders.CONTENT_TYPE,"application/json");
        headers.set("server_version","-1");
        Map map= new HashMap();
        map.put("user", "admin");
        map.put("password", "73@TuGraph");

        JSONObject res = RestTemplateUtils.post(host+"/login",headers,JSONObject.toJSONString(map),JSONObject.class,(Object) null).getBody();
        String token = (String) res.get("jwt");
        return token;
    }


    /**
     * 列出所有子图
     * @return JSON Object in form: {kg1:{desc, max_size_GB}, kg2:{}, ...}
     */
    public static JSONObject listSubgraph(){
        return RestTemplateUtils.get(host+"/db",getHeader(),JSONObject.class, (Object) null).getBody();
    }

    public static HttpStatus addSubGraph(String name, String description){
        JSONObject json = new JSONObject();
        json.put("name",name);
        JSONObject config = new JSONObject();
        config.put("max_size_GB",256);
        config.put("description",description);
        json.put("config",config);
        return RestTemplateUtils.post(host+"/db",getHeader(),json,JSONObject.class,(Object) null).getStatusCode();
    }

    public static HttpStatus deleteSubGraph(String name){
        return RestTemplateUtils.delete(host+"/db/"+name,getHeader(),JSONObject.class,(Object) null).getStatusCode();
    }

    /**
     * 列出所有标签
     */
    public static JSONObject listGraphLabel(String graphName){
        return RestTemplateUtils.get(host+"/db/"+graphName+"/label",getHeader(),JSONObject.class, (Object) null).getBody();
    }

    /** 列出单个边标签的数据格式 
     */
    public static JSONObject listEdgeLabelInfo(String graphName, String edgeName){
        return RestTemplateUtils.get(host+"/db/"+graphName+"/label/relationship/"+edgeName,getHeader(),JSONObject.class, (Object) null).getBody();
    }

    /** 列出单个节点标签的数据格式 
     */
    public static JSONObject listNodeLabelInfo(String graphName, String nodeName){
        return RestTemplateUtils.get(host+"/db/"+graphName+"/label/node/"+nodeName,getHeader(),JSONObject.class, (Object) null).getBody();
    }

    /**
     * 获取edge的schema，主要是constraint（src and dst nodes），还有properties
     * @param graphName 图名
     * @param edgeLabel 边标签名
     * @return result:[["{constraints, label, properties, type}"]]
     */
    public static JSONArray getEdgeSchema(String graphName, String edgeLabel) {
        JSONObject body = new JSONObject();
        body.put("graph", graphName);
        body.put("script", "CALL db.getEdgeSchema('"+edgeLabel+"')");
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),body,JSONObject.class,(Object) null).getBody();
        assert res !=null;
        return res.getJSONArray("result");
    }

    /**
     * 获取Vertex的schema
     * @param graphName 图名
     * @param vertexLabel 点标签名
     * @return result:[["{primary(主键), label, properties, type}"]]
     */
    public static JSONArray getVertexSchema(String graphName, String vertexLabel) {
        JSONObject body = new JSONObject();
        body.put("graph", graphName);
        body.put("script", "CALL db.getVertexSchema('"+vertexLabel+"')");
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),body,JSONObject.class,(Object) null).getBody();
        assert res !=null;
        return res.getJSONArray("result");
    }
    /** 创建标签
     */
    public static HttpStatus addNodeLabel(String graphName, String nodeName, com.alibaba.fastjson.JSONArray properties, String primary){
        JSONObject json = new JSONObject();
        json.put("name", nodeName);
        json.put("fields", properties);
        json.put("is_vertex", true);
        json.put("primary", primary);
        return RestTemplateUtils.post(host+"/db/"+graphName+"/label",getHeader(),json,JSONObject.class,(Object) null).getStatusCode();
    }

    public static HttpStatus addEdgeLabel(String graphName, String edgeName, JSONArray fields, String startNode, String endNode){
        JSONObject json = new JSONObject();
        json.put("name", edgeName);
        json.put("fields", fields);
        json.put("is_vertex", false);
        ArrayList edge_constraints = new ArrayList();
        ArrayList edge_constraint = new ArrayList();
        edge_constraint.add(startNode);
        edge_constraint.add(endNode);
        edge_constraints.add(edge_constraint);
        json.put("edge_constraints", edge_constraints);
        return RestTemplateUtils.post(host+"/db/"+graphName+"/label",getHeader(),json,JSONObject.class,(Object) null).getStatusCode();
    }

    /** 删除标签
     */

    public static HttpStatus deleteEdgeLabel(String graphName, String edgeName){
        JSONObject json = new JSONObject();
        json.put("graph", graphName);
        json.put("script", "CALL db.deleteLabel('edge', '"+edgeName+"')");
        return RestTemplateUtils.post(host+"/cypher",getHeader(),json,JSONObject.class,(Object) null).getStatusCode();
    }

    public static HttpStatus deleteNodeLabel(String graphName, String nodeName){
        JSONObject json = new JSONObject();
        json.put("graph", graphName);
        json.put("script", "CALL db.deleteLabel('vertex', '"+nodeName+"')");
        return RestTemplateUtils.post(host+"/cypher",getHeader(),json,JSONObject.class,(Object) null).getStatusCode();
    }

    public static HttpStatus importSchema(String graphName, String schema){
        JSONObject graphList = listSubgraph();
        if(!graphList.containsKey(graphName)){
            addSubGraph(graphName, "imported from file");
        }
        JSONObject json = new JSONObject();
        json.put("description", schema);
        return RestTemplateUtils.post(host+"/db/"+graphName+"/schema/text",getHeader(),json,JSONObject.class,(Object) null).getStatusCode();
    }

    /**
     * 列出顶点数量和 label 数量，只是一个估计值
     * @author zaitian
     * @param graphName the kg to be searched
     * @return JSON Object in form: {"num_label": 5,"num_vertex": 4225}
     */
    public static JSONObject getGraphNodeRoughCount(String graphName) {
        return RestTemplateUtils.get(host+"/db/"+graphName+"/node",getHeader(),JSONObject.class, (Object) null).getBody();
    }

    /**
     * 列出节点数量，使用cypher count
     * @author zaitian
     * @param graphName the kg to be searched
     * @param label filter for label, null or empty if not given
     * @param name filter for name, null or empty if not given
     * @return 节点数量，整型
     */
    public static int getGraphNodeCount(String graphName, String label, String name) {
        String labelCond = "", nameCond = "";
        if (label!=null && !label.trim().isEmpty()) {
            labelCond  = ":"+label;
        }
        if (name!=null  && !name.trim().isEmpty()) {
            nameCond = "WHERE n.name CONTAINS '"+name+"' ";
        }
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH (n"+labelCond+") "+nameCond+"RETURN count(n)");
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        assert res != null;
        if (res.getJSONArray("result").size() == 0)
            return 0;
        return res.getJSONArray("result").getJSONArray(0).getInteger(0);
    }
    /**
     * 通过节点ID获得单个节点实例，调用Restful API
     * @author zaitian
     * @param graphName name of the kg to be searched
     * @param vertex_id id of the vertex/node of the kg to be searched
     * @return JSON Object in form: {label:"", property:{name:"", desc:"", ...}}
     */
    public static JSONObject getGraphNodeInstanceById(String graphName, int vertex_id) {
        JSONObject noThisNodeRes = new JSONObject();
        noThisNodeRes.put("error_message", "Vertex does not exist.");
        try {
            ResponseEntity<JSONObject> response
                    = RestTemplateUtils.get(host+"/db/"+graphName+"/node/"+vertex_id,getHeader(),JSONObject.class, (Object) null);
            if (response.getStatusCode().is2xxSuccessful()) {
                return response.getBody();
            }
        } catch (Exception e){
            return noThisNodeRes;
        }
        return noThisNodeRes;
//        return RestTemplateUtils.get(host+"/db/"+graphName+"/node/"+vertex_id,getHeader(),JSONObject.class, (Object) null).getBody();
    }

    /**
     * 通过node name获取id，用于导入edge时把csv里的name换成api要的id
     * @param graphName 图名
     * @param nodeName 节点名
     * @return 节点id
     * @deprecated label not required, unsafe
     * @see #getGraphNodeIdByName(String, String, String)
     */
    public static Integer getGraphNodeIdByName(String graphName, String nodeName) {
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH (n"+"{name:'"+nodeName+"'}"+") RETURN n");
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        assert res != null;
        JSONArray result = res.getJSONArray("result");
        String node = result.getJSONArray(0).getString(0);
        return JSONObject.parseObject(node).getInteger("identity");
    }
    /**
     * 通过node name获取id，用于导入edge时把csv里的name换成api要的id
     * @param graphName 图名
     * @param label 标签名
     * @param nodeName 节点名
     * @return 节点id
     */
    public static Integer getGraphNodeIdByName(String graphName, String label, String nodeName) {
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH (n:"+label+"{name:'"+nodeName+"'}"+") RETURN n");
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        assert res != null;
        JSONArray result = res.getJSONArray("result");
        String node = result.getJSONArray(0).getString(0);
        return JSONObject.parseObject(node).getInteger("identity");
    }
    // TODO: fault tolerance if node not found
    /**
     * 获取所有节点实例，使用cypher，result中的[""]需要parse string
     * @author zaitian
     * @param graphName the kg to be searched
     * @return JSON Object in form: { size, result:[ [""], [""], ... ] }
     */
    public static JSONObject getGraphNodesAllWithCypher(String graphName) {
        JSONObject result = new JSONObject();
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH (n) RETURN n");
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        if (res!=null) {
            result.put("result", res.get("result"));
            result.put("resultSize", res.get("size"));
        }
        return result;
    }

    /**
     * 按页获取节点实例，使用cypher
     * @author zaitian
     * @param graphName the kg to be searched
     * @param page starts from 1
     * @param size page size, recommended: 10
     * @param label filter for label, null or empty if not given
     * @param name filter for name, null or empty if not given
     * @return JSON Array in form: [ [""], [""], ... ]，其中[""]需要parseObject
     */
    public static JSONArray getGraphNodesAsPagedWithCypher(String graphName, int page, int size, String label, String name) {
        String labelCond = "", nameCond = "";
        if (label!=null && !label.trim().isEmpty()) {
            labelCond  = ":"+label;
        }
        if (name!=null  && !name.trim().isEmpty()) {
            nameCond = "WHERE n.name CONTAINS '"+name+"' ";
        }
        JSONArray result = new JSONArray();
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH (n"+labelCond+") "+nameCond+"RETURN n SKIP "+ (page-1)*size +" LIMIT " + size);
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        if (res!=null) {
            result = res.getJSONArray("result");
        }
        return result;
    }
    /**
     * 列出边数量，使用cypher count
     * @author zaitian
     * @param graphName the kg to be searched
     * @param label filter for label, null or empty if not given
     * @return 边数量，整型
     */
    public static int getGraphEdgeCount(String graphName, String label) {
        String labelCond = "";
        if (label!=null && !label.trim().isEmpty()) {
            labelCond  = ":"+label;
        }
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH ()-[r"+labelCond+"]->() RETURN count(r)");
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        assert res != null;
        if (res.getJSONArray("result").size() == 0)
            return 0;
        return res.getJSONArray("result").getJSONArray(0).getInteger(0);
    }
    /**
     * 获取所有关系实例，使用cypher，result中的[""]需要parse string
     * @author zaitian
     * @param graphName the kg to be searched
     * @return JSON Object in form: { size, result:[ [""], [""], ... ] }
     */
    public static JSONObject getGraphEdgesAll(String graphName) {
        JSONObject result = new JSONObject();
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH ()-[r]->() RETURN r");
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        if (res!=null) {
            result.put("result", res.get("result"));
            result.put("resultSize", res.get("size"));
        }
        return result;
    }

    /**
     * 按页获取边实例，使用cypher
     * @author zaitian
     * @param graphName the kg to be searched
     * @param page starts from 1
     * @param size page size, recommended: 10
     * @return JSON Object in form: { size, result:[ [""], [""], ... ] }
     */
    public static JSONArray getGraphEdgesAsPaged(String graphName, int page, int size, String label) {
        String labelCond = "";
        if (label!=null && !label.trim().isEmpty()) {
            labelCond  = ":"+label;
        }
        JSONArray result = new JSONArray();
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH ()-[r"+labelCond+"]->() RETURN r SKIP "+ (page-1)*size +" LIMIT " + size);
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        if (res!=null) {
            result = res.getJSONArray("result");
        }
        return result;
    }
    public static JSONArray getOutgoingEdges(String graphName, String nodeId) {
        return RestTemplateUtils.get(host+"/db/"+graphName+"/node/"+nodeId+"/relationship/out",getHeader(),JSONArray.class, (Object) null).getBody();
    }
    public static JSONArray getIncomingEdges(String graphName, String nodeId) {
        return RestTemplateUtils.get(host+"/db/"+graphName+"/node/"+nodeId+"/relationship/in",getHeader(),JSONArray.class, (Object) null).getBody();
    }
    /**
     * 添加节点。
     * 注：因为tugraph添加节点的返回值是一个int（节点id），
     * 所以post()的response type需要改为Integer，否则能添加但抛异常
     * @author zaitian
     * @param graphName 图名
     * @param name 节点名
     * @param label 节点类型标签
     * @param description 节点描述
     * @return 200
     * @deprecated
     */
    public static HttpStatus addNode(String graphName, String name, String label, String description) {
        JSONObject property = new JSONObject();
        property.put("name", name);
        if (description != null)
            property.put("description", description);
        JSONObject body = new JSONObject();
        body.put("label", label);
        body.put("property", property);
        return RestTemplateUtils.post(host+"/db/"+graphName+"/node",getHeader(),body,Integer.class,(Object) null).getStatusCode();
    }
    /**
     * description should not be a must
     * @author zaitian
     * @param graphName 图名
     * @param name 节点名
     * @param label 节点类型标签
     * @return 200
     */
    public static HttpStatus addNode(String graphName, String name, String label) {
        JSONObject property = new JSONObject();
        property.put("name", name);
        JSONObject body = new JSONObject();
        body.put("label", label);
        body.put("property", property);
        return RestTemplateUtils.post(host+"/db/"+graphName+"/node",getHeader(),body,Integer.class,(Object) null).getStatusCode();
    }

    /**
     * 批量添加node
     * @param graphName 图名
     * @param label 批节点标签
     * @param fields 属性字段列表，["",""]
     * @param values 所有节点属性值列表，[["",""],["",""]]
     * @return 200
     */
    public static HttpStatus addNodesBatch(String graphName, String label, JSONArray fields, JSONArray values) {
        JSONObject body = new JSONObject();
        body.put("label", label);
        body.put("fields", fields);
        body.put("values", values);
        return RestTemplateUtils.post(host+"/db/"+graphName+"/node",getHeader(),body, JSONArray.class,(Object) null).getStatusCode();
    }
    /**
     * 删除节点
     * @author zaitian
     * @param graphName 图名
     * @param nodeId 节点id
     * @return 200
     */
    public static HttpStatus deleteNode(String graphName, String nodeId) {
        return RestTemplateUtils.delete(host+"/db/"+graphName+"/node/"+nodeId,getHeader(),JSONObject.class,(Object) null).getStatusCode();
    }

    /**
     * update node attributes/properties
     * @author zaitian
     * @param graphName graph to update
     * @param nodeId target node
     * @param property a json object of name property, {"attr1":"", "attr2":""}, implicit k-v
     * @return 200
     */
    public static HttpStatus updateNode(String graphName, String nodeId, JSONObject property) {
        JSONObject body = new JSONObject();
        body.put("property", property);
        return RestTemplateUtils.put(host+"/db/"+graphName+"/node/"+nodeId,getHeader(),body,null,(Object) null).getStatusCode();
    }

    /**
     * delete an edge
     * @param graphName 图名
     * @param relationship 关系名称
     * @param start 起点实体名称
     * @param end 重点实体名称
     * @return 200
     * @deprecated label not required, unsafe
     * @see #deleteEdge(String, String, String, String, String)
     */
    public static HttpStatus deleteEdge(String graphName, String relationship, String start, String end) {
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH (n {name: '"+start+"'})-[r:"+relationship+"]->(m {name:'"+end+"'}) DELETE r");
        return RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getStatusCode();
    }
   /**
     * delete an edge
     * @param graphName 图名
     * @param relationship 关系名称
     * @param startLabel 起点标签
     * @param start 起始节点
     * @param endLabel 终点标签
     * @param end 重点实体名称
     * @return 200
     * @deprecated node id not required, edge may not be found
     * @see #deleteEdge(String, String, String, String, String)
     */
    public static HttpStatus deleteEdge(String graphName, String relationship, String startLabel, String start, String endLabel, String end) {
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH (n:"+startLabel+" {name: '"+start+"'})-[r:"+relationship+"]->(m:"+endLabel+" {name:'"+end+"'}) DELETE r");
        return RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getStatusCode();
    }

    /**
     * delete an edge by nodes' ID
     * @param graphName 图名
     * @param relationship 关系名称
     * @param startId 起始节点
     * @param endId 终止节点
     * @return 200
     */
    public static HttpStatus deleteEdge(String graphName, String relationshipName, String relationship, String startId, String endId) {
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH (n)-[r:"+relationship+"]->(m) WHERE id(n)="+startId+" AND id(m)="+endId+" DELETE r");
        return RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getStatusCode();
    }
    public static HttpStatus deleteEdge(String graphName, String edgeId) {
        return RestTemplateUtils.delete(host+"/db/"+graphName+"/relationship/"+edgeId,getHeader(),JSONObject.class,(Object) null).getStatusCode();
    }

    /**
     * 添加一条边，不带属性（用于新建）
     * @param graphName 图名
     * @param name 边名
     * @param start 起始节点
     * @param end 终点节点
     * @return 200
     * @deprecated label not required, unsafe
     * @see #addEdge(String, String, String, String, String, String)
     */
    public static HttpStatus addEdge(String graphName, String name, String start, String end) {
        return addEdge(graphName, name, start, end, null);
    }

    /**
     * 添加一条边，包含属性（用于更改时先删再加）
     * @param graphName 图名
     * @param name 边名
     * @param start 起始节点
     * @param end 终点节点
     * @param propertyList 属性列表，格式:[ {"key":"","value":""}, ...], explicitly declare k-v
     * @return 200
     * @deprecated label not required, unsafe
     * @see #addEdge(String, String, String, String, String, String, JSONArray)
     */
    public static HttpStatus addEdge(String graphName, String name, String start, String end, JSONArray propertyList) {
        StringBuilder edgeProperty = new StringBuilder();
        JSONObject labelInfo = TugraphUtil.listEdgeLabelInfo(graphName,name);
        if (propertyList != null) {
            edgeProperty.append(" {");
            for (int i = 0; i < propertyList.size(); i++) {
                JSONObject entry = JSONObject.parseObject(propertyList.get(i).toString());
                edgeProperty.append(entry.get("key").toString());
                String dataType = labelInfo.getJSONObject(entry.getString("key")).getString("type");
                if (dataType.equalsIgnoreCase("string")) {
                    edgeProperty.append(":'");
                } else {
                    edgeProperty.append(":");
                }
                edgeProperty.append(entry.getOrDefault("value","").toString());
                if (i == propertyList.size() - 1) {
                    if (dataType.equalsIgnoreCase("string")) {
                        edgeProperty.append("'}");
                    } else {
                        edgeProperty.append("}");
                    }
                } else {
                    if (dataType.equalsIgnoreCase("string")) {
                        edgeProperty.append("',");
                    } else {
                        edgeProperty.append(",");
                    }
                }
            }
        }
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        reqBody.put("script", "MATCH (n {name: '"+start+"'}), (m {name:'"+end+"'}) CREATE (n)-[r:"+name+edgeProperty+"]->(m) RETURN type(r)");
        ResponseEntity<JSONObject> response = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class);
        HttpStatus httpStatus = response.getStatusCode();
        if (httpStatus.is2xxSuccessful()) {
            JSONObject resBody = response.getBody();
            assert resBody != null;
            if (resBody.getInteger("size") > 0) {
                return httpStatus;
            } else {
                return HttpStatus.valueOf(400);
            }
        }
        else {
            return HttpStatus.valueOf(400);
        }
    }

    /**
     * 添加一条边，不带属性（用于新建）
     * @param graphName 图名
     * @param name 边名
     * @param startLabel 起点标签
     * @param start 起始节点
     * @param endLabel 终点标签
     * @param end 终点节点
     * @return 200
     */
    public static HttpStatus addEdge(String graphName, String name, String startLabel, String start, String endLabel, String end) {
        return addEdge(graphName, name, startLabel, start, endLabel,end, null);
    }

    /**
     * 添加一条边，包含属性（用于更改时先删再加）
     * @param graphName 图名
     * @param name 边名
     * @param startLabel 起点标签
     * @param start 起始节点
     * @param endLabel 终点标签
     * @param end 终点节点
     * @param propertyList 属性列表，格式:[ {"key":"","value":""}, ...], explicitly declare k-v
     * @return 200
     */
    public static HttpStatus addEdge(String graphName, String name, String startLabel, String start, String endLabel, String end, JSONArray propertyList) {
        StringBuilder edgeProperty = new StringBuilder();
        JSONObject labelInfo = TugraphUtil.listEdgeLabelInfo(graphName,name);
        if (propertyList != null) {
            edgeProperty.append(" {");
            for (int i = 0; i < propertyList.size(); i++) {
                JSONObject entry = JSONObject.parseObject(propertyList.get(i).toString());
                edgeProperty.append(entry.get("key").toString());
                String dataType = labelInfo.getJSONObject(entry.getString("key")).getString("type");
                if (dataType.equalsIgnoreCase("string")) {
                    edgeProperty.append(":'");
                } else {
                    edgeProperty.append(":");
                }
                edgeProperty.append(entry.getOrDefault("value","").toString());
                if (i == propertyList.size() - 1) {
                    if (dataType.equalsIgnoreCase("string")) {
                        edgeProperty.append("'}");
                    } else {
                        edgeProperty.append("}");
                    }
                } else {
                    if (dataType.equalsIgnoreCase("string")) {
                        edgeProperty.append("',");
                    } else {
                        edgeProperty.append(",");
                    }
                }
            }
        }
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        if (startLabel!=null || endLabel!=null) {
            reqBody.put("script", "MATCH (n:" + startLabel + " {name: '" + start + "'}), (m:" + endLabel + " {name:'" + end + "'}) CREATE (n)-[r:" + name + edgeProperty + "]->(m) RETURN type(r)");
        } else {
            reqBody.put("script", "MATCH (n), (m) WHERE id(n)="+start+" AND id(m)="+end+" CREATE (n)-[r:" + name + edgeProperty + "]->(m) RETURN type(r)");
        }
        ResponseEntity<JSONObject> response = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class);
        HttpStatus httpStatus = response.getStatusCode();
        if (httpStatus.is2xxSuccessful()) {
            JSONObject resBody = response.getBody();
            assert resBody != null;
            if (resBody.getInteger("size") > 0) {
                return httpStatus;
            } else {
                return HttpStatus.valueOf(400);
            }
        }
        else {
            return HttpStatus.valueOf(400);
        }
    }

    /**
     * 批量创建edge
     * @param graphName 图名
     * @param label edge名
     * @param fields 属性字段列表，["",""]
     * @param edge 所有节点拓扑及属性列表，[{source:int,destination:int,values:[]},...]
     * @return 200
     */
    public static HttpStatus addEdgesBatch(String graphName, String label, JSONArray fields, JSONArray edge) {
        JSONObject body = new JSONObject();
        body.put("label", label);
        body.put("fields", fields);
        body.put("edge", edge);
        return RestTemplateUtils.post(host+"/db/"+graphName+"/relationship",getHeader(),body, JSONArray.class,(Object) null).getStatusCode();
    }

    /**
     * 更改边的属性（不改拓扑，用于可视化编辑）
     * @param graphName 图名
     * @param edgeId 边的uid，如1_19_0_0_0，只能在添加时或用subgraph获取
     * @param property 新属性
     * @return 200
     */
    public static HttpStatus updateEdge(String graphName, String edgeId, JSONObject property) {
        JSONObject body = new JSONObject();
        body.put("property", property);
        return RestTemplateUtils.put(host+"/db/"+graphName+"/relationship/"+edgeId,getHeader(),body,null,(Object) null).getStatusCode();
    }
    /**
     * 更改边的属性（不改拓扑，用于表单编辑）
     * <b>不能用，因为TuGraph还不支持SET</b>
     * @param graphName 图名
     * @param start 边的起点，如1
     * @param end 边的终点，如19
     * @param property 新属性
     * @return 200
     */
    public static HttpStatus updateEdge(String graphName, String start, String end, String edgeLabel, JSONObject property) {
        JSONObject body = new JSONObject();
        body.put("graph", graphName);
        String script = "MATCH (n)-[r:"+edgeLabel+"]->(m) WHERE id(n)="+start+" AND id(m)="+end+" SET r="+property.toJSONString()+" RETURN r";
        // property.toString may not be precise
        body.put("script", script);
        body.put("property", property);
        return RestTemplateUtils.put(host+"/cypher/",getHeader(),body,null,(Object) null).getStatusCode();
    }
    /**
     * 获取节点数组的最小子图
     * @param IDArray 点击ID JSON数组
     * @param graphName 点击ID JSON数组
     * @return JSON Object in form: { size, result:[ [""], [""], ... ] }
     */
    public static JSONObject getSubGraphDetails(String graphName,JSONArray IDArray) {

        JSONObject json = new JSONObject();
        json.put("vertex_ids",IDArray);

        return RestTemplateUtils.post(host+"/db/"+graphName+"/misc/sub_graph",getHeader(),json,JSONObject.class,(Object) null).getBody();
    }

    //根据头节点、关系类型(可选) 获取与之相连一跳的所有节点，使用cypher，result中的[""]需要parse string
    public static JSONObject getsubGraphNodesByHeadNode(String graphName,String nodename,String edgelabel) {
        JSONObject result = new JSONObject();
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        if(edgelabel!=null)
            reqBody.put("script", "MATCH (n {name:'"+nodename+"'})-[:"+edgelabel+"]->(m) RETURN n,m");
        else
            reqBody.put("script", "MATCH (n {name:'"+nodename+"'})-[]->(m) RETURN n,m");
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        if (res!=null) {
            result.put("result", res.get("result"));
            result.put("resultSize", res.get("size"));
        }
        return result;
    }

    //根据尾节点获取与之相连一跳的所有节点，使用cypher，result中的[""]需要parse string
    public static JSONObject getsubGraphNodesByTailNode(String graphName,String nodename,String edgelabel) {
        JSONObject result = new JSONObject();
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        if(edgelabel!=null)
            reqBody.put("script", "MATCH (n)-[:"+edgelabel+"]->(m {name:'" +nodename+"'}) RETURN n,m");
        else
            reqBody.put("script", "MATCH (n)-[]->(m {name:'" +nodename+"'}) RETURN n,m");
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        if (res!=null) {
            result.put("result", res.get("result"));
            result.put("resultSize", res.get("size"));
        }
        return result;
    }

    /** 根据节点名获取节点的邻居节点
     * @author zaitian
     * @param graphName 图名
     * @param nodeName 节点名
     * @param relationName 第一跳的关系名，可选（null或""则不限制）
     * @param maxHop 最大跳数，可选，范围[1,5]
     * @return 节点的任意跳邻居节点
     */
    public static JSONObject getNodeNeighbours(String graphName, String nodeName, String relationName, int maxHop) {
        JSONObject result = new JSONObject();
        JSONObject reqBody = new JSONObject();
        reqBody.put("graph", graphName);
        if (relationName == null || relationName.isEmpty()) {
            // 节点+最大跳数，如“A的相关信息”
            reqBody.put("script", "MATCH (n {name:'"+nodeName+"'})-[r*1.."+maxHop+"]-(m) RETURN n,m");
        } else if (maxHop == 1) {
            // 节点+关系类型（一跳），如“A拍的电影”
            reqBody.put("script", "MATCH (n {name:'" + nodeName + "'})-[:" + relationName + "]-(m) RETURN n,m");
        }
        else {
            // 节点+第一跳关系类型+最大跳数，如“A拍的电影的相关信息”
            reqBody.put("script", "MATCH (n {name:'" + nodeName + "'})-[:" + relationName + "]-(m)-[*1.." + (maxHop - 1) + "]-(l) RETURN n,m,l");
        }
        JSONObject res = RestTemplateUtils.post(host+"/cypher",getHeader(),reqBody,JSONObject.class).getBody();
        if (res!=null) {
            result.put("result", res.get("result"));
            result.put("resultSize", res.get("size"));
        }
        return result;
    }
}
