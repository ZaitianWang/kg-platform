package com.bupt.kgplatform.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bupt.kgplatform.common.RestTemplateUtils;
import com.bupt.kgplatform.common.RetResult;
import org.apache.commons.io.FilenameUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/kgplatform/kgdatasets")
public class KgDatasetsController {

    @GetMapping("/getdatasetlist")
    public static RetResult getDatasetList() {
        JSONArray result = new JSONArray();
        JSONArray datasetList = DatasetUtil.getDatasetsList();
        for (Object dataset :
                datasetList) {
            JSONObject datasetInfo = JSONObject.parseObject(JSONArray.parseArray(dataset.toString()).getString(0));
            JSONObject datasetProp = datasetInfo.getJSONObject("properties");
            JSONObject resultEntry = new JSONObject();
            resultEntry.put("identity", datasetInfo.get("identity"));
            resultEntry.put("name", datasetProp.getString("name"));
            resultEntry.put("type", datasetProp.getString("type"));
            resultEntry.put("format", datasetProp.getString("format"));
            resultEntry.put("instance", datasetProp.getString("instance"));
            resultEntry.put("index", datasetProp.getString("index"));
            resultEntry.put("file", datasetProp.getString("file"));
            result.add(resultEntry);
        }
        return new RetResult(200, result);
    }
    @DeleteMapping("/deletedataset")
    public static RetResult deleteDataset(String datasetId) {
        try {
            HttpStatus httpStatus = DatasetUtil.deleteDataset(datasetId);
            if (httpStatus.is2xxSuccessful())
                return new RetResult(200, "dataset deleted");
        } catch (Exception e) {
            e.printStackTrace();
            return new RetResult(400, e.getMessage());
        }
        return new RetResult(400, "delete failed");
    }
    @PostMapping("/importdatasets")
    public static RetResult importDatasets(String name, String type, String instance, String index, MultipartFile upFile) {

        String file = upFile.getOriginalFilename(); // label.csv
        String format = FilenameUtils.getExtension(file); //csv
        Integer fileId = DatasetUtil.addDataset(name, type, file, format, instance, index);
        try {
            InputStream in = upFile.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String head = br.readLine();
            JSONArray lines = new JSONArray();
            String line;
            do {
                line = br.readLine();
                if (line != null)
                    lines.add(line);
            }
            while (line != null);
            HttpStatus httpStatus = DatasetUtil.addDatasetContent(fileId, name, head, lines);
            if (httpStatus.is2xxSuccessful()) {
                return new RetResult(200, "dataset imported");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new RetResult(400, e.getMessage());
        }
        return new RetResult(400, "dataset import failed");
    }
    @GetMapping("/getdatasetcontent")
    public static RetResult getDatasetContent(String datasetId) {
        JSONObject result = new JSONObject();
        String head = "";
        StringBuilder content = new StringBuilder();
        StringBuilder lines = new StringBuilder();
        JSONArray res = DatasetUtil.getDatasetContent(datasetId);
        for (Object r: res) {
            JSONObject filePartition = JSONObject.parseObject(JSONArray.parseArray(r.toString()).getString(0));
            head = filePartition.getJSONObject("properties").getString("head");
            lines.append(filePartition.getJSONObject("properties").getString("five_lines"));
        }
        content.append(head).append(lines);
        result.put("content", content);
        return new RetResult(200, result);
    }
    static class DatasetUtil {
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
            JSONObject body = new JSONObject();
            body.put("user", "admin");
            body.put("password", "73@TuGraph");
            JSONObject res = RestTemplateUtils.post("http://114.67.200.41:7070/login",headers,body,JSONObject.class,(Object) null).getBody();
            assert res != null;
            return res.getString("jwt");
        }
        static JSONArray getDatasetsList() {
            JSONArray result = new JSONArray();
            JSONObject reqBody = new JSONObject();
            reqBody.put("graph", "datasets");
            reqBody.put("script", "MATCH (n:file) RETURN n");
            JSONObject res = RestTemplateUtils.post("http://114.67.200.41:7070/cypher",getHeader(),reqBody,JSONObject.class).getBody();
            if (res!=null) {
                result = res.getJSONArray("result");
            }
            return result;
        }
        static HttpStatus deleteDataset(String datasetId) {
            JSONObject returnReq = new JSONObject();
            returnReq.put("graph", "datasets");
            returnReq.put("script", "MATCH (n:file) -[r:file_contains]-> (m:file_content) WHERE id(n)="+datasetId+" RETURN n, r, m");
            JSONObject returnRes = RestTemplateUtils.post("http://114.67.200.41:7070/cypher",getHeader(),returnReq,JSONObject.class).getBody();
            assert returnRes != null;
            JSONObject deleteReq = new JSONObject();
            deleteReq.put("graph", "datasets");
            if (returnRes.getInteger("size")==0) {
                // file no content, only delete file name
                deleteReq.put("script", "MATCH (n:file) WHERE id(n)="+datasetId+" DELETE n");
            } else {
                // also delete file content
                deleteReq.put("script", "MATCH (n:file) -[r:file_contains]-> (m:file_content) WHERE id(n)="+datasetId+" DELETE n, r, m");
            }
            return  RestTemplateUtils.post("http://114.67.200.41:7070/cypher",getHeader(),deleteReq,JSONObject.class).getStatusCode();
        }
        static Integer addDataset(String name, String type, String file, String format, String instance, String index) {
            JSONObject property = new JSONObject();
            property.put("name", name);
            property.put("type", type);
            property.put("file", file);
            property.put("format", format);
            property.put("instance", instance);
            property.put("index", index);
            JSONObject body = new JSONObject();
            body.put("label", "file");
            body.put("property", property);
            ResponseEntity<Integer> response = RestTemplateUtils.post("http://114.67.200.41:7070/db/"+"datasets"+"/node",getHeader(),body,Integer.class,(Object) null);
            if (!response.getStatusCode().is2xxSuccessful())
            {
                throw new RuntimeException();
            } else {
                return response.getBody();
            }
        }
        static HttpStatus addDatasetContent(Integer fileId, String name, String head, JSONArray lines) {
            // 1 file_content per 5 lines
            // an edge from file to each content
            JSONObject edgeBody = new JSONObject();
            JSONArray edge = new JSONArray();
            for (int i = 0; i < lines.size(); i+=5) {
                StringBuilder five_lines = new StringBuilder();
                for (int j = i; j < Math.min(i+5, lines.size()); j++) {
                    five_lines.append("\n").append(lines.get(j));
                }
                JSONObject property = new JSONObject();
                property.put("name", name+"-"+i/5);
                property.put("head", head);
                property.put("five_lines", five_lines);
                JSONObject body = new JSONObject();
                body.put("label", "file_content");
                body.put("property", property);
                Integer fileContentId = RestTemplateUtils.post("http://114.67.200.41:7070/db/"+"datasets"+"/node",getHeader(),body,Integer.class,(Object) null).getBody();
                JSONObject file_contains = new JSONObject();
                file_contains.put("source", fileId);
                file_contains.put("destination", fileContentId);
                file_contains.put("values", new JSONArray());
                edge.add(file_contains);
            }
            edgeBody.put("label", "file_contains");
            edgeBody.put("fields", new JSONArray());
            edgeBody.put("edge", edge);
            return RestTemplateUtils.post("http://114.67.200.41:7070/db/"+"datasets"+"/relationship",getHeader(),edgeBody,JSONArray.class,(Object) null).getStatusCode();
        }
        static JSONArray getDatasetContent(String fileId) {
            JSONObject body = new JSONObject();
            body.put("graph", "datasets");
            body.put("script", "MATCH (n:file) -[r:file_contains]-> (m:file_content) WHERE id(n)="+fileId+" RETURN m");
            JSONObject res = RestTemplateUtils.post("http://114.67.200.41:7070/cypher",getHeader(),body,JSONObject.class).getBody();
            assert res != null;
            return res.getJSONArray("result");
        }
    }
}
