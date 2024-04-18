package cn.northpark.flink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;

/**
 * 同步最新的需求到本地数据库
 * @author bruce
 * @date 2024年04月12日 16:13:45
 */
@Slf4j
public class FetchReqAndSaveData {
    private static final String URL = "http://localhost/api/req/reqRequest/queryReqRequestList";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/flink";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "123456";

    public static void main(String[] args) throws Exception{
        int pageNum = 1;
        int pageSize = 20;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            while (true) {
                String requestUrl = URL + "?pageNum=" + pageNum + "&pageSize=" + pageSize;
                String jsonResponse = sendGetRequest(requestUrl);

                JSONObject jsonObject = JSONObject.parseObject(jsonResponse);
                int total = jsonObject.getIntValue("total");
                JSONArray rows = jsonObject.getJSONArray("rows");

                if (rows.isEmpty()) {
                    break;
                }

                upsertToDatabase(conn, rows);

                if (rows.size() < pageSize) {
                    break;
                }

                pageNum++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String sendGetRequest(String urlString) throws Exception {
        StringBuilder result = new StringBuilder();
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        rd.close();
        return result.toString();
    }


    private static void upsertToDatabase(Connection conn, JSONArray rows) {
        String insertSql = "INSERT INTO requests (reqCode, reqName, reqDesc, businessPurpose, reqAuthorId, reqAuthor, reqAuthorDept, reqProjectId, reqProject, reqProductId, reqProduct, reqType, reqSource, reqSubmitTime, reqEstimatedEffort, reqExpEndTime, reqPriority, reqStatus, reqProductTypeId, reqProductTypeName, reqEvaluatorId, reqEvaluator) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String updateSql = "UPDATE requests SET reqName = ?, reqDesc = ?, businessPurpose = ?, reqAuthorId = ?, reqAuthor = ?, reqAuthorDept = ?, reqProjectId = ?, reqProject = ?, reqProductId = ?, reqProduct = ?, reqType = ?, reqSource = ?, reqSubmitTime = ?, reqEstimatedEffort = ?, reqExpEndTime = ?, reqPriority = ?, reqStatus = ?, reqProductTypeId = ?, reqProductTypeName = ?, reqEvaluatorId = ?, reqEvaluator = ? WHERE reqCode = ?";

        try (PreparedStatement insertStmt = conn.prepareStatement(insertSql);
             PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {
            for (int i = 0; i < rows.size(); i++) {
                JSONObject row = rows.getJSONObject(i);
                String reqCode = row.getString("reqCode");

                try {
                    // Check if the record with the given reqCode already exists
                    String checkSql = "SELECT COUNT(*) FROM requests WHERE reqCode = ?";
                    PreparedStatement checkStmt = conn.prepareStatement(checkSql);
                    checkStmt.setString(1, reqCode);
                    ResultSet rs = checkStmt.executeQuery();
                    rs.next();
                    int count = rs.getInt(1);
                    rs.close();
                    checkStmt.close();

                    if (count > 0) {
                        // Update the existing record
                       updateStmt.setString(1, row.getString("reqName"));
                       updateStmt.setString(2, row.getString("reqDesc"));
                       updateStmt.setString(3, row.getString("businessPurpose"));
                       updateStmt.setString(4, row.getString("reqAuthorId"));
                       updateStmt.setString(5, row.getString("reqAuthor"));
                       updateStmt.setString(6, row.getString("reqAuthorDept"));
                       updateStmt.setString(7, row.getString("reqProjectId"));
                       updateStmt.setString(8, row.getString("reqProject"));
                       updateStmt.setString(9,  row.getString("reqProductId"));
                       updateStmt.setString(10, row.getString("reqProduct"));
                       updateStmt.setString(11, row.getString("reqType"));
                       updateStmt.setString(12, row.getString("reqSource"));
                       updateStmt.setString(13, row.getString("reqSubmitTime"));
                       updateStmt.setInt   (14, row.getIntValue("reqEstimatedEffort"));
                       updateStmt.setString(15, row.getString("reqExpEndTime"));
                       updateStmt.setString(16, row.getString("reqPriority"));
                       updateStmt.setString(17, row.getString("reqStatus"));
                       updateStmt.setString(18, row.getString("reqProductTypeId"));
                       updateStmt.setString(19, row.getString("reqProductTypeName"));
                       updateStmt.setString(20, row.getString("reqEvaluatorId"));
                       updateStmt.setString(21, row.getString("reqEvaluator"));
                       updateStmt.setString(22, row.getString("reqCode"));

                       updateStmt.addBatch();
                    } else {
                        // Insert a new record
                        insertStmt.setString(1, row.getString("reqCode"));
                        insertStmt.setString(2, row.getString("reqName"));
                        insertStmt.setString(3, row.getString("reqDesc"));
                        insertStmt.setString(4, row.getString("businessPurpose"));
                        insertStmt.setString(5, row.getString("reqAuthorId"));
                        insertStmt.setString(6, row.getString("reqAuthor"));
                        insertStmt.setString(7, row.getString("reqAuthorDept"));
                        insertStmt.setString(8, row.getString("reqProjectId"));
                        insertStmt.setString(9, row.getString("reqProject"));
                        insertStmt.setString(10, row.getString("reqProductId"));
                        insertStmt.setString(11, row.getString("reqProduct"));
                        insertStmt.setString(12, row.getString("reqType"));
                        insertStmt.setString(13, row.getString("reqSource"));
                        insertStmt.setString(14, row.getString("reqSubmitTime"));
                        insertStmt.setInt(15, row.getIntValue("reqEstimatedEffort"));
                        insertStmt.setString(16, row.getString("reqExpEndTime"));
                        insertStmt.setString(17, row.getString("reqPriority"));
                        insertStmt.setString(18, row.getString("reqStatus"));
                        insertStmt.setString(19, row.getString("reqProductTypeId"));
                        insertStmt.setString(20, row.getString("reqProductTypeName"));
                        insertStmt.setString(21, row.getString("reqEvaluatorId"));
                        insertStmt.setString(22, row.getString("reqEvaluator"));
                        insertStmt.addBatch();
                    }
                } catch (SQLException e) {
                    System.out.println("Error processing record with reqCode: " + reqCode);
                    log.error("Error processing record with reqCode: " + reqCode);
                    e.printStackTrace();
                }
            }

            insertStmt.executeBatch();
            updateStmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
