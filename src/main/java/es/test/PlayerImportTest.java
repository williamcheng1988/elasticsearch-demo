package es.test;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import es.ElasticSearchServiceImpl;
import es.EsData;
import org.json.JSONObject;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * User: William Cheng
 * Create Time: 2018/7/15 18:18
 * Description:

 CREATE TABLE `player` (
 `id` bigint(20) NOT NULL AUTO_INCREMENT,
 `name` varchar(255) DEFAULT NULL,
 `age` int(11) DEFAULT NULL,
 `salary` bigint(20) DEFAULT NULL,
 `team` varchar(255) DEFAULT NULL,
 `position` varchar(255) DEFAULT NULL,
 PRIMARY KEY (`id`)
 ) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4

 INSERT INTO `player` (`name`, `age`, `salary`, `team`, `position`) VALUES ('james', 23, 3000, 'cav', 'sf');
 INSERT INTO `player` (`name`, `age`, `salary`, `team`, `position`) VALUES ('irving', 25, 3000, 'cav', 'pg');
 INSERT INTO `player` (`name`, `age`, `salary`, `team`, `position`) VALUES ('curry', 29, 1000, 'war', 'pg');
 INSERT INTO `player` (`name`, `age`, `salary`, `team`, `position`) VALUES ('thompson', 26, 2000, 'war', 'sg');
 INSERT INTO `player` (`name`, `age`, `salary`, `team`, `position`) VALUES ('green', 26, 2000, 'war', 'pf');
 INSERT INTO `player` (`name`, `age`, `salary`, `team`, `position`) VALUES ('garnett', 40, 1000, 'tim', 'pf');
 INSERT INTO `player` (`name`, `age`, `salary`, `team`, `position`) VALUES ('towns', 21, 500, 'tim', 'c');
 INSERT INTO `player` (`name`, `age`, `salary`, `team`, `position`) VALUES ('lavin', 21, 300, 'tim', 'sg');
 INSERT INTO `player` (`name`, `age`, `salary`, `team`, `position`) VALUES ('wigins', 20, 500, 'tim', 'sf');

 */
public class PlayerImportTest {
    private static Connection getConn() {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://10.166.224.24:3306/es-test";
        String username = "integration";
        String password = "123456";
        Connection conn = null;
        try {
            Class.forName(driver); // classLoader,
            conn = (Connection) DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void main(String[] args) throws Exception {
        final ElasticSearchServiceImpl elasticSearchServiceImpl = new ElasticSearchServiceImpl();
        try {
            String sql ="select * from player";
            System.out.println(sql);
            final Connection connection = getConn();
            PreparedStatement pstmt;
            pstmt = (PreparedStatement) connection.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            rs.setFetchSize(100);
            ResultSetMetaData meta = rs.getMetaData();
            List<EsData> datas = new ArrayList<EsData>();
            while (rs.next()) {
                EsData data = new EsData();
                Map<String, Object> map = new HashMap<String, Object>();
                int columnCount = meta.getColumnCount();
                for (int j = 1; j <= columnCount; j++) {
                    String columnName = meta.getColumnLabel(j);
                    if (columnName.equals("id")) {
                        data.setId(rs.getObject(j).toString());		//id主要是设置主键es保留字段
                    }
                    map.put(columnName, rs.getObject(j));
                }
                map.put("timestamp", "2018-07-15T20:54:36.948Z");
                data.setData(JSONObject.valueToString(map));
                datas.add(data);
            }
            elasticSearchServiceImpl.searchBulkIn("players-normal", "players", datas);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        /*final ElasticSearchServiceImpl elasticSearchServiceImpl = new ElasticSearchServiceImpl();

        List<EsData> datas = new ArrayList<EsData>();
        EsData esData = null;
        Map<String, Object> map = null;
        map = new HashMap<String, Object>(); map.put("id",1);map.put("name","james");map.put("age",23);map.put("salary",3000);map.put("team","cav");map.put("position","sf");
        esData = new EsData(""+map.get("id"), JSONObject.valueToString(map));datas.add(esData);map = new HashMap<String, Object>(); map.put("id",2);map.put("name","irving");map.put("age",25);map.put("salary",3000);map.put("team","cav");map.put("position","pg");
        esData = new EsData(""+map.get("id"), JSONObject.valueToString(map));datas.add(esData);map = new HashMap<String, Object>(); map.put("id",3);map.put("name","curry");map.put("age",29);map.put("salary",1000);map.put("team","war");map.put("position","pg");
        esData = new EsData(""+map.get("id"), JSONObject.valueToString(map));datas.add(esData);map = new HashMap<String, Object>(); map.put("id",4);map.put("name","thompson");map.put("age",26);map.put("salary",2000);map.put("team","war");map.put("position","sg");
        esData = new EsData(""+map.get("id"), JSONObject.valueToString(map));datas.add(esData);map = new HashMap<String, Object>(); map.put("id",5);map.put("name","green");map.put("age",26);map.put("salary",2000);map.put("team","war");map.put("position","pf");
        esData = new EsData(""+map.get("id"), JSONObject.valueToString(map));datas.add(esData);map = new HashMap<String, Object>(); map.put("id",6);map.put("name","garnett");map.put("age",40);map.put("salary",1000);map.put("team","tim");map.put("position","pf");
        esData = new EsData(""+map.get("id"), JSONObject.valueToString(map));datas.add(esData);map = new HashMap<String, Object>(); map.put("id",7);map.put("name","towns");map.put("age",21);map.put("salary",500);map.put("team","tim");map.put("position","c");
        esData = new EsData(""+map.get("id"), JSONObject.valueToString(map));datas.add(esData);map = new HashMap<String, Object>(); map.put("id",8);map.put("name","lavin");map.put("age",21);map.put("salary",300);map.put("team","tim");map.put("position","sg");
        esData = new EsData(""+map.get("id"), JSONObject.valueToString(map));datas.add(esData);map = new HashMap<String, Object>(); map.put("id",9);map.put("name","wigins");map.put("age",20);map.put("salary",500);map.put("team","tim");map.put("position","sf");
        esData = new EsData(""+map.get("id"), JSONObject.valueToString(map));datas.add(esData);
        elasticSearchServiceImpl.searchBulkIn("players-test", "players", datas);*/
    }



}