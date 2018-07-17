package es.test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import es.ElasticSearchServiceImpl;
import es.EsData;
import org.json.JSONObject;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

public class GoodsImport {
    private static Connection getConn() {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/data";
        String username = "canal";
        String password = "canal";
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
        // sql=SELECT * from bbg_dtp_mom_itemloc a inner join (SELECT id from
        // bbg_dtp_mom_itemloc limit 5000000, 10) b on a.id = b.id;
        int tot = 200000;
        final int pageSize = 100;
        int pageIndex = tot / pageSize;
        if (tot % pageSize != 0)
            pageIndex++;
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        final ElasticSearchServiceImpl elasticSearchServiceImpl = new ElasticSearchServiceImpl();
        for (int i = 1; i <= pageIndex; i++) {
            final int p = i;
            executorService.execute(new Runnable() {

                public void run() {
                    try {
                    	  String sql =
                                  "SELECT item as id,item_desc as description ,brand as brand,short_desc as 'name',item_group as price,item_subclass as sale from bbg_dtp_mom_item a inner join (SELECT id from bbg_dtp_mom_item limit "
                                  + (p - 1) * pageSize + ", " + pageSize +
                                  ") b on a.id = b.id";
                        /*
                         * String sql =
                         * "SELECT * from bbg_dtp_mom_item a inner join (SELECT id from bbg_dtp_mom_item limit "
                         * + (p - 1) pageSize + ", " + pageSize +
                         * ") b on a.id = b.id";
                         */
                       /* String sql = "SELECT a.id, b.item, b.barcode, b.brand, b.catch_weight_ind, b.default_waste_pct, b.diff_desc, b.`disable`, b.fresh_item_ind, b.inventory_ind, b.item_class, b.item_dept, b.item_desc, b.item_group, b.item_level, b.item_number_type, b.item_parent, b.item_status, b.item_subclass, b.joint_item_ind, b.mfg_rec_retail, b.primary_ref_item_ind, b.sellable_ind, b.vat_out_rate, b.vat_in_rate, b.tran_level, b.short_desc, b.standard_uom, a.item_loc, a.loc_type, a.unit_retail, a.regular_unit_retail, a.selling_unit_retail, a.selling_uom, a.av_cost, a.standard_gross_margin, a.promo_gross_margin FROM `bbg_dtp_mom_itemloc` AS a INNER JOIN ( SELECT id FROM bbg_dtp_mom_itemloc LIMIT "
                                + (p - 1)
                                * pageSize
                                + ", "
                                + pageSize
                                + ") c ON a.id = c.id, bbg_dtp_mom_item AS b WHERE a.item = b.item";
*/                        System.out.println(sql);
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
                                if(columnName.equals("id"))data.setId(rs.getObject(j).toString());		//id主要是设置主键es保留字段
                                map.put(columnName, rs.getObject(j));
                            }
                           
                            // System.out.println(JSONObject.valueToString(map));
                            data.setData(JSONObject.valueToString(map));
                             datas.add(data);

                            // elasticSearchServiceImpl.add(map);
                        }
                         elasticSearchServiceImpl.searchBulkIn(datas);
                        connection.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        try {
            Thread.sleep(10000);
            executorService.shutdown();
            executorService.awaitTermination(10000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }
    }
}

