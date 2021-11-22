package cn.qtech.bigdata.core;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;

import java.util.List;

import static cn.qtech.bigdata.comm.Constants.*;

public class replicaCount {
    public static void main(String[] args)  {
        try {
            KuduClient kuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
      /*      HostAndPort leaderMasterServer = kuduClient.findLeaderMasterServer();
            System.out.println(leaderMasterServer);
*/

            List<String> tablesList = kuduClient.getTablesList().getTablesList();
            // 获取所有表名

            for (String tableName : tablesList) {
                KuduTable kuduTable = kuduClient.openTable(tableName);
//                List<Integer> rangeSchemaIds = kuduTable.getSchema();
                if (kuduTable.getNumReplicas() < 3 ) {
                    System.out.println("表名:"+kuduTable.getName()+"、副本数为"+kuduTable.getNumReplicas());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
