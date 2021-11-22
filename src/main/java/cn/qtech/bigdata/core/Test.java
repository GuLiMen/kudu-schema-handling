package cn.qtech.bigdata.core;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;


public class Test {

    public static void main(String[] args)  {
        String kuduMaster = "10.170.3.134";
        try {
            KuduClient kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build();
      /*      HostAndPort leaderMasterServer = kuduClient.findLeaderMasterServer();
            System.out.println(leaderMasterServer);
*/
            String masterAddressesAsString = kuduClient.getMasterAddressesAsString();
            System.out.println("------"+masterAddressesAsString);
            List<String> tablesList = kuduClient.getTablesList().getTablesList();
            // 获取所有表名

            for (String tableName : tablesList) {
                System.out.println(tableName);


            }
        } catch (Exception e) {
            e.printStackTrace();
        }





    }
}
