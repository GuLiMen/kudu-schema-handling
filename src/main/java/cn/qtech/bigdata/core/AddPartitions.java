package cn.qtech.bigdata.core;

import cn.qtech.bigdata.comm.Constants;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class AddPartitions {


    private static final Logger LOG = LoggerFactory.getLogger(AddPartitions.class.getClass());

    public static void main(String[] args) throws Exception {
        String kuduMaster = Constants.KUDU_MASTER;
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultAdminOperationTimeoutMs(600000);
        KuduClient kuduClient = kuduClientBuilder.build();
        LocalDate limitDate = LocalDate.now().plusMonths(4);
        // 获取所有表名
//        List<String> tablesList = kuduClient.getTablesList().getTablesList();

        //以下两行是
        List<String> tablesList = new ArrayList<>();
        tablesList.add("ADS_SPCMESRESULTLIST_TEST2");

        for (String tableName : tablesList) {

            KuduTable kuduTable = kuduClient.openTable(tableName);
            List<Integer> rangeSchemaIds = kuduTable.getPartitionSchema().getRangeSchema().getColumnIds();

            // 检查表是否有range分区
            if (rangeSchemaIds.size() != 1) {
                LOG.warn(tableName + "没有range分区");
                continue;
            }
            List<PartitionSchema.HashBucketSchema> hashBucketSchemas = kuduTable.getPartitionSchema().getHashBucketSchemas();
            // System.out.println(hashBucketSchemas.size()+"---------------");
            //设置主键
            Schema tableSchema = kuduTable.getSchema();
            List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
            List<ColumnSchema> primaryKeyColumns = kuduTable.getSchema().getPrimaryKeyColumns();
            primaryKeyColumns.stream().forEach(e -> {
                System.out.println(e);
                columns.add(new ColumnSchema.ColumnSchemaBuilder(e.getName(), e.getType()).key(true).build());
            });
            ColumnSchema rangeKeySchema = tableSchema.getColumnByIndex(rangeSchemaIds.get(0));
       /*       columns.add(new ColumnSchema.ColumnSchemaBuilder(rangeKeySchema.getName(), rangeKeySchema.getType()).key(true).build());
            if(hashBucketSchemas.size() > 0){
                List<Integer> PKIds = hashBucketSchemas.get(0).getColumnIds();
                for (Integer hashId : PKIds) {
                    ColumnSchema PKSchema = tableSchema.getColumnByIndex(hashId);
                    // parimary key既有hash又有range分区
                    if (!rangeKeySchema.getName().equals(PKSchema.getName())) {
                        columns.add(new ColumnSchema.ColumnSchemaBuilder(PKSchema.getName(), PKSchema.getType()).key(true).build());
                    }
                }
            }else{
                LOG.info(tableName + "没有hash分区");
            }*/

            if (hashBucketSchemas.size() <= 0) {
                LOG.info(tableName + "没有hash分区");
            }

            /**
             *  1.获取表最后的range分区 开始结束时间
             *  2.获取分区步长
             *  3.根据原有分区步长设置分区时间
             */
            List<Partition> rangePartitions = kuduTable.getRangePartitions(Integer.MAX_VALUE);
            Partition endRangePartition = rangePartitions.get(rangePartitions.size() - 1);
            if (endRangePartition.getRangeKeyEnd().length == 0) {
                /**排除这种定义
                 * HASH (ID) PARTITIONS 3,
                 * RANGE (ID) (
                 *     PARTITION UNBOUNDED
                 * )
                 * */
                LOG.warn(tableName + "  RANGE PARTITION UNBOUNDE 未指定range分区");
                continue;
            }
            String rangeKeyEnd = new String(endRangePartition.getRangeKeyEnd());
            String rangeKeyStart = new String(endRangePartition.getRangeKeyStart());
            System.out.println(tableName + "目前最后分区:" + rangeKeyStart + "->" + rangeKeyEnd);

            String style = "";
            String partten = "";
            if (rangeKeyEnd.contains(".")) {
                partten = "yyyy.MM.dd";
                style = ".";
            } else if (rangeKeyEnd.contains("-")) {
                partten = "yyyy-MM-dd";
                style = "-";
            } else if (rangeKeyEnd.contains("/")) {
                partten = "yyyy/MM/dd";
                style = "/";
            } else if (rangeKeyEnd.length() == 8) {
                partten = "yyyyMMdd";
            } else {
                System.out.println(tableName + "range格式不在范围内" + rangeKeyEnd);
            }

            //指定转换格式
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(partten);
            //进行转换
            LocalDate end = LocalDate.parse(rangeKeyEnd, dateFormatter);
            LocalDate start = LocalDate.parse(rangeKeyStart, dateFormatter);
            long step = start.until(end, ChronoUnit.DAYS);
            int year = end.getYear();
            int dayOfMonth = end.getDayOfMonth();
            int monthValue = end.getMonthValue();
            int nextMonth = 0;
            int nextDay = 0;
            int nextYear = year;
            /** 分区格式:
             PARTITION '2020-08-01' <= VALUES < '2020-08-16',
             PARTITION '2020-08-16' <= VALUES < '2020-09-01',
             * */
            if (step >= 14 && step <= 17) {
                while (true) {
                    if (dayOfMonth == 01 || dayOfMonth == 28 || dayOfMonth == 29 || dayOfMonth == 30 || dayOfMonth == 31) {
                        nextDay = 16;
                        nextMonth = monthValue;
                    } else if (dayOfMonth == 15 || dayOfMonth == 16) {
                        nextDay = 01;
                        nextMonth = monthValue + 1;
                        if (monthValue == 12) {
                            nextYear = year + 1;
                            nextMonth = 01;
                        }
                    } else {
                        LOG.error("分区结束dayOfMonth !=15 and dayOfMonth!=1 ");
                    }
                    String formatNextMonth = String.format("%02d", nextMonth);
                    String formatNextDay = String.format("%02d", nextDay);
                    String formatBeforeMonth = String.format("%02d", monthValue);
                    String formatBeforeDay = String.format("%02d", dayOfMonth);

                    String nextPartition = nextYear + style + formatNextMonth + style + formatNextDay; //由数字组合2020-08-01 2020.08.01 2020/08/01 ..
                    String beforePartition = year + style + formatBeforeMonth + style + formatBeforeDay;
                    //    System.out.println(beforePartition+" -> "+nextPartition );
                    year = nextYear;
                    monthValue = Integer.parseInt(formatNextMonth);
                    dayOfMonth = Integer.parseInt(formatNextDay);
                    LocalDate beforePartitionDate = LocalDate.parse(beforePartition, dateFormatter);
                    if (beforePartitionDate.isAfter(limitDate)) {
                        break;
                    }
                    /**
                     * 执行添加分区
                     * TODO:lowerPartialRow跟upperPartialRow 需要是两个对象
                     */
                    Schema schema = new Schema(columns);
                    PartialRow lowerPartialRow = schema.newPartialRow();
                    lowerPartialRow.addString(rangeKeySchema.getName(), beforePartition);
                    PartialRow upperPartialRow = schema.newPartialRow();
                    upperPartialRow.addString(rangeKeySchema.getName(), nextPartition);
                    kuduClient.alterTable(tableName, new AlterTableOptions().addRangePartition(
                            lowerPartialRow, upperPartialRow, RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.EXCLUSIVE_BOUND));


                }
            } else if (step >= 29 && step <= 31) {
                while (true) {
                    if (dayOfMonth == 01 || dayOfMonth == 28 || dayOfMonth == 29 || dayOfMonth == 30 || dayOfMonth == 31) {
                        nextDay = 01;
                        nextMonth = monthValue + 1;
                        if (monthValue == 12) {
                            nextYear = year + 1;
                            nextMonth = 01;
                        }
                    } else {
                        LOG.error("分区结束dayOfMonth !=15 and dayOfMonth!=1 ");
                    }
                    String formatNextMonth = String.format("%02d", nextMonth);
                    String formatNextDay = String.format("%02d", nextDay);
                    String formatBeforeMonth = String.format("%02d", monthValue);
                    String formatBeforeDay = String.format("%02d", dayOfMonth);

                    String nextPartition = nextYear + style + formatNextMonth + style + formatNextDay; //由数字组合2020-08-01 2020.08.01 2020/08/01 ..
                    String beforePartition = year + style + formatBeforeMonth + style + formatBeforeDay;
                    //    System.out.println(beforePartition+" -> "+nextPartition );
                    year = nextYear;
                    monthValue = Integer.parseInt(formatNextMonth);
                    dayOfMonth = Integer.parseInt(formatNextDay);
                    LocalDate beforePartitionDate = LocalDate.parse(beforePartition, dateFormatter);
                    if (beforePartitionDate.isAfter(limitDate)) {
                        break;
                    }
                    /**
                     * 执行添加分区
                     * TODO:lowerPartialRow跟upperPartialRow 需要是两个对象
                     */
                    Schema schema = new Schema(columns);
                    PartialRow lowerPartialRow = schema.newPartialRow();
                    lowerPartialRow.addString(rangeKeySchema.getName(), beforePartition);
                    PartialRow upperPartialRow = schema.newPartialRow();
                    upperPartialRow.addString(rangeKeySchema.getName(), nextPartition);
                    kuduClient.alterTable(tableName, new AlterTableOptions().addRangePartition(
                            lowerPartialRow, upperPartialRow, RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.EXCLUSIVE_BOUND));
                }
            } else {
                LOG.error(tableName + "分区步长：" + step + "  " + rangeKeyEnd + "  " + rangeKeyStart + "分区步长不在 14 <= tep <= 17 或者 29 <= tep <= 31 范围内,请增加此段逻辑 ");
            }

        }

    }
}
