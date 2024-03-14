package app;

import bean.TableProcessDim;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import constant.Constant;
import function.HBaseSinkFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import util.HBaseUtil;
import util.JdbcUtil;

import java.io.IOException;
import java.util.*;

/**
 * @ClassName: DimApp
 * @Description: TODO 类描述
 * @Author: Tanjh
 * @Date: 2024/03/12 11:40
 * @Company: Copyright©
 **/

@Slf4j
public class DimApp extends BaseApp {

    public static void main(String[] args) {
        new DimApp().start(10001,
                2,
                "dim_app",
                Constant.TOPIC_ODS_DB
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

//        stream.print();
        // 1. 对消费的数据, 做数据清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
//        etlStream.print();

        // 2. 通过 flink cdc 读取配置表的数据
        SingleOutputStreamOperator<TableProcessDim> configStream = readTableProcess(env);
//        configStream.print();

        // 3. 根据配置表的数据, 在 HBase 中建表
        configStream = createHBaseTable(configStream);
//        configStream.print();

        // 4.关联两个流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect = connect(etlStream, configStream);
//        connect.print();

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> streamOperator = deleteNotNeedColumns(connect);

        // 6. 写出到 HBase 目标表
        writeToHBase(streamOperator);

    }

    //过滤数据
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            System.out.println(value);

                            String database = jsonObject.getString("database");
                            String type = jsonObject.getString("type");
                            String data = jsonObject.getString("data");

                            return "gmall2022".equals(database)
                                    && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                                    && data != null
                                    && data.length() > 2;

                        } catch (Exception e) {
                            log.warn("非JSON格式数据:" + value);
                            return false;
                        }

                    }
                })
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        return JSON.parseObject(s);
                    }
                });
    }

    //拉取配置表
    public SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema())
//                .serverTimeZone("Asia/ShangHai")
//                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .build();

        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
                .setParallelism(1)
                .map(new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String s) throws Exception {

                        JSONObject jsonObject = JSON.parseObject(s);

                        JSONObject obj = JSON.parseObject(s);
                        String op = obj.getString("op");
                        TableProcessDim tableProcessDim;
                        if ("d".equals(op)) {
                            tableProcessDim = obj.getObject("before", TableProcessDim.class);
                        } else {
                            tableProcessDim = obj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }).setParallelism(1);

    }

    //创建维度表
    public SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpStream) {
        return tpStream.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 1. 获取到 HBase 的连接
                        hbaseConn = HBaseUtil.getHBaseConnection();

                    }

                    @Override
                    public void close() throws Exception {
                        // 2. 关闭连接
                        HBaseUtil.closeHBaseConn(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        String op = tableProcessDim.getOp();
                        if ("d".equals(op)) {
                            dropTable(tableProcessDim);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            createTable(tableProcessDim);
                        } else { // u 应该先删除表,再建表. 表的历史数据需要重新同步
                            dropTable(tableProcessDim);
                            createTable(tableProcessDim);
                        }
                        return tableProcessDim;
                    }

                    private void createTable(TableProcessDim tableProcessDim) throws IOException {
                        // namespace
                        HBaseUtil.createHBaseTable(hbaseConn,
                                Constant.HBASE_NAMESPACE,
                                tableProcessDim.getSinkTable(),
                                tableProcessDim.getSinkFamily());
                    }

                    private void dropTable(TableProcessDim tableProcessDim) throws IOException {
                        HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                    }


                })
                .setParallelism(1);
    }

    //关联两个流
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(
            SingleOutputStreamOperator<JSONObject> dataStream,
            SingleOutputStreamOperator<TableProcessDim> configStream) {

        // 1. 把配置流做成广播流
        // key: 表名   user_info
        // value: TableProcess
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("table_process_dim", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStream = configStream.broadcast(mapStateDescriptor);
        // 2. 数据流去 connect 广播流
        return dataStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {

                    private HashMap<String, TableProcessDim> map;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // open 中没有办法访问状态!!!
                        map = new HashMap<>();
                        // 1. 去 mysql 中查询 table_process 表所有数据

                        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
                        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gmall_config.table_process_dim", TableProcessDim.class, true);

                        for (TableProcessDim tableProcessDim : tableProcessDims) {
                            String key = tableProcessDim.getSourceTable();
                            map.put(key,tableProcessDim);
                        }

                        JdbcUtil.closeConnection(mysqlConnection);
                    }
                    // 2. 处理广播流中的数据: 把配置信息存入到广播状态中
                    @Override
                    public void processBroadcastElement(TableProcessDim tableProcessDim,
                                                        BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context,
                                                        Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);

                        String key = tableProcessDim.getSinkTable();

                        if ("d".equals(tableProcessDim.getOp())){
                            broadcastState.remove(key);
                            map.remove(key);
                        }else {
                            broadcastState.put(key,tableProcessDim);
                        }

                    }

                    // 3. 处理数据流中的数据: 从广播状态中读取配置信息
                    @Override
                    public void processElement(JSONObject object,
                                               BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext,
                                               Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

                        String key = object.getString("table");
                        TableProcessDim tableProcessDim = broadcastState.get(key);

                        if (tableProcessDim == null){
                            tableProcessDim = map.get(key);
                            if (tableProcessDim != null){
                                log.info("在 map 中查找到 " + key);
                            }
                        }else {
                            log.info("在 状态 中查找到 " + key);
                        }
                        if (tableProcessDim != null){
                            // 这条数据找到了对应的配置信息
                            JSONObject data = object.getJSONObject("data");

                            data.put("op_type",object.getString("type"));

                            collector.collect(Tuple2.of(data,tableProcessDim));
                        }


                    }

                });

    }

    //删除行
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> deleteNotNeedColumns(
            SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDataToTpStream){
        return dimDataToTpStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> dataWithConfig) throws Exception {
                JSONObject data = dataWithConfig.f0;

                List<String> columns = new ArrayList<>(Arrays.asList(dataWithConfig.f1.getSinkColumns().split(",")));
                columns.add("op_type");

                data.keySet().removeIf(data::containsKey);

                return dataWithConfig;

            }
        });


    }

    //写出Hbase
    private void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultStream) {
    /*
    1. 有没有专门的 HBase 连接器
    2. sql 有专门的 HBase 连接器, 由于一次只能写到一个表中, 所以也不能把流转成表再写
    3. 自定义sink
     */
        resultStream.addSink(new HBaseSinkFunction());
    }


}
