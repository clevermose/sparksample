//package cn.migu.comms.unifyopreation.spark.corebiz;
//
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.Serializable;
//import java.math.BigDecimal;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Set;
//import java.util.regex.Pattern;
//
//import org.apache.log4j.Logger;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.SaveMode;
//import org.apache.spark.sql.hive.HiveContext;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.HasOffsetRanges;
//import org.apache.spark.streaming.kafka.KafkaCluster;
//import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import org.apache.spark.streaming.kafka.OffsetRange;
//
//import cn.migu.comms.unifyopreation.common.CommFunctionUtil;
//import cn.migu.comms.unifyopreation.common.Constants;
//import cn.migu.comms.unifyopreation.common.DataBaseUtil;
//import cn.migu.comms.unifyopreation.common.ReadyDataConstants;
//import cn.migu.comms.unifyopreation.common.SparkSQLUtil;
//import cn.migu.comms.unifyopreation.kafka.KafkaProducer;
//import cn.migu.comms.unifyopreation.model.DataBaseInfo;
//import cn.migu.comms.unifyopreation.model.SQLLogModel;
//import cn.migu.comms.unifyopreation.spark.dataprocess.CleanDataLoadProcess;
//import cn.migu.comms.unifyopreation.spark.dataprocess.ReadyDataLoadProcess;
//import cn.migu.comms.unifyopreation.spark.dataprocess.SQLLogProcess;
//import kafka.common.TopicAndPartition;
//import kafka.message.MessageAndMetadata;
//import kafka.serializer.DefaultDecoder;
//import kafka.serializer.StringDecoder;
//import scala.Predef;
//import scala.Tuple2;
//import scala.collection.JavaConversions;
//
///**
// * 
// * streaming主业务程序
//
// */
//public class FlumeKafkaSparkStreaming implements Serializable
//{
//    /**
//     * 注释内容
//     */
//    private static final long serialVersionUID = 1L;
//    
//    //    private static final Logger logger = LoggerFactory.getLogger(SparkStreamingTaskImpl.class);
//    private static Logger logger = Logger.getLogger("STREAMING");
//    
//    //JAR包清洗信息
//    private static Map<String, Object> jarClean = new HashMap<String, Object>();
//    
//    //列清洗信息
//    private static Map<Integer, Object> colClean = new HashMap<Integer, Object>();
//    
//    //输入数据源配置
//    private static DataBaseInfo inDBInfo = null;
//    
//    //一般运行SQL
//    private static Map<Integer, Object> computeSQL = new HashMap<Integer, Object>();
//    
//    //合并运行SQL
//    private static Map<Integer, Object> combineSQL = new HashMap<Integer, Object>();
//    
//    //输出SQL
//    private static Map<Integer, Object> outSQL = new HashMap<Integer, Object>();
//    
//    //基础信息MAP
//    private static Map<String, String> baseMap = new HashMap<String, String>();
//    
//    private KafkaCluster kafkaCluster = null;
//    
//    private Map<String, String> kafkaParams = new HashMap<String, String>();
//    
//    private Set<String> topics = new HashSet<String>();
//    
//    private Duration duration = new Duration(Constants.STREAMING_DURATION);
//    
//    private static SQLContext sqlcontext = null;
//    
//    private static HiveContext hivecontext = null;
//    
//    private static JavaSparkContext jsc = null;
//    
//    //运行SQL中间表数据存储
//    private static Map<String, DataFrame> dfMap = new HashMap<String, DataFrame>();
//    
//    //输出输据源配置
//    private static DataBaseInfo outDBInfo = null;
//    
//    //今天日期
//    private static String todayString = "";
//    
//    //输出数据集
//    private static List<Row> outList = null;
//    
//    //输出数据标签集
//    private static StructField[] outFields = null;
//    
//    private java.util.Map<kafka.common.TopicAndPartition, Long> fromOffsets =
//        new java.util.HashMap<kafka.common.TopicAndPartition, Long>();
//        
//    private static String appId = "";
//    
//    private static String serverId = "";
//    
//    private static String jarId = "";
//    
//    private static List<SQLLogModel> logList = new ArrayList<SQLLogModel>();
//    
//    //当前时间，到天
//    private static String TODAY_D = "";
//    
//    //当前时间，到小时
//    private static String TODAY_H = "";
//    
//    //当前时间，到分    
//    private static String TODAY_M = "";
//    
//    //当前时间，到秒    
//    private static String TODAY_S = "";
//    
//    //当前时间，到天
//    private static String TODAYD = "";
//    
//    //当前时间，到小时
//    private static String TODAYH = "";
//    
//    //当前时间，到分    
//    private static String TODAYM = "";
//    
//    //当前时间，到秒    
//    private static String TODAYS = "";
//    
//    @SuppressWarnings("static-access")
//    public FlumeKafkaSparkStreaming(String appId, String serverId, String jarId, JavaSparkContext jsc,
//        SQLContext sqlcontext, HiveContext hivecontext)
//    {
//        
//        Calendar cal = Calendar.getInstance();
//        int year = cal.get(Calendar.YEAR);//获取年份
//        int month = cal.get(Calendar.MONTH) + 1;//获取月份
//        int day = cal.get(Calendar.DATE);//获取日
//        todayString = year + "-" + month + "-" + day;
//        
//        jarClean = CleanDataLoadProcess.getJarClean(appId, serverId, jarId, sqlcontext);
//        logger.error(Constants.LOG_FLAG + "|||" + "JAR包信息加载成功:" + jarClean.toString());
//        
//        inDBInfo = CommFunctionUtil.getInDataSource(appId, serverId, jarId, sqlcontext, getDBInfo());
//        logger.error(Constants.LOG_FLAG + "|||" + "输入数据源信息加载成功:" + inDBInfo.toString());
//        
//        colClean = CleanDataLoadProcess.getColClean(appId, serverId, jarId, sqlcontext);
//        logger.error(Constants.LOG_FLAG + "|||" + "列清洗信息加载成功:" + colClean.toString());
//        
//        computeSQL = CleanDataLoadProcess.getComputeSQL(appId, serverId, jarId, sqlcontext);
//        logger.error(Constants.LOG_FLAG + "|||" + "运行SQL信息加载成功:" + computeSQL.toString());
//        
//        outSQL = CleanDataLoadProcess.getOutSQL(appId, serverId, jarId, sqlcontext);
//        logger.error(Constants.LOG_FLAG + "|||" + "输出SQL信息加载成功:" + outSQL.toString());
//        
//        combineSQL = CleanDataLoadProcess.getCombineSQL(appId, serverId, jarId, sqlcontext);
//        logger.error(Constants.LOG_FLAG + "|||" + "运行合并SQL信息加载成功:" + combineSQL.toString());
//        
//        baseMap.put("serverId", serverId);
//        baseMap.put("jarId", jarId);
//        baseMap.put("tableName", inDBInfo.getTableName());
//        
//        //将重要变量变成类全局变量
//        this.jsc = jsc;
//        
//        this.sqlcontext = sqlcontext;
//        
//        this.hivecontext = hivecontext;
//        
//        this.appId = appId;
//        
//        this.serverId = serverId;
//        
//        this.jarId = jarId;
//        
//        kafkaParams.put("metadata.broker.list", inDBInfo.getDbUrl());
//        kafkaParams.put("group.id", inDBInfo.getUserName());
//        //        kafkaParams.put("zookeeper.connect", "172.16.11.21:2181,172.16.11.22:2181,172.16.11.23:2181");
//        
//        scala.collection.mutable.Map<String, String> mutableKafkaParam = JavaConversions.mapAsScalaMap(kafkaParams);
//        scala.collection.immutable.Map<String, String> immutableKafkaParam =
//            mutableKafkaParam.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>()
//            {
//                /**
//                 * 
//                 */
//                private static final long serialVersionUID = 1L;
//                
//                @Override
//                public Tuple2<String, String> apply(Tuple2<String, String> v1)
//                {
//                    return v1;
//                }
//            });
//        this.kafkaCluster = new KafkaCluster(immutableKafkaParam);
//        //        this.topics.add("streamTP_01");
//        this.topics.add(inDBInfo.getTableName());
//    }
//    
//    /**
//     * 
//     * 获取数据库配置
//     * <功能详细描述>
//     * @return
//     * @see [类、类#方法、类#成员]
//     */
//    private static DataBaseInfo getDBInfo()
//    {
//        DataBaseInfo dbInfo = new DataBaseInfo();
//        dbInfo.setConnectionType(ReadyDataConstants.CONNECT_WITH_SID);
//        dbInfo.setIp(ReadyDataConstants.DATABASE_IP);
//        dbInfo.setPort(Integer.valueOf(ReadyDataConstants.DATABASE_PORT));
//        dbInfo.setDataBaseName(ReadyDataConstants.DATABASE_NAME);
//        dbInfo.setUserName(ReadyDataConstants.DATABASE_USERNAME);
//        dbInfo.setPassword(ReadyDataConstants.DATABASE_PASSWORD);
//        
//        return dbInfo;
//    }
//    
//    /**
//     * 
//     * 开始STREAMING
//     * <功能详细描述>
//     * @see [类、类#方法、类#成员]
//     */
//    public void startStreaming()
//    {
//        logger.warn(Constants.LOG_FLAG + "|||" + "STREAING STARTED");
//        //启动成功标志
//        System.err.println(Constants.LOG_FLAG + "|||" + "STREAING STARTED");
//        logger.error(Constants.LOG_FLAG + "|||" + "--------------startStreaming------------");
//        
//        List<Row> list = new ArrayList<Row>();
//        
//        final Map<String, List<Row>> map = new HashMap<String, List<Row>>();
//        map.put("df", list);
//        
//        //明细全量数据全局存储对像
//        final Map<String, JavaRDD<Row>> allRddMap = new HashMap<String, JavaRDD<Row>>();
//        
//        JavaStreamingContext jsctx = new JavaStreamingContext(jsc, duration);
//        
//        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(this.topics);
//        scala.collection.immutable.Set<String> immutableTopics = mutableTopics.toSet();
//        scala.collection.immutable.Set<TopicAndPartition> scalaTopicAndPartitionSet =
//            kafkaCluster.getPartitions(immutableTopics).right().get();
//            
//        // 首次消费，默认设置为0
//        if (kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id"), scalaTopicAndPartitionSet).isLeft())
//        {
//            Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions.setAsJavaSet(scalaTopicAndPartitionSet);
//            
//            scala.collection.immutable.Map<TopicAndPartition, LeaderOffset> earliestOffset =
//                kafkaCluster.getEarliestLeaderOffsets(scalaTopicAndPartitionSet).right().get();
//                
//            Map<TopicAndPartition, LeaderOffset> earliestOffsetMap = JavaConversions.asJavaMap(earliestOffset);
//            
//            for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet)
//            {
//                Long offset = 0L;
//                Long leaderOffset = earliestOffsetMap.get(topicAndPartition).offset();
//                
//                if (offset < leaderOffset)
//                    offset = leaderOffset;
//                this.fromOffsets.put(topicAndPartition, offset);
//            }
//        }
//        else
//        {
//            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp =
//                kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id"), scalaTopicAndPartitionSet).right().get();
//            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);
//            //wqg---------------------------
//            scala.collection.immutable.Map<TopicAndPartition, LeaderOffset> earliestOffset =
//                kafkaCluster.getEarliestLeaderOffsets(scalaTopicAndPartitionSet).right().get();
//            Map<TopicAndPartition, LeaderOffset> earliestOffsetMap = JavaConversions.asJavaMap(earliestOffset);
//            //-----------------------------
//            Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions.setAsJavaSet(scalaTopicAndPartitionSet);
//            for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet)
//            {
//                Long offset = (Long)consumerOffsets.get(topicAndPartition);
//                //wqg------------------------
//                Long leaderOffset = earliestOffsetMap.get(topicAndPartition).offset();
//                if (offset < leaderOffset)
//                    offset = leaderOffset;
//                //---------------------
//                this.fromOffsets.put(topicAndPartition, offset);
//            }
//        }
//        
//        //主动连接kafka
//        JavaInputDStream<byte[]> stream = KafkaUtils.createDirectStream(jsctx,
//            String.class,
//            byte[].class,
//            StringDecoder.class,
//            DefaultDecoder.class,
//            byte[].class,
//            kafkaParams,
//            this.fromOffsets,
//            new Function<MessageAndMetadata<String, byte[]>, byte[]>()
//            {
//                /**
//                 * 
//                 */
//                private static final long serialVersionUID = 1L;
//                
//                @Override
//                public byte[] call(MessageAndMetadata<String, byte[]> v1)
//                    throws Exception
//                {
//                    return v1.message();
//                }
//            });
//            
//        stream.foreachRDD(new VoidFunction<JavaRDD<byte[]>>()
//        
//        {
//            
//            private static final long serialVersionUID = 1L;
//            
//            @SuppressWarnings("unchecked")
//            @Override
//            public void call(JavaRDD<byte[]> arg0)
//                throws Exception
//            {
//                logger.error(
//                    Constants.LOG_FLAG + "|||" + "---------------------------新一批数据获取，开始STREAMING侧处理----------------");
//                    
//                Calendar cal = Calendar.getInstance();
//                int year = cal.get(Calendar.YEAR);//获取年份
//                int month = cal.get(Calendar.MONTH) + 1;//获取月份
//                int day = cal.get(Calendar.DATE);//获取日
//                int hour = cal.get(Calendar.HOUR_OF_DAY);//24小时制
//                int minute = cal.get(Calendar.MINUTE);//分 
//                int second = cal.get(Calendar.SECOND);//秒
//                
//                TODAY_D = year + "-" + toTwoString(month) + "-" + toTwoString(day);
//                TODAY_H = year + "-" + toTwoString(month) + "-" + toTwoString(day) + " " + toTwoString(hour);
//                TODAY_M = year + "-" + toTwoString(month) + "-" + toTwoString(day) + " " + toTwoString(hour) + ":"
//                    + toTwoString(minute);
//                TODAY_S = year + "-" + toTwoString(month) + "-" + toTwoString(day) + " " + toTwoString(hour) + ":"
//                    + toTwoString(minute) + ":" + toTwoString(second);
//                    
//                TODAYD = year + toTwoString(month) + toTwoString(day);
//                TODAYH = year + toTwoString(month) + toTwoString(day) + " " + toTwoString(hour);
//                TODAYM =
//                    year + toTwoString(month) + toTwoString(day) + " " + toTwoString(hour) + ":" + toTwoString(minute);
//                TODAYS = year + toTwoString(month) + toTwoString(day) + " " + toTwoString(hour) + ":"
//                    + toTwoString(minute) + ":" + toTwoString(second);
//                    
//                //清空SQL日志信息    
//                logList = new ArrayList<SQLLogModel>();
//                
//                logger.error(Constants.LOG_FLAG + "|||" + "原始数据集长度:" + arg0.count());
//                
//                if (allRddMap.get("allRdd") != null)
//                {
//                    logger.error(Constants.LOG_FLAG + "|||" + "内存表数据长度:" + allRddMap.get("allRdd").count());
//                }
//                else
//                {
//                    logger.error(Constants.LOG_FLAG + "|||" + "内存表数据长度:" + 0);
//                }
//                //定时清理数据
//                if (jarClean.get("clearTime") != null)
//                {
//                    String nowDateString = year + "-" + month + "-" + day;
//                    
//                    //分隔设定清理时间
//                    String[] cleanTimeS = ((String)jarClean.get("clearTime")).split(":");
//                    //判断是否已经更换日期
//                    if (!todayString.equals(nowDateString) && cleanTimeS.length >= 2)
//                    {
//                        //判断是否已经过了清除时间
//                        if (hour > Integer.valueOf(cleanTimeS[0])
//                            || (hour == Integer.valueOf(cleanTimeS[0]) && minute > Integer.valueOf(cleanTimeS[1])))
//                        {
//                            todayString = nowDateString;
//                            allRddMap.clear();
//                            allRddMap.put("allRdd", null);
//                            logger.error(
//                                Constants.LOG_FLAG + "|||" + "到达清除数据时间点，清除数据，数据清除时间点为：" + jarClean.get("clearTime"));
//                        }
//                    }
//                }
//                
//                if (arg0.count() > 0)
//                {
//                    
//                    //是否为空标志（为文本输出特别定制业务，当一批数据进来后，经过清洗和计算后，无数据的，需要产生空白的文件，并传给相应指定服务器）
//                    Boolean isNullFlag = true;
//                    
//                    //数据列数清洗
//                    JavaRDD<String> baseS = jarCleanFun(arg0, jarClean, baseMap);
//                    logger.error(Constants.LOG_FLAG + "|||" + "jar清洗,清洗后数据集长度:" + baseS.count());
//                    
//                    //注册基础数据
//                    registerBaseTable(baseS, jarClean, colClean);
//                    logger.error(Constants.LOG_FLAG + "|||" + "注册JAR清洗后数据表");
//                    
//                    //数据列清洗
//                    JavaRDD<String> cleanS = colCleanFun(baseS, jarClean, colClean, baseMap);
//                    logger.error(Constants.LOG_FLAG + "|||" + "数据列信息清洗后长度：" + cleanS.count());
//                    
//                    //数据计算
//                    DataFrame computeR = null;
//                    long computeSize = 0;
//                    if (cleanS.count() > 0)
//                    {
//                        //注册清洗后数据
//                        registerCleanTable(cleanS, jarClean, colClean, allRddMap);
//                        logger.error(Constants.LOG_FLAG + "|||" + "注册列清洗后数据表");
//                        
//                        computeR = dataCompute(jarClean, computeSQL, sqlcontext);
//                        if (computeR != null)
//                        {
//                            
//                            computeSize = computeR.count();
//                        }
//                        if ((computeR != null && computeSize > 0) || computeSQL.size() == 0)
//                        {
//                            logger.error(Constants.LOG_FLAG + "|||" + "数据计算，计算后的数据集长度：" + computeSize);
//                            
//                            SQLLogModel sqlLog = new SQLLogModel();
//                            //合并运行SQL，对信息进行合并
//                            for (int i = 0; i < combineSQL.size(); i++)
//                            {
//                                Map<String, String> oneMap = (Map<String, String>)combineSQL.get(i);
//                                
//                                sqlLog = new SQLLogModel();
//                                sqlLog.setAppId(appId);
//                                sqlLog.setServerId(serverId);
//                                sqlLog.setJarId(jarId);
//                                sqlLog.setKind(new BigDecimal(3));
//                                sqlLog.setSeq(new BigDecimal(i + 1));
//                                sqlLog.setJarSql(oneMap.get("sql"));
//                                Date startTime = new Date();
//                                Date endTime = new Date();
//                                
//                                List<StructField> structFieldsFoo =
//                                    SparkSQLUtil.structFieldByFielsMap(CleanDataLoadProcess
//                                        .getColFromUnifySourceFiled(sqlcontext, oneMap.get("sourceId")));
//                                        
//                                StructType structTypeFoo = DataTypes.createStructType(structFieldsFoo);
//                                
//                                sqlcontext.createDataFrame(map.get("df"), structTypeFoo)
//                                    .registerTempTable(oneMap.get("tableName"));
//                                try
//                                {
//                                    DataFrame combineDF = sqlcontext.sql(oneMap.get("sql"));
//                                    
//                                    combineDF.show();
//                                    map.put("df", combineDF.collectAsList());
//                                    
//                                    String nowDateString = year + "-" + month + "-" + day;
//                                    
//                                    //分隔设定清理时间
//                                    String[] cleanTimeS = ((String)jarClean.get("clearTime")).split(":");
//                                    //判断是否已经更换日期
//                                    if (!todayString.equals(nowDateString))
//                                    {
//                                        //判断是否已经过了清除时间
//                                        if (hour > Integer.valueOf(cleanTimeS[0])
//                                            || (hour == Integer.valueOf(cleanTimeS[0])
//                                                && minute > Integer.valueOf(cleanTimeS[1])))
//                                        {
//                                            todayString = nowDateString;
//                                            map.put("df", null);
//                                        }
//                                        
//                                    }
//                                    
//                                    //                                combineDF = null;
//                                    //                                computeR = null;
//                                    combineDF.registerTempTable(oneMap.get("tableName"));
//                                    logger.error(Constants.LOG_FLAG + "|||" + "合并运行SQL：" + oneMap.get("sql"));
//                                    
//                                    //1：正常结束，2：异常中止
//                                    sqlLog.setState(new BigDecimal(1));
//                                    
//                                    endTime = new Date();
//                                }
//                                catch (Exception e)
//                                {
//                                    logger.error(
//                                        Constants.LOG_FLAG + "|||" + "合并运行SQL异常,请确认SQL是否正确：" + oneMap.get("sql"));
//                                    logger.error(Constants.LOG_FLAG + "|||" + e.toString());
//                                    
//                                    //1：正常结束，2：异常中止
//                                    sqlLog.setState(new BigDecimal(2));
//                                    
//                                    endTime = new Date();
//                                    
//                                    logger.error(Constants.LOG_FLAG + "|||合并运行SQL异常,该异常导致程序无法正常完成，现中止程序");
//                                    //SQL执行错误，程序停止
//                                    System.exit(0);
//                                }
//                                
//                                sqlLog.setStartTime(startTime);
//                                sqlLog.setEndTime(endTime);
//                                sqlLog.setElapse(new BigDecimal(endTime.getTime() - startTime.getTime()));
//                                logList.add(sqlLog);
//                                
//                            }
//                            
//                            for (int i = 0; i < outSQL.size(); i++)
//                            {
//                                isNullFlag = false;
//                                Map<String, String> oneMap = (Map<String, String>)outSQL.get(i);
//                                outDBInfo =
//                                    CommFunctionUtil.getDataSource(oneMap.get("sourceId"), sqlcontext, getDBInfo());
//                                    
//                                //迭代结果，入库处理
//                                if (Constants.DB_TYPE_KAFKA.equals(outDBInfo.getDbType()))
//                                {
//                                    pushDataToKafka(computeR, outDBInfo);
//                                }
//                                else if (Constants.DB_TYPE_HIVE.equals(outDBInfo.getDbType()))
//                                {
//                                    pushDataToHive(computeR,
//                                        sqlcontext,
//                                        hivecontext,
//                                        outDBInfo,
//                                        oneMap.get("sql"),
//                                        oneMap.get("tableName"),
//                                        oneMap.get("runMode"),
//                                        computeSize,
//                                        i + 1);
//                                }
//                                else if (Constants.DB_TYPE_ORCAL.equals(outDBInfo.getDbType()))
//                                {
//                                    pushDataToOrcal(computeR,
//                                        sqlcontext,
//                                        outDBInfo,
//                                        oneMap.get("sql"),
//                                        oneMap.get("tableName"),
//                                        oneMap.get("runMode"),
//                                        computeSize,
//                                        i + 1);
//                                }
//                                else if (Constants.DB_TYPE_REDIS.equals(outDBInfo.getDbType()))
//                                {
//                                    pushDataToGPl();
//                                }
//                                else if (Constants.DB_TYPE_MYSQL.equals(outDBInfo.getDbType()))
//                                {
//                                    pushDataToMysql(computeR,
//                                        sqlcontext,
//                                        outDBInfo,
//                                        oneMap.get("sql"),
//                                        oneMap.get("tableName"));
//                                }
//                                else if (Constants.DB_TYPE_FTP.equals(outDBInfo.getDbType()))
//                                {
//                                    pushDataToText(computeR,
//                                        sqlcontext,
//                                        oneMap.get("sql"),
//                                        outDBInfo.getIp(),
//                                        outDBInfo.getDbUrl(),
//                                        outDBInfo.getUserName(),
//                                        outDBInfo.getPassword(),
//                                        oneMap.get("tableName"),
//                                        outDBInfo.getDecollator());
//                                }
//                                else if (Constants.DB_TYPE_HBASE.equals(outDBInfo.getDbType()))
//                                {
//                                    pushDataToHbase(computeR,
//                                        sqlcontext,
//                                        outDBInfo,
//                                        oneMap.get("sql"),
//                                        oneMap.get("tableName"),
//                                        oneMap.get("runMode"),
//                                        computeSize,
//                                        i + 1);
//                                }
//                            }
//                        }
//                        
//                    }
//                    if (isNullFlag)
//                    {
//                        isEmptyResultToFtpNullFile();
//                    }
//                }
//                SQLLogProcess.saveLog(logList);
//                OffsetRange[] offsets = ((HasOffsetRanges)arg0.rdd()).offsetRanges();
//                for (OffsetRange o : offsets)
//                {
//                    // 封装topic.partition 与 offset对应关系 java Map
//                    TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
//                    Map<TopicAndPartition, Object> topicAndPartitionObjectMap =
//                        new HashMap<TopicAndPartition, Object>();
//                    topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());
//                    
//                    // 转换java map to scala immutable.map
//                    scala.collection.mutable.Map<TopicAndPartition, Object> map =
//                        JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);
//                    scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
//                        map.toMap(
//                            new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>()
//                    {
//                        /**
//                         * 
//                         */
//                        private static final long serialVersionUID = 1L;
//                        
//                        @Override
//                        public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1)
//                        {
//                            return v1;
//                        }
//                    });
//                    
//                    // 更新offset到kafkaCluster 第一个参数 group_id
//                    kafkaCluster.setConsumerOffsets(inDBInfo.getUserName(), scalatopicAndPartitionObjectMap);
//                }
//                logger.error(
//                    Constants.LOG_FLAG + "|||" + "---------------------------一批数据结束STREAMING侧处理----------------");
//                    
//                logger.error(Constants.LOG_FLAG + "|||" + "---------------------------刷新维表START----------------");
//                new ReadyDataLoadProcess().loadReadyData(appId,
//                    serverId,
//                    jarId,
//                    jsc,
//                    sqlcontext,
//                    hivecontext,
//                    Constants.DATA_LOAD_REFRESH);
//                logger.error(Constants.LOG_FLAG + "|||" + "---------------------------刷新维表END----------------");
//            }
//            
//        });
//        jsctx.start();
//        jsctx.awaitTermination();
//        
//    }
//    
//    /**
//     * 
//     * 数据列数清洗
//     * <功能详细描述>
//     * @param rddByte
//     * @return
//     * @see [类、类#方法、类#成员]
//     */
//    private static JavaRDD<String> jarCleanFun(JavaRDD<byte[]> rddByte, final Map<String, Object> jarCleanR,
//        final Map<String, String> baseMap)
//    {
//        JavaRDD<String> s = rddByte.flatMap(new FlatMapFunction<byte[], String>()
//        {
//            
//            private static final long serialVersionUID = 1L;
//            
//            @Override
//            public Iterable<String> call(byte[] line)
//                throws Exception
//            {
//                String v = new String(line);
//                //                //返回每行数据，这里需要把第一个FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF截取掉
//                //                String v1 = v.substring(v.indexOf("FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF") + 42);
//                //                //将信息转编码为UTF-8
//                //                String v2 = new String(v1.toString().getBytes((String)jarCleanR.get("character")), "UTF-8");
//                
//                return Arrays.asList(v);
//            }//以下为清洗业务示例
//        }).filter(new Function<String, Boolean>()
//        {
//            
//            private static final long serialVersionUID = 1L;
//            
//            @Override
//            public Boolean call(String news)
//                throws Exception
//            {
//                String v1 = news;
//                //返回每行数据，这里需要把第一个FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF截取掉
//                if (news.indexOf("FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF") > -1)
//                {
//                    v1 = news.substring(news.indexOf("FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF") + 42);
//                }
//                
//                Boolean isOk = true;
//                //判断该JAR包是否需要清洗
//                if ((boolean)jarCleanR.get("isClean"))
//                {
//                    String splitFlag = (String)jarCleanR.get("splitFlag");
//                    //增加分隔符为空的，当分隔符为空时，按长度分隔
//                    if (null == splitFlag || "".equals(splitFlag))
//                    {
//                        if (v1.length() >= (int)jarCleanR.get("charNum"))
//                        {
//                            isOk = true;
//                        }
//                        else
//                        {
//                            Map<String, String> errMap = new HashMap<String, String>();
//                            errMap.put("failCode", Constants.JAR_DATA_CLEAR);//失败代码
//                            errMap.put("note", "数据长度清洗||数据长度为：" + v1.length() + "||标准长度为：" + jarCleanR.get("charNum"));//失败原因
//                            errMap.put("failKind", "2");//记录清洗
//                            //                            errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                            errMap.put("failRecord", news);//清洗数据
//                            errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                            errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                            errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                            errMap.put("startNum", "");//开始量
//                            logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                            isOk = false;
//                        }
//                    }
//                    else
//                    {
//                        //读取配置文件中的分隔符
//                        String[] ls = v1.split(splitFlag, -1);
//                        //读取配置文件中的列数
//                        if (ls.length >= (int)jarCleanR.get("colNum"))
//                        {
//                            isOk = true;
//                        }
//                        else
//                        {
//                            Map<String, String> errMap = new HashMap<String, String>();
//                            errMap.put("failCode", Constants.JAR_DATA_CLEAR);//失败代码
//                            errMap.put("note", "数据列数清洗||数据列数为：" + ls.length + "||标准列数为：" + jarCleanR.get("colNum"));//失败原因
//                            errMap.put("failKind", "2");//记录清洗
//                            //                            errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                            errMap.put("failRecord", news);//清洗数据
//                            errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                            errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                            errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                            errMap.put("startNum", "");//开始量
//                            logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                            isOk = false;
//                        }
//                    }
//                }
//                return isOk;
//            }
//        });
//        
//        return s;
//    }
//    
//    /**
//     * 
//     * 注册基础数据表
//     * <功能详细描述>
//     * @param s
//     * @param hc
//     * @see [类、类#方法、类#成员]
//     */
//    @SuppressWarnings("unchecked")
//    private void registerBaseTable(JavaRDD<String> s, final Map<String, Object> jarCleanR,
//        final Map<Integer, Object> colCleanR)
//    {
//        //将第一遍数据记录入DATAFRAME
//        JavaRDD<Row> baseData = s.map(new Function<String, Row>()
//        {
//            
//            private static final long serialVersionUID = 1L;
//            
//            @Override
//            public Row call(String news)
//                throws Exception
//            {
//                
//                String v1 = news;
//                //返回每行数据，这里需要把第一个FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF截取掉
//                if (news.indexOf("FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF") > -1)
//                {
//                    v1 = news.substring(news.indexOf("FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF") + 42);
//                }
//                
//                String splitFlag = (String)jarCleanR.get("splitFlag");
//                //增加分隔符为空的，当分隔符为空时，按长度分隔
//                if (null == splitFlag || "".equals(splitFlag))
//                {
//                    Object[] objArr = new Object[colCleanR.size()];
//                    int i = 0;
//                    for (Integer key : colCleanR.keySet())
//                    {
//                        Object obj = new Object();
//                        Map<String, Object> colMap = (Map<String, Object>)colCleanR.get(key);
//                        String kind = (String)colMap.get("kind");
//                        if ("string".equals(kind))
//                        {
//                            obj = v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"));
//                        }
//                        else if ("int".equals(kind))
//                        {
//                            if (CommFunctionUtil
//                                .isInteger(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                    .replace(" ", "")))
//                            {
//                                obj = Integer
//                                    .valueOf(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                        .replace(" ", ""));
//                            }
//                            else
//                            {
//                                logger.error(Constants.LOG_FLAG + "|||" + "数据'"
//                                    + v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                    + "'非整型数据，转换失败");
//                                obj = 0;
//                                
//                            }
//                        }
//                        else if ("float".equals(kind))
//                        {
//                            if (CommFunctionUtil
//                                .isFloat(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                    .replace(" ", "")))
//                            {
//                                obj = Integer
//                                    .valueOf(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                        .replace(" ", ""));
//                            }
//                            else
//                            {
//                                logger.error(Constants.LOG_FLAG + "|||" + "数据'"
//                                    + v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                    + "'非浮点型数据，转换失败");
//                                obj = new Float(0);
//                            }
//                        }
//                        else if ("double".equals(kind))
//                        {
//                            if (CommFunctionUtil
//                                .isDouble(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))))
//                            {
//                                obj = Double
//                                    .valueOf(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum")));
//                            }
//                            else
//                            {
//                                logger.error(Constants.LOG_FLAG + "|||" + "数据'"
//                                    + v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                    + "'非DOUBLE型数据，转换失败");
//                            }
//                        }
//                        else if ("date".equals(kind))
//                        {
//                            obj = v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum")).trim();
//                        }
//                        else if ("timestamp".equals(kind))
//                        {
//                            obj = v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum")).trim();
//                        }
//                        objArr[i] = obj;
//                        i++;
//                    }
//                    return RowFactory.create(objArr);
//                }
//                else
//                {
//                    String[] sls = v1.split(splitFlag, -1);
//                    Object[] objArr = new Object[sls.length];
//                    for (int i = 0; i < sls.length; i++)
//                    {
//                        Object obj = new Object();
//                        Map<String, Object> colMap = (Map<String, Object>)colCleanR.get(i);
//                        String kind = (String)colMap.get("kind");
//                        if ("string".equals(kind))
//                        {
//                            obj = sls[i];
//                        }
//                        else if ("int".equals(kind))
//                        {
//                            if (CommFunctionUtil.isInteger(sls[i].replace(" ", "")))
//                            {
//                                obj = Integer.valueOf(sls[i].replace(" ", ""));
//                            }
//                            else
//                            {
//                                logger.error(Constants.LOG_FLAG + "|||" + "数据'" + sls[i] + "'非整型数据，转换失败");
//                                obj = 0;
//                            }
//                        }
//                        else if ("float".equals(kind))
//                        {
//                            if (CommFunctionUtil.isFloat(sls[i].replace(" ", "")))
//                            {
//                                obj = Float.valueOf(sls[i].replace(" ", ""));
//                            }
//                            else
//                            {
//                                logger.error(
//                                    Constants.LOG_FLAG + "|||" + "数据'" + sls[i].replace(" ", "") + "'非浮点型数据，转换失败");
//                                obj = new Float(0);
//                            }
//                        }
//                        else if ("date".equals(kind))
//                        {
//                            obj = sls[i].trim();
//                        }
//                        else if ("double".equals(kind))
//                        {
//                            if (CommFunctionUtil.isDouble(sls[i].replace(" ", "")))
//                            {
//                                obj = Double.valueOf(sls[i].replace(" ", ""));
//                            }
//                            else
//                            {
//                                logger.error(
//                                    Constants.LOG_FLAG + "|||" + "数据'" + sls[i].replace(" ", "") + "'非DOUBLE型数据，转换失败");
//                            }
//                        }
//                        else if ("timestamp".equals(kind))
//                        {
//                            obj = sls[i].trim();
//                        }
//                        objArr[i] = obj;
//                    }
//                    return RowFactory.create(objArr);
//                }
//                
//            }
//        });
//        List<StructField> structFields = new ArrayList<StructField>();
//        for (int i = 0; i < (int)jarCleanR.get("colNum"); i++)
//        {
//            Map<String, Object> colMap = (Map<String, Object>)colCleanR.get(i);
//            if (null != colMap)
//            {
//                String kind = (String)colMap.get("kind");
//                if ("string".equals(kind))
//                {
//                    structFields
//                        .add(DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.StringType, true));
//                }
//                else if ("int".equals(kind))
//                {
//                    structFields
//                        .add(DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.IntegerType, true));
//                }
//                else if ("float".equals(kind))
//                {
//                    structFields
//                        .add(DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.FloatType, true));
//                }
//                else if ("date".equals(kind))
//                {
//                    structFields
//                        .add(DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.DateType, true));
//                }
//                else if ("double".equals(kind))
//                {
//                    structFields
//                        .add(DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.DoubleType, true));
//                }
//                else if ("timestamp".equals(kind))
//                {
//                    structFields.add(
//                        DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.TimestampType, true));
//                }
//            }
//        }
//        StructType structType = DataTypes.createStructType(structFields);
//        DataFrame baseDataDF = sqlcontext.createDataFrame(baseData, structType);
//        baseDataDF.registerTempTable((String)jarCleanR.get("tableName") + "_init");
//    }
//    
//    /**
//     * 
//     * 数据列清洗
//     * <功能详细描述>
//     * @param baseS
//     * @return
//     * @see [类、类#方法、类#成员]
//     */
//    private JavaRDD<String> colCleanFun(JavaRDD<String> baseS, final Map<String, Object> jarCleanR,
//        final Map<Integer, Object> colCleanR, final Map<String, String> baseMap)
//    {
//        JavaRDD<String> cleanS = baseS.filter(new Function<String, Boolean>()
//        {
//            
//            private static final long serialVersionUID = 1L;
//            
//            @SuppressWarnings("unchecked")
//            @Override
//            public Boolean call(String news)
//                throws Exception
//            {
//                
//                String v1 = news;
//                //返回每行数据，这里需要把第一个FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF截取掉
//                if (news.indexOf("FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF") > -1)
//                {
//                    v1 = news.substring(news.indexOf("FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF") + 42);
//                }
//                
//                String splitFlag = (String)jarCleanR.get("splitFlag");
//                String ls[] = new String[(int)jarCleanR.get("colNum")];
//                String lsWithSpace[] = new String[(int)jarCleanR.get("colNum")];
//                
//                //增加分隔符为空的，当分隔符为空时，按长度分隔
//                if (null == splitFlag || "".equals(splitFlag))
//                {
//                    int i = 0;
//                    for (Integer key : colCleanR.keySet())
//                    {
//                        Map<String, Object> colMap = (Map<String, Object>)colCleanR.get(key);
//                        ls[i] = v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum")).trim();
//                        lsWithSpace[i] = v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"));
//                        i++;
//                    }
//                }
//                else
//                {
//                    lsWithSpace = v1.split(splitFlag, -1);
//                    ls = v1.split(splitFlag, -1);
//                }
//                
//                for (int i = 0; i < ls.length; i++)
//                {
//                    Map<String, Object> colMap = (Map<String, Object>)colCleanR.get(i);
//                    //判断该列是否需要清洗
//                    if (null != colMap && (boolean)colMap.get("isclean"))
//                    {
//                        //判断是否为空
//                        if (!(boolean)colMap.get("isnull") && (null == lsWithSpace[i] || "".equals(lsWithSpace[i])))
//                        {
//                            
//                            Map<String, String> errMap = new HashMap<String, String>();
//                            errMap.put("failCode", Constants.COL_DATA_CLEAR_ENUM);//失败代码
//                            errMap.put("note", "该列不可为空||数据值为：" + lsWithSpace[i]);//失败原因
//                            errMap.put("failKind", "3");//记录清洗
//                            //                            errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                            errMap.put("failRecord", news);//清洗数据
//                            errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                            errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                            errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                            errMap.put("startNum", String.valueOf(i + 1));//开始量
//                            logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                            return false;
//                        }
//                        //判断枚举值字段是否为空
//                        if (null != colMap.get("enum_value") && !"".equals(colMap.get("enum_value")))
//                        {
//                            String[] enumValues = colMap.get("enum_value").toString().split(",");
//                            //不在枚举值中，不在为true,在为false
//                            Boolean notEnum = true;
//                            for (int m = 0; m < enumValues.length; m++)
//                            {
//                                if (enumValues[m].equals(ls[i]))
//                                {
//                                    notEnum = false;
//                                }
//                                
//                            }
//                            if (notEnum)
//                            {
//                                Map<String, String> errMap = new HashMap<String, String>();
//                                errMap.put("failCode", Constants.COL_DATA_CLEAR_ENUM);//失败代码
//                                errMap.put("note",
//                                    "数据列清洗||数据值为：" + ls[i] + "||枚举数据为：" + String.valueOf(colMap.get("enum_value"))
//                                        .replace(",", "|")
//                                        .replace("\"", "|")
//                                        .replace("\'", "|"));//失败原因
//                                errMap.put("failKind", "3");//记录清洗
//                                //                                errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                                errMap.put("failRecord", news);//清洗数据
//                                errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                                errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                                errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                                errMap.put("startNum", String.valueOf(i + 1));//开始量
//                                logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                                return false;
//                            }
//                        }
//                        //判断是否为正则表达式过滤
//                        if (null != colMap.get("regular") && !"".equals(colMap.get("regular")))
//                        {
//                            Pattern pattern = Pattern.compile((String)colMap.get("regular"));
//                            if (!pattern.matcher(ls[i]).matches())
//                            {
//                                Map<String, String> errMap = new HashMap<String, String>();
//                                errMap.put("failCode", Constants.COL_DATA_CLEAR_RDGULAR);//失败代码
//                                errMap.put("note", "数据列清洗||数据值为：" + ls[i] + "||正则表达式为：" + colMap.get("regular"));//失败原因
//                                errMap.put("failKind", "3");//记录清洗
//                                //                                errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                                errMap.put("failRecord", news);//清洗数据
//                                errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                                errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                                errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                                errMap.put("startNum", String.valueOf(i + 1));//开始量
//                                logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                                return false;
//                            }
//                        }
//                        String kind = (String)colMap.get("kind");
//                        if ("string".equals(kind))
//                        {
//                            //判断长度
//                            if (colMap.get("length") != null && null != lsWithSpace[i]
//                                && !"".equals(colMap.get("length"))
//                                && (lsWithSpace[i].length() != (int)colMap.get("length")))
//                            {
//                                Map<String, String> errMap = new HashMap<String, String>();
//                                errMap.put("failCode", Constants.COL_DATA_CLEAR_STRING_LEANTH);//失败代码
//                                errMap.put("note", "数据列清洗||数据值为：" + lsWithSpace[i] + "||数据位长度：" + colMap.get("length"));//失败原因
//                                errMap.put("failKind", "3");//记录清洗
//                                //                                errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                                errMap.put("failRecord", news);//清洗数据
//                                errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                                errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                                errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                                errMap.put("startNum", String.valueOf(i + 1));//开始量
//                                logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                                return false;
//                            }
//                        }
//                        else if ("int".equals(kind))
//                        {
//                            if (CommFunctionUtil.isInteger(ls[i]))
//                            {
//                                int value = Integer.valueOf(ls[i]);
//                                //判断最大、最小值
//                                if (colMap.get("max_value") != null)
//                                {
//                                    if ((int)colMap.get("max_value") < value)
//                                    {
//                                        Map<String, String> errMap = new HashMap<String, String>();
//                                        errMap.put("failCode", Constants.COL_DATA_CLEAR_INT_MAX);//失败代码
//                                        errMap.put("note", "数据列清洗||数据值为：" + value + "||最大值：" + colMap.get("max_value"));//失败原因
//                                        errMap.put("failKind", "3");//记录清洗
//                                        //                                        errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                                        errMap.put("failRecord", news);//清洗数据
//                                        errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                                        errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                                        errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                                        errMap.put("startNum", String.valueOf(i + 1));//开始量
//                                        logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                                        return false;
//                                    }
//                                }
//                                if (colMap.get("min_value") != null)
//                                {
//                                    if ((int)colMap.get("min_value") > value)
//                                    {
//                                        Map<String, String> errMap = new HashMap<String, String>();
//                                        errMap.put("failCode", Constants.COL_DATA_CLEAR_INT_MIN);//失败代码
//                                        errMap.put("note", "数据列清洗||数据值为：" + value + "||最小值：" + colMap.get("min_value"));//失败原因
//                                        errMap.put("failKind", "3");//记录清洗
//                                        //                                        errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                                        errMap.put("failRecord", news);//清洗数据
//                                        errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                                        errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                                        errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                                        errMap.put("startNum", String.valueOf(i + 1));//开始量
//                                        logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                                        return false;
//                                    }
//                                }
//                            }
//                            else
//                            {
//                                Map<String, String> errMap = new HashMap<String, String>();
//                                errMap.put("failCode", Constants.COL_DATA_TYPE_ERROR);//失败代码
//                                errMap.put("note", "数据列清洗||数据值为：" + ls[i] + "||非有效int型");//失败原因
//                                errMap.put("failKind", "3");//记录清洗
//                                //                                errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                                errMap.put("failRecord", news);//清洗数据
//                                errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                                errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                                errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                                errMap.put("startNum", String.valueOf(i + 1));//开始量
//                                logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                                return false;
//                            }
//                            
//                        }
//                        else if ("float".equals(kind))
//                        {
//                            if (CommFunctionUtil.isFloat(ls[i]))
//                            {
//                                float value = Float.valueOf(ls[i]);
//                                //判断最大、最小值
//                                if (colMap.get("max_value") != null)
//                                {
//                                    if ((float)colMap.get("max_value") < value)
//                                    {
//                                        Map<String, String> errMap = new HashMap<String, String>();
//                                        errMap.put("failCode", Constants.COL_DATA_CLEAR_FLOAT_MAX);//失败代码
//                                        errMap.put("note", "数据列清洗||数据值为：" + value + "||最小值：" + colMap.get("min_value"));//失败原因
//                                        errMap.put("failKind", "3");//记录清洗
//                                        //                                        errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                                        errMap.put("failRecord", news);//清洗数据
//                                        errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                                        errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                                        errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                                        errMap.put("startNum", String.valueOf(i + 1));//开始量
//                                        logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                                        return false;
//                                    }
//                                }
//                                if (colMap.get("min_value") != null)
//                                {
//                                    if ((float)colMap.get("min_value") > value)
//                                    {
//                                        Map<String, String> errMap = new HashMap<String, String>();
//                                        errMap.put("failCode", Constants.COL_DATA_CLEAR_FLOAT_MIN);//失败代码
//                                        errMap.put("note", "数据列清洗||数据值为：" + value + "||最小值：" + colMap.get("min_value"));//失败原因
//                                        errMap.put("failKind", "3");//记录清洗
//                                        //                                        errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                                        errMap.put("failRecord", news);//清洗数据
//                                        errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                                        errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                                        errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                                        errMap.put("startNum", String.valueOf(i + 1));//开始量
//                                        logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                                        return false;
//                                    }
//                                }
//                            }
//                            else
//                            {
//                                Map<String, String> errMap = new HashMap<String, String>();
//                                errMap.put("failCode", Constants.COL_DATA_TYPE_ERROR);//失败代码
//                                errMap.put("note", "数据列清洗||数据值为：" + ls[i] + "||非有效float型");//失败原因
//                                errMap.put("failKind", "3");//记录清洗
//                                //                                errMap.put("failRecord", news.substring(news.indexOf(",") + 1));//清洗数据
//                                errMap.put("failRecord", news);//清洗数据
//                                errMap.put("serverId", baseMap.get("serverId"));//所属服务器
//                                errMap.put("jarId", baseMap.get("jarId"));//所属jar
//                                errMap.put("sourceId", baseMap.get("tableName"));//数据源
//                                errMap.put("startNum", String.valueOf(i + 1));//开始量
//                                logger.error(Constants.LOG_FLAG + "|||" + errMap);
//                                return false;
//                            }
//                            
//                        }
//                        else if ("date".equals(kind))
//                        {
//                            //判断格式是否正确
//                            //TODO
//                        }
//                    }
//                    
//                }
//                return true;
//            }
//        });
//        return cleanS;
//    }
//    
//    /**
//     * 
//     * 注册清先后的数据表
//     * <功能详细描述>
//     * @param cleanS
//     * @param jarCleanR
//     * @param colCleanR
//     * @param allRddMap
//     * @return
//     * @see [类、类#方法、类#成员]
//     */
//    @SuppressWarnings("unchecked")
//    private JavaRDD<Row> registerCleanTable(JavaRDD<String> cleanS, final Map<String, Object> jarCleanR,
//        final Map<Integer, Object> colCleanR, final Map<String, JavaRDD<Row>> allRddMap)
//    {
//        JavaRDD<Row> cleanData = cleanS.map(new Function<String, Row>()
//        {
//            
//            private static final long serialVersionUID = 1L;
//            
//            @Override
//            public Row call(String news)
//                throws Exception
//            {
//                String v1 = news;
//                //返回每行数据，这里需要把第一个FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF截取掉
//                if (news.indexOf("FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF") > -1)
//                {
//                    v1 = news.substring(news.indexOf("FFFFFa4b6f1e41849c064eef9d0d2a193cd00FFFFF") + 42);
//                }
//                
//                String splitFlag = (String)jarCleanR.get("splitFlag");
//                if (null == splitFlag || "".equals(splitFlag))
//                {
//                    Object[] objArr = new Object[colCleanR.size()];
//                    int i = 0;
//                    for (Integer key : colCleanR.keySet())
//                    {
//                        Object obj = new Object();
//                        Map<String, Object> colMap = (Map<String, Object>)colCleanR.get(key);
//                        String kind = (String)colMap.get("kind");
//                        if ("string".equals(kind))
//                        {
//                            obj = v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"));
//                        }
//                        else if ("int".equals(kind))
//                        {
//                            if (CommFunctionUtil
//                                .isInteger(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                    .replace(" ", "")))
//                            {
//                                obj = Integer
//                                    .valueOf(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                        .replace(" ", ""));
//                            }
//                            else
//                            {
//                                logger.error(Constants.LOG_FLAG + "|||" + "数据'"
//                                    + v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                    + "'非整型数据，转换失败");
//                                obj = 0;
//                                
//                            }
//                        }
//                        else if ("float".equals(kind))
//                        {
//                            if (CommFunctionUtil
//                                .isFloat(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                    .replace(" ", "")))
//                            {
//                                obj = Integer
//                                    .valueOf(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                        .replace(" ", ""));
//                            }
//                            else
//                            {
//                                logger.error(Constants.LOG_FLAG + "|||" + "数据'"
//                                    + v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                    + "'非浮点型数据，转换失败");
//                                obj = new Float(0);
//                            }
//                        }
//                        else if ("date".equals(kind))
//                        {
//                            obj = v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum")).trim();
//                        }
//                        else if ("double".equals(kind))
//                        {
//                            if (CommFunctionUtil
//                                .isDouble(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))))
//                            {
//                                obj = Double
//                                    .valueOf(v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum")));
//                            }
//                            else
//                            {
//                                logger.error(Constants.LOG_FLAG + "|||" + "数据'"
//                                    + v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum"))
//                                    + "'非DOUBLE型数据，转换失败");
//                            }
//                        }
//                        else if ("timestamp".equals(kind))
//                        {
//                            obj = v1.substring((int)colMap.get("startnum") - 1, (int)colMap.get("endnum")).trim();
//                        }
//                        objArr[i] = obj;
//                        i++;
//                    }
//                    return RowFactory.create(objArr);
//                }
//                else
//                {
//                    String[] sls = v1.split(splitFlag, -1);
//                    Object[] objArr = new Object[(int)jarCleanR.get("colNum")];
//                    //修改业务需求，在实际数据列数大于配置的数据列时，只需要取配置的数据列数
//                    //                    for (int i = 0; i < sls.length; i++)
//                    for (int i = 0; i < (int)jarCleanR.get("colNum"); i++)
//                    {
//                        Object obj = new Object();
//                        Map<String, Object> colMap = (Map<String, Object>)colCleanR.get(i);
//                        String kind = (String)colMap.get("kind");
//                        if ("string".equals(kind))
//                        {
//                            obj = sls[i];
//                        }
//                        else if ("int".equals(kind))
//                        {
//                            if (CommFunctionUtil.isInteger(sls[i].replace(" ", "")))
//                            {
//                                obj = Integer.valueOf(sls[i].replace(" ", ""));
//                            }
//                            else
//                            {
//                                logger.error(Constants.LOG_FLAG + "|||" + "数据'" + sls[i] + "'非整型数据，转换失败");
//                                obj = 0;
//                            }
//                        }
//                        else if ("float".equals(kind))
//                        {
//                            if (CommFunctionUtil.isFloat(sls[i].replace(" ", "")))
//                            {
//                                obj = Float.valueOf(sls[i].replace(" ", ""));
//                            }
//                            else
//                            {
//                                logger.error(
//                                    Constants.LOG_FLAG + "|||" + "数据'" + sls[i].replace(" ", "") + "'非浮点型数据，转换失败");
//                            }
//                        }
//                        else if ("date".equals(kind))
//                        {
//                            obj = sls[i].trim();
//                        }
//                        else if ("double".equals(kind))
//                        {
//                            if (CommFunctionUtil.isDouble(sls[i].replace(" ", "")))
//                            {
//                                obj = Double.valueOf(sls[i].replace(" ", ""));
//                            }
//                            else
//                            {
//                                logger.error(
//                                    Constants.LOG_FLAG + "|||" + "数据'" + sls[i].replace(" ", "") + "'非DOUBLE型数据，转换失败");
//                            }
//                        }
//                        else if ("timestamp".equals(kind))
//                        {
//                            obj = sls[i].trim();
//                        }
//                        objArr[i] = obj;
//                    }
//                    return RowFactory.create(objArr);
//                }
//                
//            }
//        });
//        
//        List<StructField> structFields = new ArrayList<StructField>();
//        for (int i = 0; i < (int)jarCleanR.get("colNum"); i++)
//        {
//            Map<String, Object> colMap = (Map<String, Object>)colCleanR.get(i);
//            if (null != colMap)
//            {
//                String kind = (String)colMap.get("kind");
//                if ("string".equals(kind))
//                {
//                    structFields
//                        .add(DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.StringType, true));
//                }
//                else if ("int".equals(kind))
//                {
//                    structFields
//                        .add(DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.IntegerType, true));
//                }
//                else if ("float".equals(kind))
//                {
//                    structFields
//                        .add(DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.FloatType, true));
//                }
//                else if ("date".equals(kind))
//                {
//                    structFields
//                        .add(DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.DateType, true));
//                }
//                else if ("double".equals(kind))
//                {
//                    structFields
//                        .add(DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.DoubleType, true));
//                }
//                else if ("timestamp".equals(kind))
//                {
//                    structFields.add(
//                        DataTypes.createStructField((String)colMap.get("fieldcode"), DataTypes.TimestampType, true));
//                }
//            }
//        }
//        StructType structType = DataTypes.createStructType(structFields);
//        
//        //存储增加数据，表名为全量表_back
//        allRddMap.put("addRdd", cleanData);
//        DataFrame addDataDF = sqlcontext.createDataFrame(cleanData, structType);
//        addDataDF.registerTempTable((String)jarCleanR.get("tableName") + "_bak");
//        
//        //明细数据全量存储
//        Boolean isSaveAllData = (Boolean)jarCleanR.get("isSaveAllData");
//        if (isSaveAllData)
//        {
//            //若不清除数据，则将数据累加
//            if (allRddMap.get("allRdd") != null)
//            {
//                cleanData = cleanData.union(allRddMap.get("allRdd"));
//            }
//            allRddMap.put("allRdd", cleanData);
//        }
//        DataFrame baseDataDF = sqlcontext.createDataFrame(cleanData, structType);
//        //清洗后数据入临时表
//        baseDataDF.registerTempTable((String)jarCleanR.get("tableName"));
//        baseDataDF.show();
//        return cleanData;
//        
//    }
//    
//    /**
//     * 
//     * 数据计算
//     * <功能详细描述>
//     * @param jarCleanR
//     * @param computeSQL
//     * @param sqlcontext
//     * @return
//     * @see [类、类#方法、类#成员]
//     */
//    @SuppressWarnings("unchecked")
//    private DataFrame dataCompute(final Map<String, Object> jarCleanR, final Map<Integer, Object> computeSQL,
//        final SQLContext sqlcontext)
//    {
//        DataFrame df = null;
//        SQLLogModel sqlLog = new SQLLogModel();
//        //结果集按次被调用，不存在跨记录调用
//        for (int i = 0; i < computeSQL.size(); i++)
//        {
//            Map<String, String> oneMap = (Map<String, String>)computeSQL.get(i);
//            
//            sqlLog = new SQLLogModel();
//            sqlLog.setAppId(appId);
//            sqlLog.setServerId(serverId);
//            sqlLog.setJarId(jarId);
//            sqlLog.setKind(new BigDecimal(3));
//            sqlLog.setSeq(new BigDecimal(i + 1));
//            sqlLog.setJarSql(oneMap.get("sql"));
//            Date startTime = new Date();
//            Date endTime = new Date();
//            try
//            {
//                dfMap.put(oneMap.get("tableName"), null);
//                dfMap.put(oneMap.get("tableName"), sqlcontext.sql(oneMap.get("sql")));
//                dfMap.get(oneMap.get("tableName")).registerTempTable(oneMap.get("tableName"));
//                
//                logger.error(Constants.LOG_FLAG + "|||" + "运行SQL：" + oneMap.get("sql"));
//                logger.error(Constants.LOG_FLAG + "|||" + "注册临时表：" + oneMap.get("tableName"));
//                
//                df = dfMap.get(oneMap.get("tableName"));
//                //1：正常结束，2：异常中止
//                sqlLog.setState(new BigDecimal(1));
//                
//                endTime = new Date();
//            }
//            catch (Exception e)
//            {
//                logger.error(Constants.LOG_FLAG + "|||" + "运行SQL异常,请确认SQL是否正确：" + oneMap.get("sql"));
//                logger.error(Constants.LOG_FLAG + "|||" + e.toString());
//                sqlLog.setNote(e.toString());
//                //1：正常结束，2：异常中止
//                sqlLog.setState(new BigDecimal(2));
//                endTime = new Date();
//                
//                logger.error(Constants.LOG_FLAG + "|||运行SQL异常,该异常导致程序无法正常完成，现中止程序");
//                //SQL执行错误，程序停止
//                System.exit(0);
//            }
//            sqlLog.setStartTime(startTime);
//            sqlLog.setEndTime(endTime);
//            sqlLog.setElapse(new BigDecimal(endTime.getTime() - startTime.getTime()));
//            logList.add(sqlLog);
//        }
//        return df;
//    }
//    
//    //KAFKA
//    private Boolean pushDataToKafka(DataFrame cleanDF, final DataBaseInfo outDBInfo)
//    {
//        JavaRDD<Row> cleanR = cleanDF.javaRDD();
//        cleanR.foreach(new VoidFunction<Row>()
//        {
//            
//            private static final long serialVersionUID = 1L;
//            
//            @Override
//            public void call(Row t)
//                throws Exception
//            {
//                
//                //这边只是示例，Kafka应该全局生成唯一长连接
//                new KafkaProducer(outDBInfo.getDbUrl()).produce(t.get(0) + "," + t.get(1), outDBInfo.getTableName());
//                
//            }
//        });
//        return null;
//    }
//    
//    /**
//     * 
//     * 数据存入ORCAL库
//     * <功能详细描述>
//     * @param cleanDF
//     * @param sqlContext
//     * @param dbInfo
//     * @param outSql
//     * @param tableName
//     * @param runMode
//     * @param computeSize
//     * @return
//     * @see [类、类#方法、类#成员]
//     */
//    private Boolean pushDataToOrcal(DataFrame cleanDF, SQLContext sqlContext, DataBaseInfo dbInfo, String outSql,
//        String tableName, String runMode, long computeSize, int seq)
//    {
//        SQLLogModel sqlLog = new SQLLogModel();
//        Date startTime = new Date();
//        Date endTime = new Date();
//        if (computeSize > 0)
//        {
//            cleanDF.registerTempTable(tableName);
//        }
//        
//        if (Constants.RUN_MODE_JDBC.equals(runMode))
//        {
//            Connection conn = DataBaseUtil.getConnection(dbInfo);
//            Statement stmt = null;
//            PreparedStatement ps = null;
//            try
//            {
//                sqlLog = new SQLLogModel();
//                sqlLog.setAppId(appId);
//                sqlLog.setServerId(serverId);
//                sqlLog.setJarId(jarId);
//                //5:输出，3：计算
//                sqlLog.setKind(new BigDecimal(5));
//                sqlLog.setSeq(new BigDecimal(seq));
//                sqlLog.setJarSql(outSql);
//                //判断是否为请除数据
//                if (CommFunctionUtil.isNull(outSql) || outSql.equals("null"))
//                {
//                    if (outList != null && outList.size() > 0)
//                    {
//                        StringBuffer tempSql = new StringBuffer();
//                        tempSql.append("INSERT INTO " + tableName + "(");
//                        for (StructField field : outFields)
//                        {
//                            tempSql.append(field.name() + ",");
//                        }
//                        tempSql.append(") VALUES (");
//                        
//                        for (int i = 0; i < outFields.length; i++)
//                        {
//                            tempSql.append("?,");
//                        }
//                        tempSql.append(")");
//                        String sql = tempSql.toString().replace(",)", ")");
//                        
//                        System.err.println("start-time:" + new Date());
//                        
//                        conn.setAutoCommit(false);
//                        ps = conn.prepareStatement(sql);
//                        int rowNum = 0;
//                        for (Row r : outList)
//                        {
//                            rowNum++;
//                            for (int i = 0; i < outFields.length; i++)
//                            {
//                                String type = outFields[i].dataType().typeName();
//                                if (type.equals("string"))
//                                {
//                                    ps.setString(i + 1, String.valueOf(r.get(i)));
//                                }
//                                else if (type.equals("timestamp"))
//                                {
//                                    ps.setTimestamp(i + 1, r.getTimestamp(i));
//                                }
//                                else
//                                {
//                                    ps.setBigDecimal(i + 1, new BigDecimal(r.get(i).toString()));
//                                }
//                            }
//                            ps.addBatch();
//                            if (rowNum > 2000)
//                            {
//                                ps.executeBatch();
//                                conn.commit();
//                                ps.clearBatch();
//                                rowNum = 0;
//                            }
//                        }
//                        ps.executeBatch();
//                        conn.commit();
//                        
//                        System.err.println("end-time:" + new Date());
//                        logger.error(Constants.LOG_FLAG + "|||" + "存入数据库存记录条数为：" + outList.size());
//                    }
//                }
//                else
//                {
//                    stmt = conn.createStatement();
//                    //替换指定的时间标
//                    if (outSql.indexOf("$TODAY_D$") > 0)
//                    {
//                        outSql = outSql.replace("$TODAY_D$", TODAY_D);
//                    }
//                    else if (outSql.indexOf("$TODAY_H$") > 0)
//                    {
//                        outSql = outSql.replace("$TODAY_H$", TODAY_H);
//                    }
//                    else if (outSql.indexOf("$TODAY_M$") > 0)
//                    {
//                        outSql = outSql.replace("$TODAY_M$", TODAY_M);
//                    }
//                    else if (outSql.indexOf("$TODAY_S$") > 0)
//                    {
//                        outSql = outSql.replace("$TODAY_S$", TODAY_S);
//                    }
//                    
//                    //替换指定的时间标
//                    if (outSql.indexOf("$TODAYD$") > 0)
//                    {
//                        outSql = outSql.replace("$TODAYD$", TODAYD);
//                    }
//                    else if (outSql.indexOf("$TODAYH$") > 0)
//                    {
//                        outSql = outSql.replace("$TODAYH$", TODAYH);
//                    }
//                    else if (outSql.indexOf("$TODAYM$") > 0)
//                    {
//                        outSql = outSql.replace("$TODAYM$", TODAYM);
//                    }
//                    else if (outSql.indexOf("$TODAYS$") > 0)
//                    {
//                        outSql = outSql.replace("$TODAYS$", TODAYS);
//                    }
//                    
//                    stmt.execute(outSql);
//                    logger.error(Constants.LOG_FLAG + "|||" + "清除旧数据,SQL:" + outSql);
//                }
//                
//                //1：正常结束，2：异常中止
//                sqlLog.setState(new BigDecimal(1));
//                endTime = new Date();
//                sqlLog.setStartTime(startTime);
//                sqlLog.setEndTime(endTime);
//                sqlLog.setElapse(new BigDecimal(endTime.getTime() - startTime.getTime()));
//                logList.add(sqlLog);
//            }
//            catch (Exception e)
//            {
//                e.printStackTrace();
//                logger.error(Constants.LOG_FLAG + "|||" + "输出SQL异常或输出数据源异常，异常信息为：" + e.toString());
//                
//                //1：正常结束，2：异常中止
//                sqlLog.setState(new BigDecimal(2));
//                endTime = new Date();
//                sqlLog.setStartTime(startTime);
//                sqlLog.setEndTime(endTime);
//                sqlLog.setElapse(new BigDecimal(endTime.getTime() - startTime.getTime()));
//                sqlLog.setNote(e.toString());
//                logList.add(sqlLog);
//                
//                logger.error(Constants.LOG_FLAG + "|||ORCAL输出异常,该异常导致程序无法正常完成，现中止程序");
//                //SQL执行错误，程序停止
//                System.exit(0);
//                return false;
//            }
//            finally
//            {
//                DataBaseUtil.closeConnection(null, ps, null);
//                DataBaseUtil.closeConnection(null, stmt, conn);
//            }
//        }
//        else
//        {
//            sqlLog = new SQLLogModel();
//            sqlLog.setAppId(appId);
//            sqlLog.setServerId(serverId);
//            sqlLog.setJarId(jarId);
//            sqlLog.setKind(new BigDecimal(3));
//            sqlLog.setSeq(new BigDecimal(seq));
//            sqlLog.setJarSql(outSql);
//            
//            DataFrame outClean = sqlContext.sql(outSql);
//            outList = outClean.collectAsList();
//            StructType structtype = outClean.schema();
//            outFields = structtype.fields();
//            logger.error(Constants.LOG_FLAG + "|||" + "需存库记录为：" + outList.size());
//            
//            //1：正常结束，2：异常中止
//            sqlLog.setState(new BigDecimal(1));
//            endTime = new Date();
//            sqlLog.setStartTime(startTime);
//            sqlLog.setEndTime(endTime);
//            sqlLog.setElapse(new BigDecimal(endTime.getTime() - startTime.getTime()));
//            logList.add(sqlLog);
//        }
//        return true;
//    }
//    
//    /**
//     * 
//     * 数据存入HIVE
//     * <功能详细描述>
//     * @param cleanDF
//     * @param sqlContext
//     * @param hiveContext
//     * @param dbInfo
//     * @param outSql
//     * @param tableName
//     * @return
//     * @see [类、类#方法、类#成员]
//     */
//    private Boolean pushDataToHive(DataFrame cleanDF, SQLContext sqlContext, HiveContext hiveContext,
//        DataBaseInfo dbInfo, String outSql, final String tableName, String runMode, Long computeSize, int seq)
//    {
//        SQLLogModel sqlLog = new SQLLogModel();
//        Date startTime = new Date();
//        Date endTime = new Date();
//        if (computeSize > 0)
//        {
//            cleanDF.registerTempTable(tableName);
//        }
//        try
//        {
//            
//            sqlLog.setAppId(appId);
//            sqlLog.setServerId(serverId);
//            sqlLog.setJarId(jarId);
//            //5：输出，3：计算
//            sqlLog.setKind(new BigDecimal(5));
//            //TODO
//            sqlLog.setSeq(new BigDecimal(seq));
//            sqlLog.setJarSql(outSql);
//            
//            //判断是否为请除数据
//            if (Constants.RUN_MODE_HIVE_SYNCH.equals(runMode))
//            {
//                hiveContext.sql(outSql);
//            }
//            else
//            {
//                
//                DataFrame outClean = sqlContext.sql(outSql);
//                
//                DataFrame newDF = hiveContext.createDataFrame(outClean.toJavaRDD(), outClean.schema());
//                newDF.registerTempTable("tmp");
//                
//                if (Constants.RUN_MODE_HIVE_NEW.equals(runMode))
//                {
//                    hiveContext.sql("insert into " + tableName + " select * from tmp");
//                    logger.error(Constants.LOG_FLAG + "|||运行SQL：" + "insert into " + tableName + " select * from tmp");
//                }
//                else
//                {
//                    hiveContext.sql("insert overwrite table " + tableName + " select * from tmp");
//                    logger.error(Constants.LOG_FLAG + "|||运行SQL：" + "insert overwrite table " + tableName
//                        + " select * from tmp");
//                }
//            }
//            //1：正常结束，2：异常中止
//            sqlLog.setState(new BigDecimal(1));
//            endTime = new Date();
//            sqlLog.setStartTime(startTime);
//            sqlLog.setEndTime(endTime);
//            sqlLog.setElapse(new BigDecimal(endTime.getTime() - startTime.getTime()));
//            logList.add(sqlLog);
//        }
//        catch (Exception e)
//        {
//            e.printStackTrace();
//            logger.error(Constants.LOG_FLAG + "|||" + "输出SQL异常或输出数据源异常，输出SQL为：" + outSql);
//            logger.error(Constants.LOG_FLAG + "|||" + e.toString());
//            
//            //1：正常结束，2：异常中止
//            sqlLog.setState(new BigDecimal(2));
//            endTime = new Date();
//            sqlLog.setStartTime(startTime);
//            sqlLog.setEndTime(endTime);
//            sqlLog.setElapse(new BigDecimal(endTime.getTime() - startTime.getTime()));
//            sqlLog.setNote(e.toString());
//            logList.add(sqlLog);
//            
//            logger.error(Constants.LOG_FLAG + "|||HIVE输出异常,该异常导致程序无法正常完成，现中止程序");
//            //SQL执行错误，程序停止
//            System.exit(0);
//            return false;
//        }
//        return true;
//    }
//    
//    //MYSQL
//    private Boolean pushDataToMysql(DataFrame cleanDF, SQLContext sqlContext, DataBaseInfo dbInfo, String outSql,
//        final String tableName)
//    {
//        Properties properties = new Properties();
//        properties.setProperty("driver", dbInfo.getDbDriver());
//        properties.setProperty("user", dbInfo.getUserName());
//        properties.setProperty("password", dbInfo.getPassword());
//        
//        cleanDF.registerTempTable(tableName);
//        DataFrame outClean = sqlContext.sql(outSql);
//        try
//        {
//            outClean.write().mode(SaveMode.Append).jdbc(dbInfo.getDbUrl(), tableName, properties);
//        }
//        catch (Exception e)
//        {
//            e.printStackTrace();
//            return false;
//        }
//        return true;
//    }
//    
//    //文本
//    //TODO 暂不做
//    private Boolean pushDataToText(DataFrame cleanDF, SQLContext sqlContext, String outSql, String ip, String path,
//        String useName, String passWord, String tableName, String spiltFlag)
//    {
//        //        FtpUtil ftp = new FtpUtil();
//        try
//        {
//            
//            DataFrame outDF = sqlContext.sql(outSql);
//            logger.error(Constants.LOG_FLAG + "|||输出SQL：" + outSql);
//            
//            Calendar cal = Calendar.getInstance();
//            int year = cal.get(Calendar.YEAR);//获取年份
//            int month = cal.get(Calendar.MONTH) + 1;//获取月份
//            int day = cal.get(Calendar.DATE);//获取日
//            int hour = cal.get(Calendar.HOUR_OF_DAY);//24小时制
//            int minute = cal.get(Calendar.MINUTE);//分 
//            int second = cal.get(Calendar.SECOND);//秒 
//            //R0001.{yyyyMMdd}.{hhmmss}.dat
//            if (tableName.indexOf("{yyyyMMdd}") > 0)
//            {
//                tableName = tableName.replace("{yyyyMMdd}", toTwoString(year) + toTwoString(month) + toTwoString(day));
//            }
//            if (tableName.indexOf("{hhmmss}") > 0)
//            {
//                tableName =
//                    tableName.replace("{hhmmss}", toTwoString(hour) + toTwoString(minute) + toTwoString(second));
//            }
//            
//            //判断是否存在本地相同目录
//            if (CheckPathOk(path))
//            {
//                String filePath = path + tableName;
//                //            //TODO 本地测试
//                //            String filePath = "D:/tmp/" + tableName;
//                logger.error(Constants.LOG_FLAG + "|||生成本地文件目录为：" + filePath);
//                List<Row> dataList = outDF.collectAsList();
//                logger.error(Constants.LOG_FLAG + "|||数据集转成LIST,数据集长度为：" + dataList.size());
//                BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filePath), true));
//                logger.error(Constants.LOG_FLAG + "|||生成本地文件成功：" + filePath);
//                StringBuffer message = new StringBuffer();
//                for (Row r : dataList)
//                {
//                    message = new StringBuffer();
//                    for (int i = 0; i < r.size(); i++)
//                    {
//                        if (i == r.size() - 1)
//                        {
//                            message.append(r.get(i));
//                        }
//                        else
//                        {
//                            message.append(r.get(i)).append(spiltFlag);
//                        }
//                    }
//                    message.append("\r\n");
//                    logger.error(Constants.LOG_FLAG + "|||插入消息体为：" + message.toString());
//                    writer.write(message.toString());
//                }
//                writer.close();
//                
//                logger.error(
//                    Constants.LOG_FLAG + "|||远程服务器信息,ip:" + ip + ",useName:" + useName + ",passWord:" + passWord);
//                    //                    
//                    //                //将写到本地的文件上传到相应的FTP
//                    //                FTPUtil ftpUtil = new FTPUtil();
//                    //                //创建连接
//                    //                ftp = ftpUtil.connect(ip, 21, useName, passWord);
//                    //                //上传文件：
//                    //                ftpUtil.upload(path, filePath, ftp);
//                    //                //关闭会话
//                    //                ftp.getSession().disconnect();
//                    //                
//                    //                logger.error(Constants.LOG_FLAG + "|||" + "FTP远程文件上传成功");
//                    
//                //                //将写到本地的文件上传到相应的FTP                
//                //                boolean isLogin = false;
//                //                
//                //                //登录ftp
//                //                ftp.setArg(useName, passWord, ip, 21);
//                //                isLogin = ftp.connectServer();
//                //                if (isLogin == true)
//                //                {
//                //                    logger.error(Constants.LOG_FLAG + "|||" + "连接ftp成功");
//                //                    
//                //                    boolean isUpload = ftp.uploadFile(filePath, tableName, path);
//                //                    if (isUpload)
//                //                    {
//                //                        logger.error(Constants.LOG_FLAG + "|||" + "文件上传FTP成功");
//                //                    }
//                //                    else
//                //                    {
//                //                        logger.error(Constants.LOG_FLAG + "|||" + "文件上传FTP失败");
//                //                    }
//                //                }
//                //                else
//                //                {
//                //                    logger.error(Constants.LOG_FLAG + "|||" + "连接ftp失败");
//                //                }
//                
//                Process process = null;
//                String command = "/home/service/ftp.sh " + ip + " " + useName + " " + passWord + " " + path + " " + path
//                    + " " + tableName;
//                    
//                logger.error(Constants.LOG_FLAG + "|||执行上传FTP命令：" + command);
//                
//                process = Runtime.getRuntime().exec(command);
//                process.waitFor();
//            }
//            else
//            {
//                logger.error(Constants.LOG_FLAG + "|||" + "本地无法创建相同的目录，不能生成本地文件");
//            }
//        }
//        catch (Exception e)
//        {
//            e.printStackTrace();
//            logger.error(Constants.LOG_FLAG + "|||" + "生成文文件或上传远程服务异常,错误信息为：" + e.toString());
//            
//            logger.error(Constants.LOG_FLAG + "|||文本输出异常,该异常导致程序无法正常完成，现中止程序");
//            //SQL执行错误，程序停止
//            System.exit(0);
//        }
//        //        finally
//        //        {
//        //            if (ftp.connectServer())
//        //            {
//        //                ftp.closeConnect();
//        //            }
//        //        }
//        return null;
//    }
//    
//    //GP
//    //TODO 暂不做
//    private Boolean pushDataToGPl()
//    {
//        return null;
//    }
//    
//    /**
//     * 
//     * 将数据存入HBASE
//     * <功能详细描述>
//     * @param cleanDF
//     * @param sqlContext
//     * @param dbInfo
//     * @param outSql
//     * @param tableName
//     * @return
//     * @see [类、类#方法、类#成员]
//     */
//    private Boolean pushDataToHbase(DataFrame cleanDF, SQLContext sqlContext, DataBaseInfo dbInfo, String outSql,
//        final String tableName, String runMode, long computeSize, int seq)
//    {
//        
//        SQLLogModel sqlLog = new SQLLogModel();
//        Date startTime = new Date();
//        Date endTime = new Date();
//        
//        if (computeSize > 0)
//        {
//            cleanDF.registerTempTable(tableName);
//        }
//        
//        if (Constants.RUN_MODE_JDBC.equals(runMode))
//        {
//            logger.error(Constants.LOG_FLAG + "|||进入pushDataToHbase:" + outSql);
//            
//            sqlLog = new SQLLogModel();
//            sqlLog.setAppId(appId);
//            sqlLog.setServerId(serverId);
//            sqlLog.setJarId(jarId);
//            //5:输出，3：计算
//            sqlLog.setKind(new BigDecimal(5));
//            sqlLog.setSeq(new BigDecimal(seq));
//            sqlLog.setJarSql(outSql);
//            
//            Connection conn = DataBaseUtil.getConnection(dbInfo);
//            PreparedStatement ps = null;
//            Statement stmt = null;
//            try
//            {
//                //判断是否为请除数据
//                if (CommFunctionUtil.isNull(outSql) || outSql.equals("null"))
//                {
//                    if (outList != null && outList.size() > 0)
//                    {
//                        StringBuffer tempSql = new StringBuffer();
//                        tempSql.append("INSERT INTO " + tableName + "(");
//                        for (StructField field : outFields)
//                        {
//                            tempSql.append(field.name() + ",");
//                        }
//                        tempSql.append(") VALUES (");
//                        
//                        for (int i = 0; i < outFields.length; i++)
//                        {
//                            tempSql.append("?,");
//                        }
//                        tempSql.append(")");
//                        String sql = tempSql.toString().replace(",)", ")");
//                        
//                        System.err.println("start-time:" + new Date());
//                        
//                        ps = conn.prepareStatement(sql);
//                        conn.setAutoCommit(false);
//                        int rowNum = 0;
//                        for (Row r : outList)
//                        {
//                            rowNum++;
//                            for (int i = 0; i < outFields.length; i++)
//                            {
//                                String type = outFields[i].dataType().typeName();
//                                if (type.equals("string"))
//                                {
//                                    ps.setString(i + 1, String.valueOf(r.get(i)));
//                                }
//                                else if (type.equals("timestamp"))
//                                {
//                                    ps.setTimestamp(i + 1, r.getTimestamp(i));
//                                }
//                                else
//                                {
//                                    ps.setBigDecimal(i + 1, new BigDecimal(r.get(i).toString()));
//                                }
//                            }
//                            ps.addBatch();
//                            if (rowNum > 1000)
//                            {
//                                ps.executeBatch();
//                                conn.commit();
//                                ps.clearBatch();
//                                rowNum = 0;
//                            }
//                        }
//                        ps.executeBatch();
//                        conn.commit();
//                    }
//                }
//                else
//                {
//                    stmt = conn.createStatement();
//                    stmt.execute(outSql);
//                    logger.error(Constants.LOG_FLAG + "|||" + "执行SQL:" + outSql);
//                }
//                
//                //1：正常结束，2：异常中止
//                sqlLog.setState(new BigDecimal(1));
//                endTime = new Date();
//                sqlLog.setStartTime(startTime);
//                sqlLog.setEndTime(endTime);
//                sqlLog.setElapse(new BigDecimal(endTime.getTime() - startTime.getTime()));
//                logList.add(sqlLog);
//            }
//            catch (Exception e)
//            {
//                e.printStackTrace();
//                logger.error(Constants.LOG_FLAG + "|||" + "存入数据库存异常：" + e.toString());
//                
//                //1：正常结束，2：异常中止
//                sqlLog.setState(new BigDecimal(2));
//                endTime = new Date();
//                sqlLog.setStartTime(startTime);
//                sqlLog.setEndTime(endTime);
//                sqlLog.setElapse(new BigDecimal(endTime.getTime() - startTime.getTime()));
//                sqlLog.setNote(e.toString());
//                logList.add(sqlLog);
//                
//                logger.error(Constants.LOG_FLAG + "|||HBASE输出异常,该异常导致程序无法正常完成，现中止程序");
//                //SQL执行错误，程序停止
//                System.exit(0);
//                
//                return false;
//            }
//            finally
//            {
//                DataBaseUtil.closeConnection(null, ps, null);
//                DataBaseUtil.closeConnection(null, stmt, conn);
//            }
//            System.err.println("end-time:" + new Date());
//            if (null != outList && outList.size() > 0)
//            {
//                logger.error(Constants.LOG_FLAG + "|||" + "存入数据库存记录条数为：" + outList.size());
//            }
//        }
//        else
//        {
//            
//            sqlLog = new SQLLogModel();
//            sqlLog.setAppId(appId);
//            sqlLog.setServerId(serverId);
//            sqlLog.setJarId(jarId);
//            //5:输出，3：计算
//            sqlLog.setKind(new BigDecimal(3));
//            sqlLog.setSeq(new BigDecimal(seq));
//            sqlLog.setJarSql(outSql);
//            
//            DataFrame outClean = sqlContext.sql(outSql);
//            outList = outClean.collectAsList();
//            StructType structtype = outClean.schema();
//            outFields = structtype.fields();
//            
//            //1：正常结束，2：异常中止
//            sqlLog.setState(new BigDecimal(1));
//            endTime = new Date();
//            sqlLog.setStartTime(startTime);
//            sqlLog.setEndTime(endTime);
//            sqlLog.setElapse(new BigDecimal(endTime.getTime() - startTime.getTime()));
//            logList.add(sqlLog);
//        }
//        return true;
//    }
//    
//    /**
//     * 
//     * 将INT型转成两位STRING
//     * <功能详细描述>
//     * @param value
//     * @return
//     * @see [类、类#方法、类#成员]
//     */
//    private String toTwoString(int value)
//    {
//        if (value < 10)
//        {
//            return "0" + String.valueOf(value);
//        }
//        else
//        {
//            return String.valueOf(value);
//        }
//    }
//    
//    private Boolean isEmptyResultToFtpNullFile()
//    {
//        try
//        {
//            for (int i = 0; i < outSQL.size(); i++)
//            {
//                
//                Map<String, String> oneMap = (Map<String, String>)outSQL.get(i);
//                outDBInfo = CommFunctionUtil.getDataSource(oneMap.get("sourceId"), sqlcontext, getDBInfo());
//                if (Constants.DB_TYPE_FTP.equals(outDBInfo.getDbType()))
//                {
//                    String tableName = oneMap.get("tableName");
//                    String path = outDBInfo.getDbUrl();
//                    Calendar cal = Calendar.getInstance();
//                    int year = cal.get(Calendar.YEAR);//获取年份
//                    int month = cal.get(Calendar.MONTH) + 1;//获取月份
//                    int day = cal.get(Calendar.DATE);//获取日
//                    int hour = cal.get(Calendar.HOUR_OF_DAY);//24小时制
//                    int minute = cal.get(Calendar.MINUTE);//分 
//                    int second = cal.get(Calendar.SECOND);//秒 
//                    //R0001.{yyyyMMdd}.{hhmmss}.dat
//                    if (tableName.indexOf("{yyyyMMdd}") > 0)
//                    {
//                        tableName =
//                            tableName.replace("{yyyyMMdd}", toTwoString(year) + toTwoString(month) + toTwoString(day));
//                    }
//                    if (tableName.indexOf("{hhmmss}") > 0)
//                    {
//                        tableName = tableName.replace("{hhmmss}",
//                            toTwoString(hour) + toTwoString(minute) + toTwoString(second));
//                    }
//                    //判断是否存在本地相同目录
//                    if (CheckPathOk(path))
//                    {
//                        String filePath = path + tableName;
//                        //            //TODO 本地测试
//                        //            String filePath = "D:/tmp/" + tableName;
//                        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filePath), true));
//                        
//                        writer.close();
//                        
//                        //                        //将写到本地的文件上传到相应的FTP
//                        //                        FTPUtil_S ftpUtil = new FTPUtil_S();
//                        //                        //创建连接
//                        //                        ChannelSftp ftp =
//                        //                            ftpUtil.connect(outDBInfo.getIp(), 22, outDBInfo.getUserName(), outDBInfo.getPassword());
//                        //                        //上传文件：
//                        //                        ftpUtil.upload(path, filePath, ftp);
//                        //                        //关闭会话
//                        //                        ftp.getSession().disconnect();
//                        //                        
//                        //                        logger.error(Constants.LOG_FLAG + "|||" + "FTP远程文件上传成功");
//                        
//                        //                Process process = null;
//                        //                String command = "/home/service/ftpput.sh";
//                        //                process = Runtime.getRuntime().exec(command);
//                        //                process.waitFor();
//                        
//                        Process process = null;
//                        String command = "/home/service/ftp.sh " + outDBInfo.getIp() + " " + outDBInfo.getUserName()
//                            + " " + outDBInfo.getPassword() + " " + path + " " + path + " " + tableName;
//                            
//                        logger.error(Constants.LOG_FLAG + "|||执行上传FTP命令：" + command);
//                        
//                        process = Runtime.getRuntime().exec(command);
//                        process.waitFor();
//                        
//                    }
//                    else
//                    {
//                        logger.error(Constants.LOG_FLAG + "|||" + "本地无法创建相同的目录，不能生成本地文件，导至FTP远程文件创建失败");
//                    }
//                }
//            }
//        }
//        catch (Exception e)
//        {
//            e.printStackTrace();
//            logger.error(Constants.LOG_FLAG + "|||" + "生成文文件或上传远程服务异常,错误信息为：" + e.toString());
//        }
//        return null;
//    }
//    
//    /**
//     * 
//     * 验证本地是否存在该目录
//     * <功能详细描述>
//     * @param path 文件目录
//     * @return
//     * @see [类、类#方法、类#成员]
//     */
//    private static Boolean CheckPathOk(String path)
//    {
//        Boolean returnFlag = true;
//        File file = new File(path);
//        //如果文件夹不存在则创建 
//        if (!file.exists() && !file.isDirectory())
//        {
//            logger.error(Constants.LOG_FLAG + "|||" + "本地文件目录不存在：" + path + ",创建中");
//            file.mkdirs();
//            
//            if (!file.exists() && !file.isDirectory())
//            {
//                logger.error(Constants.LOG_FLAG + "|||" + "本地文件目录不存在：" + path + ",系统创建失败");
//                
//                returnFlag = false;
//            }
//            else
//            {
//                logger.error(Constants.LOG_FLAG + "|||" + "系统已成功创建本地目录：" + path);
//            }
//        }
//        else
//        {
//            logger.error(Constants.LOG_FLAG + "|||" + "本地文件目录存在：" + path);
//        }
//        return returnFlag;
//    }
//}
