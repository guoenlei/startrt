//package com.ald.stat.test
//object HbaseTest {
//  val hbaseConf = org.apache.hadoop.hbase.HBaseConfiguration.create()
//  hbaseConf.set("hbase.zookeeper.quorum","10.0.100.13,10.0.100.8,10.0.100.14")
//  hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
//  val cn = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf)
//  val hbaseTable=cn.getTable(org.apache.hadoop.hbase.TableName.valueOf("STAT.ALDSTAT_ADVERTISE_OPENID")).asInstanceOf[org.apache.hadoop.hbase.client.HTable]
//  val scan =new Scan()
//  val pf = new PageFilter(5)
//  scan.setFilter(pf)
////  hbaseTable.setAutoFlushTo(false)
//
//  val rowFilter2 = new RowFilter(CompareFilter.CompareOp. EQUAL,new SubstringComparator( "cbf2dd66044b493bd0a867a54293c516"))
//  val cf = Bytes.toBytes("common")
//  val column = Bytes.toBytes("value")
//  val min = Bytes.toBytes("1566489600")
//  val max = Bytes.toBytes("1566575999")
//  val minC =new SingleColumnValueFilter(cf, column, CompareOp.GREATER,min);
//  val maxC =new SingleColumnValueFilter(cf, column, CompareOp.GREATER,max);
//  val list = new FilterList(FilterList.Operator.MUST_PASS_ALL)
//  list.addFilter(rowFilter2)
//  list.addFilter(minC)
//  list.addFilter(maxC)
//  val scan =new Scan()
//  scan.setFilter(list)
////  val result = hbaseTable.getScanner(scan)
//  hbaseConf.set(TableInputFormat.INPUT_TABLE, "STAT.ALDSTAT_ADVERTISE_OPENID")
//
////  import org.apache.hadoop.hbase.client.coprocessor.AggregationClient
//
////  val agg = new AggregationClient(hbaseConf)
////  agg.rowCount(TableName.valueOf("STAT.ALDSTAT_ADVERTISE_OPENID"), new LongColumnInterpreter(), scan);
////  val spark:SparkSession = null
//  hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
//  val rdd= spark.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
//
//}
