/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.qcj;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HBaseSinkConfigurationConstants;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * 自定义flume和HbaseSink
 * 需要解决的问题：
 * 1.读到我们配置的列名有哪些？
 * 2.得要知如何去拆分我们的数据
 *
 * 打jar包
mvn clean package -DskipTests
 */
public class MySimpleHbaseEventSerializer implements HbaseEventSerializer {
  private String rowPrefix;
  private byte[] incrementRow;
  private byte[] cf;
  private String plCol;
  private byte[] incCol;
  private byte[] payload;
  private String separator;//配置文件内容为 六个字段名称
  private byte[] columnFamily;//列簇

  public MySimpleHbaseEventSerializer() {
  }

    /**
     * 这个方法主要是来获得配置文件中的配置项的
     * @param context
     */
  @Override
  public void configure(Context context) {
    rowPrefix = context.getString("rowPrefix", "default");
    incrementRow =
        context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);
    String suffix = context.getString("suffix", "uuid");
    //列名在pcol中
    String payloadColumn = context.getString("payloadColumn", "pCol");
    String incColumn = context.getString("incrementColumn", "iCol");
    String separator = context.getString("separator",",");//分割符
    //得到六个字段
    //获取列簇
    String cf = context.getString("columnFamily","cf1");
    if (payloadColumn != null && !payloadColumn.isEmpty()) {
     plCol = payloadColumn;
     System.out.println("=====================>payloadColumn" + plCol);
    }
    if(separator != null && !separator.isEmpty()){
        this.separator = separator;
        System.out.println("=====================>xuexixiaodian下cdsn" + plCol);
    }
  if(cf != null && !cf.isEmpty()){
      this.columnFamily = cf.getBytes(Charsets.UTF_8);
      System.out.println("=====================>columnFamily" + cf);
  }
    if (incColumn != null && !incColumn.isEmpty()) {
      incCol = incColumn.getBytes(Charsets.UTF_8);
    }

  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }

  @Override
  public void initialize(Event event, byte[] cf) {
    this.payload = event.getBody();//读取得到数据
    this.cf = cf;
  }

  @Override
  public List<Row> getActions() throws FlumeException {
    List<Row> actions = new LinkedList<Row>();
    if (plCol != null) {
      byte[] rowKey = null;
      try {
        //得到数据
        //数据在pyload(字节数据)中，列信息在payLoadColumn中，这两者需要一一对应
          String[] columns = this.plCol.split(",");
          System.out.println("------------>Columns:" + Arrays.toString(columns));
          //切割的数据
          String[] datasArray = new String(this.payload).split(this.separator);
          System.out.println("------------>Columns:" + Arrays.toString(datasArray));

          if(columns == null || datasArray == null || columns.length != datasArray.length){
              return actions;
          }else{
              String userid = datasArray[1];
              String time = datasArray[0];
              Put put = null;
              //添加 每一列对应每一列
              for (int i = 0; i < columns.length; i++) {
                  String column = columns[i];
                  String data = datasArray[i];
                  //put(rowKey)
                  put =  new Put(SimpleRowKeyGenerator.getUserIdAndTimeKey(time,userid));
//                  put.addColumn("cf".getBytes(),column.getBytes(),data.getBytes());
                  put.addColumn(columnFamily,column.getBytes(),data.getBytes());
                  actions.add(put);//action存放数据
              }
          }
      } catch (Exception e) {
        throw new FlumeException("Could not get row key!", e);
      }

    }
    return actions;
  }

  @Override
  public List<Increment> getIncrements() {
    List<Increment> incs = new LinkedList<Increment>();
    if (incCol != null) {
      Increment inc = new Increment(incrementRow);
      inc.addColumn(cf, incCol, 1);
      incs.add(inc);
    }
    return incs;
  }

  @Override
  public void close() {
  }

  public enum KeyType {
    UUID,
    RANDOM,
    TS,
    TSNANO;
  }

}
