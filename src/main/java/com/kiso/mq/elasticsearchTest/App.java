package com.kiso.mq.elasticsearchTest;

import java.net.InetAddress;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Hello world!
 *
 */
public class App 
{	
	 public static void main(String[] args) {

         try {

             //设置集群名称
             Settings settings = Settings.builder()
            		 .put("client.transport.sniff", true)//这个客户端可以嗅到集群的其它部分，并将它们加入到机器列表。为了开启该功能，设置client.transport.sniff为true。
                     .put("cluster.name", "elk_fangjian")
                     .build();
             //创建client
             TransportClient client = new PreBuiltTransportClient(settings)
                     .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("14.18.234.35"), 9300));
             //搜索数据
             GetResponse response = client.prepareGet("fangjian", "songs", "2").execute().actionGet();
             //输出结果
             System.out.println(response.getSourceAsString());
             //关闭client
             client.close();

         } catch (Exception e) {
             e.printStackTrace();
         }

     }
}
