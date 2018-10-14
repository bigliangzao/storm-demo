package com.winxuan;


import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class ClustorSumStormTopology {
	
	public static class SumSpout extends BaseRichSpout{

		private static final long serialVersionUID = 1L;
		
		private SpoutOutputCollector collector;
		
		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector=collector;
		}
		
		int number=0;
		
		@Override
		public void nextTuple() {
			number++;
			collector.emit(new Values(number),number);
			System.out.println("number:"+number);
			Utils.sleep(2000);
		}
		
		@Override
		public void ack(Object msgId) {
			System.out.println("ack invoked:"+msgId);
		}
		
		@Override
		public void fail(Object msgId) {
			System.out.println("fail invoked:"+msgId);
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("num"));
		}
		
	}
	
	public static class SumBolt extends BaseRichBolt{

		private static final long serialVersionUID = 1L;
		
		private OutputCollector collector;
		
		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}
		Integer sum=0;
		@Override
		public void execute(Tuple input) {
			
			Integer value = input.getIntegerByField("num");
			sum += value;
			if (value<10) {
				collector.ack(input);
			}else {
				collector.fail(input);
			}
			System.out.println("当前线程为:"+Thread.currentThread().getId()+"接收到的数据为:"+value);
			System.out.println("sum:"+sum);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}
		
	}
	
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("SumSpout", new SumSpout());
		builder.setBolt("SumBolt", new SumBolt(),1).shuffleGrouping("SumSpout");
		try {
			StormSubmitter.submitTopology("ClustorSumStormTopology", new Config(), builder.createTopology());
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			e.printStackTrace();
		}
//		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("addUser");
//		builder.addBolt(new SumBolt());
//		LocalCluster localCluster = new LocalCluster();
//		LocalDRPC drpc = new LocalDRPC();
//		localCluster.submitTopology("local-drpc", new Config(), builder.createLocalTopology(drpc));
//		
//		drpc.execute(arg0, arg1);
		
		
		
		
		
		
	}
}
