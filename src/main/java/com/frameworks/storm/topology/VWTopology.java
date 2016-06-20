package com.frameworks.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.frameworks.storm.aggregators.Unique;
import com.frameworks.storm.debug.Debug;
import com.frameworks.storm.operation.KafkaFieldGenerator;
import com.frameworks.storm.operation.VWPredictFieldGenerator;
import com.frameworks.storm.providers.SpoutProvider;
import com.frameworks.storm.state.vw.floatstate.VWTridentFloatQuery;
import com.frameworks.storm.state.vw.floatstate.VWTridentFloatUpdater;
import com.frameworks.storm.state.vw.floatstate.VWTridentFloatStateFactory;
import com.frameworks.storm.state.vw.intstate.VWTridentIntQuery;
import com.frameworks.storm.state.vw.intstate.VWTridentIntStateFactory;
import com.frameworks.storm.state.vw.intstate.VWTridentIntUpdater;
import kafka.Kafka;
import lombok.extern.slf4j.Slf4j;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.TridentKafkaStateFactory;
import storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.selector.DefaultTopicSelector;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.util.Properties;

@Slf4j
public class VWTopology {

  /*Settings Variables*/
  //String init = "-l 10 -c --passes 2 --holdout_off --loss_function squared";
  String init = "--oaa 3 --loss_function logistic";


  /*Helper Functions*/
  private OpaqueTridentKafkaSpout createKafkaSpout() {

    BrokerHosts zk = new ZkHosts("hw0002.dev1.awse1a.datasciences.tmcs");
    TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "sts.debug.topic");
    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
    return(spout);
  }

  private void getTrainingTopologyInt()throws Exception{
    TridentTopology topology = new TridentTopology();
    SpoutProvider sp = new SpoutProvider("training_set_int.txt");
    SpoutProvider sp2 = new SpoutProvider("test_set_int.txt");
    Stream trainingStream = topology.newStream("spout1", sp.createSpout());

    TridentState vwIntState = trainingStream
            .each(new Fields("str"),new Debug("Init"),new Fields())
            .partitionPersist(new VWTridentIntStateFactory(init),new Fields("str","ts"),new VWTridentIntUpdater("str","ts"));

    VWPredictFieldGenerator vwp = new VWPredictFieldGenerator("test_set_int.txt");

    trainingStream
            .each(new Fields("str"),new Debug("training"),new Fields())
            .partitionPersist(new VWTridentIntStateFactory(init),new Fields("str","ts"),new VWTridentIntUpdater("str","ts")).newValuesStream()
            .each(new Fields(), new KafkaFieldGenerator(),new Fields("field"))
            .aggregate(new Fields("field"), new Unique(),new Fields())
            .each(new Fields(),vwp,new Fields("predStr"))
            .stateQuery(vwIntState,new Fields("predStr"),new VWTridentIntQuery("predStr"),new Fields("prediction"))
            .each(new Fields("prediction"),new Debug("prediction"),new Fields());

    StateFactory stateFactory = new TridentKafkaStateFactory()
            .withKafkaTopicSelector(new DefaultTopicSelector("sts.debug.topic"))
            .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>("key", "string"));

    Config conf = new Config();

    LocalCluster cluster = new LocalCluster();

    Properties props = new Properties();
    props.put("metadata.broker.list", "hw0002.dev1.awse1a.datasciences.tmcs:6667");
    props.put("request.required.acks", "1");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("key.serializer.class","kafka.serializer.StringEncoder");
    conf.put("kafka.broker.properties", props);

    //Local Mode
    cluster.submitTopology("kafkaTridentTest", conf, topology.build());

    //Submit to Cluster Mode
    //StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());

  }

  public static void main(String args[]){

    try{new VWTopology().getTrainingTopologyInt();}
    catch(Exception e){
      e.printStackTrace();

    }
  }
}
