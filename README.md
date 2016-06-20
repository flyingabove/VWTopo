basic framework for reading and writing from kafka and hbase

##KAFKA##
    To run:

    mvn clean compile exec:java -Dexec.mainClass="com.frameworks.storm.topology.KafkaConsumeTopology" -Dexec.args="storm-one-framework-1.0.0-SNAPSHOT-storm.jar"
    need to change last line of topology code to: cluster.submitTopology(...);

    or alternatively:

    storm jar target/storm-one-framework-1.0.0-SNAPSHOT-storm.jar com.frameworks.storm.topology.KafkaConsumeTopology
    need to change last line of topology code to: StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());

##HBASE##

    mvn clean compile exec:java -Dexec.mainClass="com.frameworks.storm.topology.HBaseWriterTopology" -Dexec.args="storm-one-framework-1.0.0-SNAPSHOT-storm.jar"


