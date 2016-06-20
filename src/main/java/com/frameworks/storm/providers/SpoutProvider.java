package com.frameworks.storm.providers;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.TimeZone;

@Slf4j
@Setter
public class SpoutProvider {
  String fileName;
  public SpoutProvider(String fileName){
    this.fileName = fileName;
  }
  public static final Fields FIELDS = new Fields("str","ts");

  public IBatchSpout createSpout() {
    return new Spout(fileName);
  }

  //@RequiredArgsConstructor
  protected static class Spout implements IBatchSpout {

    String fileName;
    public Spout(String fileName){
      this.fileName = fileName;
    }
    private static final long serialVersionUID = -3587144552523719158L;

    //private final long delay;
    private int batchSize=100;
    private int tickerPause=1000; //pause in milliseconds between each tuple
    FileReader fileReader;
    String line;
    TopologyContext context;
    BufferedReader bufferedReader;

    @Override
    public void open(Map conf, TopologyContext context) {

      this.context = context;
      //String fileName = "WASvsORL.csv";
      //String fileName = "OKCvsGSW.csv";
      try {
        this.fileReader = new FileReader(this.fileName);
        log.info("preparing file reader");
        //Thread.sleep(1000);
        return;
      } catch (IOException ex) {ex.printStackTrace();
      }
    }


    public long getTimestamp() {return System.nanoTime();}

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {

      this.bufferedReader = new BufferedReader(fileReader);

      try {
        if ((line = bufferedReader.readLine()) == null) {log.info("done with stream"); Thread.sleep(100000); return;}
        for (int i = 1; (i <= batchSize)&((line = bufferedReader.readLine()) != null); i++) {
          collector.emit(new Values(line,getTimestamp()));
        }
        //log.info("Batchsize"+batchSize);
        //Thread.sleep(tickerPause);
        return;
      }
      catch (IOException ex) {ex.printStackTrace();}
      catch(InterruptedException ex){Thread.currentThread().interrupt();}
    }

    @Override
    public void ack(long batchId) {

    }

    @Override
    public void close() {
      try{bufferedReader.close();}catch(Exception e){
        log.info("BufferReader close error");
      }
    }

    @Override
    public Map getComponentConfiguration() {
      Config conf = new Config();
      conf.setMaxTaskParallelism(1);
      return conf;
    }

    @Override
    public Fields getOutputFields() {
      return FIELDS;
    }
  }
}