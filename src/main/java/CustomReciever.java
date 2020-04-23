import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import org.jnetpcap.Pcap;
import org.jnetpcap.PcapIf;
import org.jnetpcap.packet.PcapPacketHandler;
import org.jnetpcap.protocol.network.Ip4;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.Files.readAllLines;

public class CustomReciever extends Receiver<Integer> {

    private int iteration = 0;
    private int max = 10000;
    private int min = 0;
    private String filter_ip = "";
    private List<PcapIf> alldevs = new ArrayList<>();
    private StringBuilder errbuf = new StringBuilder();
    private int deviceId;

    void startMonitoring(String filter) {
        filter_ip = filter;

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        getMinMax();

        CustomReciever bl = new CustomReciever(deviceId);

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("lol");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.minutes(5));

        JavaReceiverInputDStream<Integer> lines = ssc.receiverStream(bl);
        JavaDStream<Integer> sum = lines.reduce((a, b) -> a + b);

        sum.foreachRDD(rdd -> {
            rdd.saveAsTextFile("temp/count");
            openData();
        });

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            System.out.println("INFO: Interrupted exception");
        }

    }

    CustomReciever(int deviceId) {
        super(StorageLevel.MEMORY_ONLY());
        this.deviceId = deviceId;
    }

    CustomReciever() {
        super(StorageLevel.MEMORY_ONLY());
    }

    @Override
    public void onStart() {
        new Thread(this::recieve).start();
    }

    @Override
    public void onStop() {

    }

    private void recieve () {
        int snaplen = 64 * 1024;
        int flags = Pcap.MODE_PROMISCUOUS;
        int timeout = 10 * 1000;
        Pcap.findAllDevs(alldevs, errbuf);

        Pcap pcap = Pcap.openLive(alldevs.get(deviceId).getName(), snaplen, flags, timeout, errbuf);

        PcapPacketHandler<String> jpacketHandler =(packet, user) -> {

            Ip4 ip = new Ip4();

            if (packet.hasHeader(ip)) {
                byte[] sIP = packet.getHeader(ip).source();
                byte[] dIP = packet.getHeader(ip).destination();

                String sourceIP = org.jnetpcap.packet.format.FormatUtils.ip(sIP);
                String destIP = org.jnetpcap.packet.format.FormatUtils.ip(dIP);

                if (!filter_ip.equals("")) {
                    if (sourceIP.equals(filter_ip) || destIP.equals(filter_ip)) {
                        store(packet.getCaptureHeader().caplen());
                    }
                } else {
                    store(packet.getCaptureHeader().caplen());
                }
            }
        };

        System.out.println("INFO: start monitoring");

        while (!isStopped()) {
            pcap.loop(1000, jpacketHandler, "jNetPcap");
        }

        pcap.close();
    }

    private void openData() {
        String result = "";
        int trafic = 0;

        iteration++;
        if (iteration >= 4) {
            getMinMax();
            iteration = 0;
        }

        try {
            result = String.join("\n", readAllLines(Paths
                    .get("temp", "count", "part-00000")));
        } catch (IOException e) {
            System.out.println("INFO: File not found");
        }

        System.out.println("traffic " + result);

        if (!result.equals("")) {
            try {
                trafic = Integer.parseInt(result);
            } catch (NumberFormatException e) {
                System.out.println("INFO: Error, seems like temp/count/part-00000 was changed");
            }

        }

        compare(trafic);
    }

    void getMinMax() {
        System.out.println("INFO: Update min and max value");
        Repository repository = new Repository();

        try {
            min = repository.getMin();
            max = repository.getMax();

            System.out.println("INFO: updating limits");
        } catch (SQLException | ClassNotFoundException e){
            System.out.println("INFO: unable to connect DB.");
            System.out.println("INFO: using default MIN_VAL = 0, MAX_VAL = 10000");
        }

    }

    private void compare(int traffic) {
        if (traffic > max || traffic < min) {
            kafkaSend();
        }
    }

    private void kafkaSend() {
        KafkaProduce kafkaProduce = new KafkaProduce();
        kafkaProduce.send();
        System.out.println("ready");
    }
}
