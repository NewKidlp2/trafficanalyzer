import org.jnetpcap.Pcap;
import org.jnetpcap.PcapIf;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class CustomRecInterface {
    public static void main(String[] args) {

        String filter_ip = "";
        if (args.length != 0) {
            filter_ip = args[0];
        }

        int deviceId = chooseDevice();
        CustomRecInterface.startThread();
        CustomReciever customReciever = new CustomReciever(deviceId);
        customReciever.startMonitoring(filter_ip);
    }

    private static void startThread() {
        CustomReciever customReciever = new CustomReciever();
        Thread thread = new Thread(() -> {
            System.out.println("Now you can enter commands:");
            System.out.println("end: to end work");
            System.out.println("update: to update limits from DB");
            String command;
            Scanner scanner = new Scanner(System.in);
            while (true) {
                    command = scanner.next();
                switch (command) {
                    case "update":
                        customReciever.getMinMax();
                        break;
                    case "end": System.exit(0);
                        break;
                    default:
                        System.out.println("Unknown command");
                }
            }
        });

        thread.start();
    }

    private static int chooseDevice() {
        List<PcapIf> alldevs = new ArrayList<>();
        StringBuilder errbuf = new StringBuilder();
        int deviceId = 0;
        Pcap.findAllDevs(alldevs, errbuf);

        if (alldevs.size() == 0) {
            System.out.println("Network devices not found.");
            System.exit(0);
        }

        System.out.println("Choose network device (print number and press enter)");
        for (int i = 0; i < alldevs.size(); i++) {
            System.out.println(i + 1 + ". " + alldevs.get(i).getDescription());
        }
        Scanner input = new Scanner(System.in);

        try {
            deviceId = Integer.parseInt(input.nextLine()) - 1;
            if ((deviceId > alldevs.size() - 1) || (deviceId < 0)) {
                System.out.println("Device doesn't exist. Using default device: 1");
                deviceId = 1;
            }

        } catch (NumberFormatException e) {
            System.out.println("device not found. default device: 1");
        }
        return deviceId;
    }
}