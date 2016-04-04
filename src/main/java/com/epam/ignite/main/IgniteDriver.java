package com.epam.ignite.main;


import com.epam.ignite.cache.CacheFiller;
import com.epam.ignite.cache.DataBaseRecord;
import com.epam.ignite.calc.SubtotalCalculator;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.*;

public class IgniteDriver {

    private Ignite ignite = null;

    /** */
    public void start(String localAddress, String localPort) {
        Ignition.setClientMode(true);
        //
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        //
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singletonList(localAddress + ":" + localPort));
        spi.setIpFinder(ipFinder);
        //
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setDiscoverySpi(spi);
        //
        ignite = Ignition.start(cfg);
    }

    /** */
    public void close() {
        ignite.close();
    }

    /** */
    public void clearCache() {
        System.out.println("Trying to clear cache...");
        IgniteCache<String, List<DataBaseRecord>> cache = ignite.getOrCreateCache("dataBaseRecords");
        if (cache != null) {
            cache.clear();
            System.out.println("Cache was cleared.");
        } else {
            System.out.println("Cache was empty.");
        }
    }

    /** */
    public void fillCache() {
        IgniteCompute compute = ignite.compute();
        //
        List<CacheFiller> list = new ArrayList<CacheFiller>();
        for (int i=1;i<=4;i++) {
            list.add(new CacheFiller(i));
        }
        //
        compute.run(list);
    }

    /** */
    public void checkCache(){
        System.out.println("Trying to get cache...");
        IgniteCache<String, List<DataBaseRecord>> cache = ignite.getOrCreateCache("dataBaseRecords");
        if (cache != null) {
            int received = 0;
            System.out.println("Cache has been got.");
            for (int i=1;i<=4;i++) {
                List<DataBaseRecord> o;
                int counter = 0;
                do {
                    System.out.println("Trying to get type=" + i + "...");
                    o = cache.get("type="+i);
                    if (o != null) {
                        System.out.println("type=" + i + " has been got.");
                        break;
                    }
                    try {
                        Thread.sleep(500);
                    } catch (Exception exc) {
                        exc.printStackTrace();
                    }
                    counter++;
                } while (counter < 5);
                if (o != null) {
                    received++;
                    System.out.println("list size for type "+ i + "=" + o.size());
                }
            }
            if (received == 0) {
                System.out.println("Cache was empty");
            }
        } else {
            System.out.println("Can not find cache.");
        }
    }

    /** */
    public double calcFromCache() {
        double total = 0;
        IgniteCompute compute = ignite.compute();
        //
        List<SubtotalCalculator> list = new ArrayList<SubtotalCalculator>();
        for (int i=1;i<=4;i++) {
            list.add(new SubtotalCalculator(i));
        }
        //
        Collection<Double> subtotals = compute.call(list);
        //
        for (Double subtotal : subtotals) {
            total += subtotal;
            System.out.println("Subtotal="+subtotal);
        }
        System.out.println("Total="+total);
        //
        return total;
    }

    /** */
    public static void main(String[] argv) {
        IgniteDriver driver = new IgniteDriver();
        driver.start("127.0.0.1","48501..48504");
        driver.fillCache();
        driver.checkCache();
        driver.calcFromCache();
        driver.close();
    }

}
