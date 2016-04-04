package com.epam.ignite.calc;

import com.epam.ignite.cache.DataBaseRecord;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteCallable;

import java.util.List;

public class SubtotalCalculator implements IgniteCallable<Double> {

    private int type;

    /** */
    public SubtotalCalculator(int type){
        this.type = type;
    }

    /** */
    public Double call() throws Exception {
        double result = 0;
        Ignite ignite = Ignition.ignite();
        IgniteCache<String, List<DataBaseRecord>> cache = ignite.getOrCreateCache("dataBaseRecords");
        List<DataBaseRecord> list = cache.get("type="+type);
        if (list!=null) {
            System.out.println("Stating to compute subtotal...");
            for (DataBaseRecord record : list) {
                result += record.getAmount();
            }
            System.out.println("Subtotal computed:" + result);
        } else {
            System.out.println("Cache was empty.");
        }
        return result;
    }
}
