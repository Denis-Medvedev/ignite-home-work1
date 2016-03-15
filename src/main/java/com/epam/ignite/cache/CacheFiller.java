package com.epam.ignite.cache;

import com.epam.ignite.cache.DataBaseReader;
import com.epam.ignite.cache.DataBaseRecord;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteRunnable;

import java.util.List;

public class CacheFiller implements IgniteRunnable {

    private DataBaseReader reader;
    private int type;

    public CacheFiller(int type) {
        this.type = type;
        this.reader = new DataBaseReader();
    }

    public void run() {
        Ignite ignite = Ignition.ignite();
        System.out.println("Starting to cache type="+type+"...");
        IgniteCache<String, List<DataBaseRecord>> cache = ignite.getOrCreateCache("dataBaseRecords");
        List<DataBaseRecord> records = reader.readRecordsByFilter(type);
        cache.put("type="+type, records);
        System.out.println("type ="+type+" has been cached.");
    }
}
