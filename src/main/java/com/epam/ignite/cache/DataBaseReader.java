package com.epam.ignite.cache;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DataBaseReader implements Serializable {

    private Connection connection;

    private boolean connect() {
        boolean result = false;
        try {
            String url = "jdbc:postgresql://127.0.0.1:5432/test";
            Properties props = new Properties();
            props.setProperty("user", "postgres");
            props.setProperty("password", "qwerty12");
            //props.setProperty("ssl", "true");
            connection = DriverManager.getConnection(url, props);
            result = !connection.isClosed();
        }catch (Exception exc) {
            exc.printStackTrace();
        }
        return result;
    }

    private void disconnect() {
        try {
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    public List<DataBaseRecord> readRecordsByFilter(int type) {
        List<DataBaseRecord> result = new ArrayList<DataBaseRecord>();
        if (connect()) {
            PreparedStatement pstm = null;
            try {
                pstm = connection.prepareStatement("select balance_id, amount from balances where type = ?");
                pstm.setInt(1,type);
                ResultSet rset = pstm.executeQuery();
                while (rset.next()) {
                    DataBaseRecord record = new DataBaseRecord(rset.getLong(1),rset.getDouble(2));
                    result.add(record);
                }
            } catch (Exception exc) {
                exc.printStackTrace();
            } finally {
                try {
                    if (pstm != null) pstm.close();
                } catch (Exception exc) {
                    exc.printStackTrace();
                }
            }
            disconnect();
        }
        return result;
    }

}
