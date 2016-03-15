package com.epam.ignite.cache;

import java.io.Serializable;
import java.util.Date;

public class DataBaseRecord implements Serializable {

    private Long balanceId;
    private Double amount;
    private Date lastChangeDate;

    public DataBaseRecord(Long balanceId, Double amount) {
        this.balanceId = balanceId;
        this.amount = amount;
    }

    public Long getBalanceId() {
        return balanceId;
    }

    public void setBalanceId(Long balanceId) {
        this.balanceId = balanceId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Date getLastChangeDate() {
        return lastChangeDate;
    }

    public void setLastChangeDate(Date lastChangeDate) {
        this.lastChangeDate = lastChangeDate;
    }
}
