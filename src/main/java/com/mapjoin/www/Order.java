package com.mapjoin.www;

public class Order {
    private int oid;
    private int cid;
    private double price;
    private String date;

    public Order() {
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public Order(int oid, int cid, double price, String date) {
        this.oid = oid;
        this.cid=cid;
        this.price = price;
        this.date = date;
    }

    public int getOid() {
        return oid;
    }

    public void setOid(int oid) {
        this.oid = oid;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
