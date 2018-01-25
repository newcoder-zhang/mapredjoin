package com.mapjoin.www;

public class Customer {
    private int cid;
    private String name;
    private String tel;

    public Customer() {
    }



    public Customer(int cid, String name, String tel) {
        this.cid = cid;
        this.name = name;
        this.tel = tel;
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }
}
