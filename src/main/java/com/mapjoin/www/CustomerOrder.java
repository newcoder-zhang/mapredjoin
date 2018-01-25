package com.mapjoin.www;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomerOrder implements WritableComparable<CustomerOrder>{
    private int cid;
    private int oid;

    public CustomerOrder( int cid, int oid) {
        this.cid = cid;
        this.oid = oid;
    }

    public void set(int cid,int oid){
        this.cid=cid;
        this.oid=oid;
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public int getOid() {
        return oid;
    }

    public void setOid(int oid) {
        this.oid = oid;
    }

    public CustomerOrder() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CustomerOrder)) return false;

        CustomerOrder that = (CustomerOrder) o;

        if (getCid() != that.getCid()) return false;
        return getOid() == that.getOid();
    }

    @Override
    public int hashCode() {
        int result = getCid();
        result = 31 * result + getOid();
        return result;
    }


    public int compareTo(CustomerOrder o) {
        if(o.cid==cid){
            return Integer.compare(o.oid,oid);
        }
        return Integer.compare(o.cid,cid);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(cid);
        dataOutput.writeInt(oid);
    }

    public void readFields(DataInput dataInput) throws IOException {
        cid=dataInput.readInt();
        oid=dataInput.readInt();
    }

    @Override
    public String toString() {
        return cid+","+oid;
    }
}
