package cn.northpark.flink.MR.bean;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author bruce
 * @date 2022年04月19日 21:29:12
 */
public class CarBean implements Writable {

    private String date;
    private String upSpeed;
    private String speed;
    private String no;


    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getUpSpeed() {
        return upSpeed;
    }

    public void setUpSpeed(String upSpeed) {
        this.upSpeed = upSpeed;
    }

    public String getSpeed() {
        return speed;
    }

    public void setSpeed(String speed) {
        this.speed = speed;
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    @Override
    public String toString() {
        return "{" +
                "  date='" + date + '\'' +
                ", upSpeed='" + upSpeed + '\'' +
                ", speed='" + speed + '\'' +
                ", no='" + no + '\'' +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(date    );
        dataOutput.writeUTF(upSpeed );
        dataOutput.writeUTF(speed   );
        dataOutput.writeUTF(no      );
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.date         =dataInput.readUTF();
        this.upSpeed      =dataInput.readUTF();
        this.speed        =dataInput.readUTF();
        this.no           =dataInput.readUTF();
    }
}
