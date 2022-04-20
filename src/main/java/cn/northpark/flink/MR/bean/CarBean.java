package cn.northpark.flink.MR.bean;


import org.apache.commons.lang3.StringUtils;
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
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(no)) {
            sb.append(no).append("    ");
        }

        if (StringUtils.isNotEmpty(date)) {
            sb.append(date).append("    ");
        }
        if (StringUtils.isNotEmpty(upSpeed)) {
            sb.append(upSpeed).append("    ");
        }
        if (StringUtils.isNotEmpty(speed)) {
            sb.append(speed).append("    ");
        }

        return sb.toString();
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

    public static void main(String[] args) {
        CarBean bean = new CarBean();
        bean.setDate("2022-4-20");
        bean.setSpeed("222");
        bean.setUpSpeed("0.618");
        bean.setNo("6188");
        System.err.println(bean.toString());
    }
}
