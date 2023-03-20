package cn.northpark.hadoop.MR.covid;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author bruce
 * @date 2023年03月20日 13:58:33
 */
public class Covid  implements Writable {

    public Covid() {
        // 空构造函数
    }
    private int newCases;
    private int newDeaths;

    public Covid(int newCases, int newDeaths) {
        this.newCases = newCases;
        this.newDeaths = newDeaths;
    }

    public int getNewCases() {
        return newCases;
    }

    public int getNewDeaths() {
        return newDeaths;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(newCases);
        dataOutput.writeInt(newDeaths );
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.newCases  =dataInput.readInt();
        this.newDeaths =dataInput.readInt();
    }
}
