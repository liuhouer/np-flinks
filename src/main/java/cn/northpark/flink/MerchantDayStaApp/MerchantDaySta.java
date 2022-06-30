package cn.northpark.flink.MerchantDayStaApp;

import lombok.Data;


/**
 * @author bruce
 * @date 2022年05月09日 18:06:54
 */

@Data
public class MerchantDaySta {

    private String merchantId;

    private Double totalDeductMoney;

    public MerchantDaySta() {
    }

    public MerchantDaySta(String merchantId, Double totalDeductMoney) {
        this.merchantId = merchantId;
        this.totalDeductMoney = totalDeductMoney;
    }

    @Override
    public String toString() {
        return "MerchantDaySta{" +
                "merchantId='" + merchantId + '\'' +
                ", totalDeductMoney=" + totalDeductMoney +
                '}';
    }
}
