package cn.northpark.flink.vehicleAPP.mock;

import cn.northpark.flink.util.KafkaString;

import java.util.Properties;

/**
 * @author bruce
 * @date 2022年07月01日 13:32:47
 *
 * {
 *  "id": 0,
 *  "vehicleId": 5288,
 *  "status": "00000000000000000000000000000001",
 *  "depId": 3001223,
 *  "signalState": 0,
 *  "onlineDate": 1656380610414,
 *  "responseSn": 0,
 *  "alarmState": "00000000000000000000000000000000",
 *  "parkingTime": 0,
 *  "tiredAlarmTime": 0,
 *  "plateNo": "沪C34090",
 *  "areaId": 0,
 *  "areaAlarm": 0,
 *  "areaType": 0,
 *  "overSpeedAreaId": 0,
 *  "overSpeedAreaType": 0,
 *  "routeSegmentId": 0,
 *  "runTimeOnRoute": 0,
 *  "routeAlarmType": 0,
 *  "simNo": "12917012599",
 *  "location": "",
 *  "sendTime": 1656380610414,
 *  "updateDate": 1656380610414,
 *  "longitude": 116.403961,
 *  "latitude": 39.915119,
 *  "velocity": 0.0,
 *  "direction": 0,
 *  "altitude": 0.0,
 *  "recordVelocity": 0.0,
 *  "gas": 0.0,
 *  "mileage": 7293.0,
 *  "valid": false,
 *  "dvrStatus": "",
 *  "online": true
 * }
 */
public class MockSourceMsg {
    public static void main(String[] args) {
        //String msg = "{\"id\":0,\"vehicleId\":5288,\"status\":\"00000000000000000000000000000001\",\"depId\":3001223,\"signalState\":0,\"onlineDate\":1656380610414,\"responseSn\":0,\"alarmState\":\"00000000000000000000000000000000\",\"parkingTime\":0,\"tiredAlarmTime\":0,\"plateNo\":\"沪C34090\",\"areaId\":0,\"areaAlarm\":0,\"areaType\":0,\"overSpeedAreaId\":0,\"overSpeedAreaType\":0,\"routeSegmentId\":0,\"runTimeOnRoute\":0,\"routeAlarmType\":0,\"simNo\":\"12917012599\",\"location\":\"\",\"sendTime\":1656380610414,\"updateDate\":1656380610414,\"longitude\":116.403961,\"latitude\":39.915119,\"velocity\":0.0,\"direction\":0,\"altitude\":0.0,\"recordVelocity\":0.0,\"gas\":0.0,\"mileage\":7293.0,\"valid\":false,\"dvrStatus\":\"\",\"online\":true}";

        //构造一条online = false的跑【VehicleOnlineJudgeApp】和【VehicleOnlineHandleApp】作业
        String msg2 = "{\"id\":0,\"vehicleId\":5288,\"status\":\"00000000000000000000000000000001\",\"depId\":3001223,\"signalState\":0,\"onlineDate\":1656380610414,\"responseSn\":0,\"alarmState\":\"00000000000000000000000000000000\",\"parkingTime\":0,\"tiredAlarmTime\":0,\"plateNo\":\"沪C34090\",\"areaId\":0,\"areaAlarm\":0,\"areaType\":0,\"overSpeedAreaId\":0,\"overSpeedAreaType\":0,\"routeSegmentId\":0,\"runTimeOnRoute\":0,\"routeAlarmType\":0,\"simNo\":\"12917012599\",\"location\":\"\",\"sendTime\":1656380610414,\"updateDate\":1656380610414,\"longitude\":116.403961,\"latitude\":39.915119,\"velocity\":0.0,\"direction\":0,\"altitude\":0.0,\"recordVelocity\":0.0,\"gas\":0.0,\"mileage\":7293.0,\"valid\":false,\"dvrStatus\":\"\",\"online\":false}";
        Properties properties = KafkaString.buildBasicKafkaProperty();
        KafkaString.sendKafkaString(properties,"flink_rdf",msg2);
    }
}
