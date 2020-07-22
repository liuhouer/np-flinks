package cn.northpark.flink.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ObjectUtil {
	
	
	/**
	 * 对象转Byte数组
	 *
	 * @param obj
	 * @return
	 * @throws Exception
	 */
	public static byte[] objectToBytes(Object obj) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream sOut = new ObjectOutputStream(out);
		sOut.writeObject(obj);
		sOut.flush();
		byte[] bytes = out.toByteArray();


		return bytes;
	} 

	/**
	 * 字节数组转对象
	 *
	 * @param bytes
	 * @return
	 * @throws Exception
	 */
	public static Object bytesToObject(byte[] bytes) throws Exception {

		//byte转object
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		ObjectInputStream sIn = new ObjectInputStream(in);
		return sIn.readObject();

	}
}

