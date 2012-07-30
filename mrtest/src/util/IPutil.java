package util;

public class IPutil {

	/**
     * 把IP地址转化为int
     * @param ipAddr
     * @return int
     */
    public static int ipToInt(String ipAddr) {
        int ret = 0;
        try {
            String[] ipArr = ipAddr.split("\\.");
            ret = (Integer.parseInt(ipArr[0]) & 0xFF)<< 24 | ret;
            ret = (Integer.parseInt(ipArr[1]) & 0xFF)<< 16 | ret;
            ret = (Integer.parseInt(ipArr[2]) & 0xFF)<< 8 | ret;
            ret = (Integer.parseInt(ipArr[3]) & 0xFF) | ret;
            return ret;
        } catch (Exception e) {
//            throw new IllegalArgumentException(ipAddr + " is invalid IP");
        }
        return 0;
    }

    public static void main(String[] args) {
		System.out.println(ipToInt("10.25.192.1"));
	}
}
