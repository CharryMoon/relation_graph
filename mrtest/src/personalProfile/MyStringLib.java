package personalProfile;
import java.util.Iterator;
import java.util.Set;


public class MyStringLib {
	private static final int DEFAULT_ZERO_SIZE = 20;
	private static String[] zero;
	
	public static String combineForKey(int unitLength, String... string) {
		// TODO Auto-generated method stub
		String resultStr = "";
		
		for (String str : string) {
			resultStr += getZeroHead(unitLength - str.length()) + str;
		}	
		return resultStr;
	}
	
	public static String getZeroHead(int zeroNumber) {
		if (zero == null) {
			initZero();
		}	
		return zero[zeroNumber];
	}
	
	public static String removeZeroHead(String string) {
		// TODO Auto-generated method stub
		int beginIndex = 0;
		int length = string.length();
		
		for (beginIndex = 0; beginIndex < length; beginIndex++) {
			if (string.charAt(beginIndex) != '0') {
				break;
			}
		}					
		return string.substring(beginIndex);
	}
	
	public static String combine(String separator, String... string) {
		String combineResult = "";
		int length = string.length - 1;
		
		for (int i = 0; i < length; i++) {
			combineResult += string[i] + separator;
		}
		combineResult += string[length];	
		return combineResult;
	}
	
	public static String merge(Set<String> sets, String separator) {
		// TODO Auto-generated method stub
		String value1 = "";
		Iterator<String> iterator = sets.iterator();
		
		while (iterator.hasNext()) {
			value1 += iterator.next();
			
			if (iterator.hasNext()) {
				value1 += separator;
			}
		}		
		return value1;
	}
	
	private static void initZero() {
		String tmpStr = "";
		
		zero = new String[DEFAULT_ZERO_SIZE];
		for (int i = 0; i < DEFAULT_ZERO_SIZE; i++) {
			zero[i] = tmpStr;
			tmpStr += "0";
		}
	}
}
