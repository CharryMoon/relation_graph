import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class SimpleStringTokenizer {
	StringTokenizer st = null;
	String delim;
	int length = -1;
	
	/**
	 * 
	 * @param str
	 * 		要切分的内容
	 * @param delim
	 * 		分隔符
	 */
	public SimpleStringTokenizer(String str, String delim) {
		st = new StringTokenizer(str, delim, true);
		this.delim = delim;
	}

	/**
	 * 
	 * @param str
	 * 		要切分的内容
	 * @param delim
	 * 		分隔符
	 * @param length
	 * 		默认分隔结果的个数,如果实际分割结果小于该长度,后面的都会被填成空
	 */
	public SimpleStringTokenizer(String str, String delim, int length) {
		st = new StringTokenizer(str, delim, true);
		this.delim = delim;
		this.length = length;
	}
	
	/**
	 * 原来为空的字段都填上空字符,然后存放在list里面返回
	 * @return
	 */
	public List<String> getAllElements(){
		List<String> values = new ArrayList<String>();
		boolean dataflag = false;
		while(st.hasMoreTokens()){
			String word = st.nextToken();
			if(!delim.equals(word)){
				values.add(word);
				dataflag = true;
			}
			else{
				if(dataflag)
					dataflag=false;
				else{
					values.add("");
				}
			}
		}
		
		if(length >= 1 && values.size() < length){
			for(int i=0; i<length-values.size(); i++)
				values.add("");
		}
		
		return values;
	}
}
