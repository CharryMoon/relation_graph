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
	 * 		Ҫ�зֵ�����
	 * @param delim
	 * 		�ָ���
	 */
	public SimpleStringTokenizer(String str, String delim) {
		st = new StringTokenizer(str, delim, true);
		this.delim = delim;
	}

	/**
	 * 
	 * @param str
	 * 		Ҫ�зֵ�����
	 * @param delim
	 * 		�ָ���
	 * @param length
	 * 		Ĭ�Ϸָ�����ĸ���,���ʵ�ʷָ���С�ڸó���,����Ķ��ᱻ��ɿ�
	 */
	public SimpleStringTokenizer(String str, String delim, int length) {
		st = new StringTokenizer(str, delim, true);
		this.delim = delim;
		this.length = length;
	}
	
	/**
	 * ԭ��Ϊ�յ��ֶζ����Ͽ��ַ�,Ȼ������list���淵��
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
