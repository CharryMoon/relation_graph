package sameschool;

public class Student {

	private String userId;
	private int schoolId;
	private String enterDay;
	private String career;
	private String extra;
	
	/***
	 *  type = 1 	������Ϣ
	 *  type = 2	����ѧʱ��
	 *  type = 4	�а༶����Ժϵ��Ϣ
	 *  type = 7	����2,3
	 */
	public static final int BASIC = 1;
	public static final int WITHENTERDAY = 2;
	public static final int WITHCLASS = 4;
	public static final int FULL = 7;
	public static final int MATCHED = 8;
	private int type;
	
	public int getSchoolId() {
		return schoolId;
	}
	public void setSchoolId(int schoolId) {
		this.schoolId = schoolId;
	}
	public String getEnterDay() {
		return enterDay;
	}
	public void setEnterDay(String enterDay) {
		this.enterDay = enterDay;
	}
	public String getCareer() {
		return career;
	}
	public void setCareer(String career) {
		this.career = career;
	}
	public String getExtra() {
		return extra;
	}
	public void setExtra(String extra) {
		this.extra = extra;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	
}
