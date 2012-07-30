package samecompany;

public class Employee {

	private String userId;
	private String companyName;
	private String businessType;
	private String businessDetail;
	private String positionType;
	private String position;
	private String startDay;
	private String endDay;

	/***
	 *  type = 1 	������Ϣ
	 *  type = 2	����д����ʱ��
	 *  type = 4	��ְλ��Ϣ
	 *  type = 8	����ҵ��Ϣ,����һ��һ����˾,Ӧ�ö���ͬ��ҵ(�Ҳ����������ҵ��ͬһ����˾������в�ͬ��?)
	 *  type = 15	����2,4,8
	 */
	public static final int BASIC = 1;
	public static final int WITHDAY = 2;	// �����Ŀ�ʼ�ͽ���,ֻ������û��
	public static final int WITHPOSITION = 4;
	public static final int WITHBUSINESS = 8;
	public static final int FULL = 15;
	// �����matchָ����ѧУid����,���������Ƿ���ƥ���
	public static final int MATCHED = 16;
	/**
	 * �����е����ͣ� MATCHED ֻ��¼�����type����
	 */
	private int type;
	
//	public static final int BASIC_MATCHED = 1;
	public static final int DAY_MATCHED = 2;
	public static final int POSITIONTYPE_MATCHED = 4;
	public static final int POSITION_MATCHED = 8;
	public static final int BUSINESSTYPE_MATCHED = 16;
	public static final int BUSINESSDETAIL_MATCHED = 32;
	/**
	 * ��Ŀ��ƥ���ϵ�����
	 */
	private int matchedType;
	
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getCompanyName() {
		return companyName;
	}
	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}
	public String getBusinessType() {
		return businessType;
	}
	public void setBusinessType(String businessType) {
		this.businessType = businessType;
	}
	public String getBusinessDetail() {
		return businessDetail;
	}
	public void setBusinessDetail(String businessDetail) {
		this.businessDetail = businessDetail;
	}
	public String getPositionType() {
		return positionType;
	}
	public void setPositionType(String positionType) {
		this.positionType = positionType;
	}
	public String getPosition() {
		return position;
	}
	public void setPosition(String position) {
		this.position = position;
	}
	public String getStartDay() {
		return startDay;
	}
	public void setStartDay(String startDay) {
		this.startDay = startDay;
	}
	public String getEndDay() {
		return endDay;
	}
	public void setEndDay(String endDay) {
		this.endDay = endDay;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public void setMatchedType(int matchedType) {
		this.matchedType = matchedType;
	}
	public int getMatchedType() {
		return matchedType;
	}
	
	/**
	 * ����type��matchtype�ĺϲ�����
	 * @return
	 */
	public String getCombineType(){
		return type+"_"+matchedType;
	}
	
	public static int[] getSplitType(String typeStr){
		int[] types = new int[2];
		String[] values = typeStr.split("_");
		// values max length is 2 (type matchtype)
		for (int i = 0; i < 2; i++) {
			types[i] = Integer.parseInt(values[i]);
		}
		return types;
	}
}
