package personalProfile;

public class Staff {
	private String business1 = "";
	private String business2 = "";
	private String postion1 = "";
	private String postion2 = "";
	private String userId = "";
	private int level;
	
	public String getBusiness1() {
		return business1;
	}
	public void setBusiness1(String business1) {
		this.business1 = business1;
	}
	public String getBusiness2() {
		return business2;
	}
	public void setBusiness2(String business2) {
		this.business2 = business2;
	}
	public String getPostion1() {
		return postion1;
	}
	public void setPostion1(String postion1) {
		this.postion1 = postion1;
	}
	public String getPostion2() {
		return postion2;
	}
	public void setPostion2(String postion2) {
		this.postion2 = postion2;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;		
	}
	@Override
	public String toString() {
		return "Staff [business1=" + business1 + ", business2=" + business2
				+ ", postion1=" + postion1 + ", postion2=" + postion2
				+ ", userId=" + userId + "]";
	}
	
}
