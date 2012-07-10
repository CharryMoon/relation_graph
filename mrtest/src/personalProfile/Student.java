package personalProfile;

public class Student {
//	private List<Student>
	private String career = "";
	private String schoolDepartment = "";
	private String schoolClass = "";
	private String enterDay = "";
	private String userId = "";
	private int level = 0;
	
	public String getCareer() {
		return career;
	}
	public void setCareer(String career) {
		this.career = career;
	}
	public String getSchoolDepartment() {
		return schoolDepartment;
	}
	public void setSchoolDepartment(String schoolDepartment) {
		this.schoolDepartment = schoolDepartment;
	}
	public String getSchoolClass() {
		return schoolClass;
	}
	public void setSchoolClass(String schoolClass) {
		this.schoolClass = schoolClass;
	}
	public String getEnterDay() {
		return enterDay;
	}
	public void setEnterDay(String enterDay) {
		this.enterDay = enterDay;
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
		return "Student [career=" + career + ", schoolDepartment="
				+ schoolDepartment + ", schoolClass=" + schoolClass
				+ ", enterDay=" + enterDay + ", userId=" + userId + ", level="
				+ level + "]";
	}
	
	
}
