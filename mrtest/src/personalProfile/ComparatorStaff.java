package personalProfile;
import java.util.Comparator;


public class ComparatorStaff implements Comparator<Staff> {

	@Override
	public int compare(Staff o1, Staff o2) {
		// TODO Auto-generated method stub
		return o2.getLevel() - o1.getLevel();
	}

}
