package personalProfile;

import java.util.Comparator;


public class ComparatorStudent implements Comparator<Student>{

	@Override
	public int compare(Student o1, Student o2) {
		// TODO Auto-generated method stub
		
		return o2.getLevel() - o1.getLevel();
	}
	
}
