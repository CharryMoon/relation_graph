import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FollowAvailabilityJoinKey implements
		WritableComparable<FollowAvailabilityJoinKey> {

	private Text userId = new Text();
	private Text subscribeId = new Text();

	public FollowAvailabilityJoinKey(){}
	
	public FollowAvailabilityJoinKey(String userId, String subscribeId) {
		this.userId = new Text(userId);
		this.subscribeId = new Text(subscribeId);
	}

	public Text getUserId() {
		return userId;
	}

	public void setUserId(Text userId) {
		this.userId = userId;
	}

	public Text getSubscribeId() {
		return subscribeId;
	}

	public void setSubscribeId(Text subscribeId) {
		this.subscribeId = subscribeId;
	}

	public void write(DataOutput out) throws IOException {
		userId.write(out);
		subscribeId.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		userId.readFields(in);
		subscribeId.readFields(in);
	}

	@Override
	public int hashCode() {
		return userId.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof FollowAvailabilityJoinKey) || obj == null) {
			return false;
		}

		FollowAvailabilityJoinKey key = (FollowAvailabilityJoinKey) obj;
		return compareTo(key)==0;
	}

	public int compareTo(FollowAvailabilityJoinKey obj) {
		if (obj == null) {
			return 1;
		}
		if (this.userId.equals(obj.userId)) {
			return this.subscribeId.compareTo(obj.subscribeId);
//			if (this.subscribeId.equals(obj.subscribeId)) {
//				return this.source.compareTo(obj.source);
//			} else {
//				return this.subscribeId.compareTo(obj.subscribeId);
//			}
		} else {
			return this.userId.compareTo(obj.userId);
		}
	}

}
