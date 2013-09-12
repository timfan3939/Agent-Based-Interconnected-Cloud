package tw.idv.ctfan.cloud.middleware.policy.data;

import java.util.HashMap;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;

/**
 * 
 * @author C.T.Fan
 * 
 * New Job Type, describing what a job like.  Except UID, all other attributes
 * are managed by java.util.HashMap.
 * 
 * Attributes are divided into two types: Continuous or Discrete.
 * Continuous value will be viewed as a Positive Long value.
 * Discrete value will takes more times to deal with, as our prediction tool
 * is Rough Set.
 *
 */

public class JobNode implements Comparable<JobNode> {
	
	
	static public enum AttributeType {
		Continuous, Discrete
	}
	static public enum JobStatus {
		Waiting, Running, Finished;
	}
	
	// Attributes that not in HashMap
	public long UID;
	public JobStatus status;
	public long executionTime;
	public long deadline;
	public JobType jobType;

	private HashMap<String,String> attributes;
	static public HashMap<String, AttributeType> attributeType = new HashMap<String, AttributeType>();
	
	public JobNode(){
		this.UID = System.nanoTime();
		this.executionTime = -1;
		this.deadline = -1;
		attributes = new HashMap<String, String>();
	}
	
	public boolean AddDiscreteAttribute(String key, String value){
		if(!attributeType.containsKey(key)) {
			attributeType.put(key, AttributeType.Discrete);
		} else if(attributeType.get(key)!=AttributeType.Discrete) {
			return true;
		}
		
		attributes.put(key, value);
		return false;
	}
	
	public boolean AddContinuousAttribute(String key, long value) {
		if(!attributeType.containsKey(key)) {
			attributeType.put(key, AttributeType.Continuous);
		} else if(attributeType.get(key)!=AttributeType.Continuous) {
			return true;
		}
		
		attributes.put(key, Long.toString(value));
		return false;
	}
	
	public String GetDiscreteAttribute(String key) {
		return attributes.get(key);
	}
	
	public long GetContinuousAttribute(String key) {
		if(attributeType.get(key)==AttributeType.Continuous)
			return Long.parseLong(attributes.get(key));
		return -1;
	}
	

	@Override
	public int compareTo(JobNode obj) {
		if(obj == this) return 0;
		return (int) (this.UID-((JobNode)obj).UID);
	}

}
