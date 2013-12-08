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
	
	/**
	 * Representing job's status
	 * @author C.T.Fan
	 *
	 */
	static public enum JobStatus {
		Waiting, Running, Finished;
	}
	
	// Attributes that not in HashMap
	/**
	 * Job's UID
	 */
	public long UID;
	
	/**
	 * Job's status<br/>
	 * @see JobStatus
	 */
	public JobStatus status;
	
	/**
	 * The amount of time the job being executed.<br/>
	 * If the job is not finish, it shows the eclipse of the time.
	 */
	public long completionTime = -1;
	
	/**
	 * Deadline attribute.
	 */
	public long deadline;
	
	public boolean isDeadlineJob() {
		return deadline>0;
	}
	
	public long GetTrueDeadline() {
		return (this.isDeadlineJob()?(this.submitTime+this.deadline*1000):-1);
	}
	
	/**
	 * The type of the job.
	 * @see JobType
	 */
	public JobType jobType;
	
	/**
	 * The cluster the job is running on.<br/>
	 * null if not running on any cluster.
	 */
	public ClusterNode runningCluster = null;
	
	/**
	 * -+-----------------+-------------------+------------------------------------------------------
	 *  |-Submit time     |-Start time        |-finish time
	 *                    |--Execution time---|
	 */
	public long submitTime;
	public long startTime;
	public long finishTime;
	public long lastSeen;
	
	public void SetExists() {
		lastSeen = System.currentTimeMillis();
	}

	/**
	 * The attributes of a job.<br/>
	 * {@link JobNode#AddContinuousAttribute(String, long)}, {@link JobNode#AddDiscreteAttribute(String, String)} should be used to add attributes.<br/>
	 * {@link JobNode#GetContinuousAttribute(String)}, {@link JobNode#GetDiscreteAttribute(String)} should be used to get attribute values.<br/>
	 * The attribute key will be stored in {@link JobNode#attributes}.  Type of attribute will be check before adding/getting the value.
	 */
	private HashMap<String,String> attributes;
	
	/**
	 * Type of attributes.
	 * @see AttributeType
	 */
	static public HashMap<String, AttributeType> attributeType = new HashMap<String, AttributeType>();
	
	
	/**
	 * Default Constructor
	 */
	public JobNode(){
		this.UID = System.nanoTime();
		this.completionTime = -1;
		this.deadline = -1;
		attributes = new HashMap<String, String>();
	}
	
	/**
	 * Encapsulate the job's attribute and return the string.
	 * @return
	 */
	public String EncapsulateJob() {
		String job = "";
		job += "UID:" + UID + "\n";
		job += "JobType:" + jobType.getTypeName() + "\n";
		for(String typeName:attributeType.keySet()) {
			switch(attributeType.get(typeName)) {
			case Discrete:
				String value;
				if( (value = attributes.get(typeName)) != null ) {
					job += typeName + ":Discrete:" + value + "\n";
				}
				break;
			case Continuous:
				Long valueL;
				if( (valueL = this.GetContinuousAttribute(typeName)) >0 ) {
					job += typeName + ":Continuous:" + valueL + "\n";
				}
				break;
			}
		}
		
		return job;
	}
	
	/**
	 * This function is only used at the {@link AdminAgent} site.
	 * Note that jobType will not be set when return.
	 * @param job
	 * @param jt
	 * @return
	 */
	public boolean DecapsulateJob(String job, JobType jt) {
		String[] jobInfo = job.split("\n");
		UID = Long.parseLong(jobInfo[0].split(":")[1]);
		for(int i=2; i<jobInfo.length; i++) {
			String[] lineInfo = jobInfo[i].split(":");
			if(lineInfo.length==3) {
				if(lineInfo[1].equals("Discrete")) {
					this.AddDiscreteAttribute(lineInfo[0], lineInfo[2]);
				} else if(lineInfo[1].equals("Continuous")) {
					this.AddContinuousAttribute(lineInfo[0], Long.parseLong(lineInfo[2]));
				}
			}
		}
		return jobInfo[1].split(":")[1].equals(jt.getTypeName());
	}
	
	private boolean ReservedAttributeKey(String key) {
//		if(key.equals("JobType")) return true;
		return false;
	}
	
	/**
	 * @see JobNode#attributes
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean AddDiscreteAttribute(String key, String value){
		if(!attributeType.containsKey(key)) {
			attributeType.put(key, AttributeType.Discrete);
		} else if(ReservedAttributeKey(key)) {
			return true;
		} else if(attributeType.get(key)!=AttributeType.Discrete) {
			return true;
		}
		
		attributes.put(key, value);
		return false;
	}
	
	/**
	 * @see JobNode#attributes
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean AddContinuousAttribute(String key, long value) {
		if(!attributeType.containsKey(key)) {
			attributeType.put(key, AttributeType.Continuous);
		} else if(ReservedAttributeKey(key)) {
			return true;
		}else if(attributeType.get(key)!=AttributeType.Continuous) {
			return true;
		}
		
		attributes.put(key, Long.toString(value));
		return false;
	}
	
	/**
	 * @see JobNode#attributes
	 * @param key
	 * @return
	 */	
	public String GetDiscreteAttribute(String key) {
		if(!ReservedAttributeKey(key))
			return attributes.get(key);
		else
			return GetDiscreteReservedAttributeKey(key);
	}
	
	/**
	 * @see JobNode#attributes
	 * @param key
	 * @return
	 */
	public long GetContinuousAttribute(String key) {
		if(!ReservedAttributeKey(key)) {
			if(attributeType.get(key)==AttributeType.Continuous&&attributes.containsKey(key))
				return Long.parseLong(attributes.get(key));
			else return -1;
		} else
			return GetContinuousReservedAttributeKey(key);
	}	
	

	private long GetContinuousReservedAttributeKey(String key) {		
		return -1;
	}
	
	private String GetDiscreteReservedAttributeKey(String key) {
		if(key.equals("JobType")) return this.jobType.getTypeName();
		return null;
	}
	
	
	@Override
	public int compareTo(JobNode obj) {
		if(obj == this) return 0;
		return (int) (this.UID-((JobNode)obj).UID);
	}
	
	/**
	 * Simple function to display every attributes in a {@link JobNode}
	 */
	public void DisplayDetailedInfo() {
		System.out.println("-----Job Information-----");
		System.out.println("JobUID: " + this.UID);
		System.out.println("Deadline: " + this.deadline);
		for(String typeName: JobNode.attributeType.keySet()) {
			switch(JobNode.attributeType.get(typeName)) {
			case Discrete:{
				String s = this.GetDiscreteAttribute(typeName);
				System.out.println(typeName + ":" + (s==null?"null":s));
			}
			break;
			case Continuous:
				long l = this.GetContinuousAttribute(typeName);
				System.out.println(typeName + ":" + (l==-1?"null":l));
				break;
			}
		}
		System.out.println("-----The End of Job Information-----");
	}
}
