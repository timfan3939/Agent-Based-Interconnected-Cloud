package tw.idv.ctfan.cloud.middleware.policy.data;

public abstract class JobNodeBase implements Comparable<Object> {
	
	public static final int WAITING = 0;
	public static final int RUNNING = 1;
	public static final int FINISHED= 2;
		
	
	
	/**
	 *   V : rough set element
	 *   
	 * 			Pregiven				Instant					Postgiven
	 * 	--------------------------------------------------------------------------
	 * 			  UID					Current Position		V Executed Position
	 * 			V executer				Last heartbeats			V execution time
	 * 			V command				job status
	 * 			V program size			has Migrated Count
	 * 			V input size total
	 * 			V job name
	 * 			  predicted time
	 */
	
	/**
	 *  Atributes that will be put in rough set
	 *  
	 *  Continuous == need to be discretized
	 *  
	 *  				Name				Discrete/Continuous
	 *  --------------------------------------------------------
	 *  Condition Attributes:
	 *  				executer			Discrete
	 *  				Program Size		Continuous
	 *  				Input Size Total	Continuous
	 *  				MEMORY				Continuous
	 *  				CPU SPEED			Continuous
	 *  				CORE				Continuous
	 *  Decision Attributes:
	 *  				Execution Time		Continuous
	 */
	
	// Job's submit ID, which is also the time the user submit the job
	// Pregiven
	public long			UID;
	public String		jobName;
	
	// Job's position
	// Instant information
	public ClusterNode	currentPosition;
	
	// Job's Type
	// Supported: java, hadoop
	public String jobType;
	
	// Last time the heartbeat being heard
	// Instant information
	public long			lastExist;
	
	// Job Status 0 waiting;
	// Job Status 1 running;
	// Job Status 2 finished
	// Instant information
	public int 			jobStatus;
	
	// value that waits until timeout after a migration
	// instant information
	public int 			hasMigrate = 0;
	
	// User's information
	// Pregiven
	public String		executer;
	

	// Job's information
	// Pregiven job size
	public long			jobSize;
	
	// Postgiven execution time
	public long			executeTime;
	
	// Pregiven predicted time
	public long 		predictTime;
	public long			hasBeenExecutedTime;
	public long			deadline;

	// pregiven
	public byte[]		binaryFile;
	

	// Pregiven
	public String		command;
	
	
	// finishTime
	public long			startTime;
	public long 		finishTime;
	
	public JobNodeBase(String jobType, String executer, String setting, byte[] job)
	{
		this.jobType = jobType;
		this.UID = System.nanoTime();
		this.startTime = System.currentTimeMillis();
		this.executer = executer;
		this.command = setting;
		if(job!=null)
			this.binaryFile = job.clone();
		else
			this.binaryFile = null;
	}
	
	@Override
	public int compareTo(Object obj) {
		if(obj == this)
			return 0;
		return (int)(this.UID - ((JobNodeBase)obj).UID);
	}
	
	public String toString()
	{
		return Long.toString(UID);
	}
	
	public abstract String transferString();

}
