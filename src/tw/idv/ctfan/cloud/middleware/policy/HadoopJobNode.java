package tw.idv.ctfan.cloud.middleware.policy;



public class HadoopJobNode extends JobNodeBase
{
	
	
	/**
	 *   V : rough set element
	 *   
	 * 			Pregiven				Instant					Postgiven
	 * 	--------------------------------------------------------------------------
	 * 			  UID					Current Position		V Executed Position
	 * 			  input folder			Last heartbeats			  output size total
	 * 			  output folder			map status				V execution time
	 * 			V executer				reduce status
	 * 			  command				job status
	 * 			V job size				has Migrate Count
	 * 			V map total
	 * 			V input size total
	 * 			V ALGORITHM
	 */
	
	/**
	 *  Atributes that will be put in rough set
	 *  
	 *  Continuous == need to be discretized
	 *  
	 *  				Name				Discrete/Continuous
	 *  --------------------------------------------------------
	 *  Condition Attributes:
	 *  				ALGORITHM			Discrete
	 *  				MEMORY				Continuous
	 *  				CPU SPEED			Continuous
	 *  				CORE				Continuous
	 *  				Job Size			Continuous
	 *  				Map Total			Continuous
	 *  				Input Size Total	Continuous
	 *  Decision Attributes:
	 *  				Execution Time		Continuous
	 *  				Output Size Total	Continuous
	 */
	
	public static final String JOBTYPENAME = "hadoop";
	
	// Input, output folder of the job
	// Pregiven
	public String		inputFolder;
	public String		outputFolder;
	
	// Current executing status
	// Instant information
	public double		mapStatus;
	public double 		reduceStatus;
		
	
	/**
	 * Example of setting:
	 * 		-input /usr/ctfan/input0 -output /usr/ctfan/output/output1 -command [/usr/ctfan/input0 /usr/ctfan/output1]
	 * 
	 * Command should at the last one, quoted with []
	 * 
	 * [] within the charactor is not considered
	 */
	
	
	// Pregiven map number, input size
	public int			mapNumber;
	public long			inputFileSize;
	
	// Postgiven output size
	public long			outputFileSize;	

	
	public HadoopJobNode(String jobType, String executer, String setting, byte[] job, String inputFolder, String outputFolder)
	{
		super(jobType, executer, setting, job);
		this.inputFolder = inputFolder;
		this.outputFolder = outputFolder;
	}	
	
	// Quick defined test cast constructor
	public HadoopJobNode(int maxMapSlot,
			int maxReduceSlot,
			long JobSize,
			int MapTotal,
			long InputSizeTotal,
			long ExecutionTime,
			long OutputSizeTotal)
	{
		super("hadoop", "", "", new byte[1]);
		this.currentPosition = new ClusterNode(maxMapSlot, maxReduceSlot);
		this.jobSize = JobSize;
		this.mapNumber = MapTotal;
		this.inputFileSize = InputSizeTotal;
		this.executeTime = ExecutionTime;
		this.outputFileSize = OutputSizeTotal;
	}
	
	static public String toHTMLHead()
	{
		String head =
			"<tr>" +
			"<th>" + "UID" + "</th>" +
			"<th>" + "Current<br />Place" + "</th>" +
			"<th>" + "Last<br />Heartbeat" + "</th>" +
			"<th>" + "Map %<br />Complete" + "</th>" +
			"<th>" + "Reduce%<br />Complete" + "</th>" +
			"<th>" + "Map<br />Total" + "</th>" +
			"<th>" + "Input<br />File Size" + "</th>" +
			"<th>" + "Executer" + "</th>" +
			"<th>" + "Job<br />Size" + "</th>" +
			"<th>" + "Job<br />State" + "</th>" +
			"</tr>";
			return head;
	}
	
	public String toHTML()
	{
		String result = 
			"<tr>" +
			"<td>" + UID + "</td>" +
			"<td>" + (currentPosition==null?"null":currentPosition.name) + "</td>" +
			"<td>" + lastExist + "</td>" +
			"<td>" + mapStatus + "</td>" +
			"<td>" + reduceStatus + "</td>" +
			"<td>" + mapNumber + "</td>" +
			"<td>" + inputFileSize + "</td>" +
			"<td>" + executer + "</td>" +
			"<td>" + jobSize + "</td>" +
			"<td>" + (jobStatus==0?"WAITING":
						jobStatus==1?"RUNNING":
							jobStatus==2?"FINISHED":"UNKNOWN") + "</td>" +
			"<td>" + executeTime + "</td>" +
			"</td>";
		
		
		return result;
	}

	static public String toHTMLFinishedHead()
	{
		String head =
			"<tr>" +
			"<th>" + "UID" + "</th>" +
			"<th>" + "Execution<br />Place" + "</th>" +
			"<th>" + "Map<br />Total" + "</th>" +
			"<th>" + "Input<br />File Size" + "</th>" +
			"<th>" + "Executer" + "</th>" +
			"<th>" + "Job<br />Size" + "</th>" +
			"<th>" + "Execute<br />time" + "</th>" +
			"<th>" + "Output<br />File Size" + "</th>" +
			"</tr>";
			return head;
	}
	
	public String toHTMLFinished()
	{
		String result =
			"<tr>" +
				"<td>" + UID + "</td>" +
				"<td>" + currentPosition.name + "</td>" +
				"<td>" + mapNumber + "</td>" +
				"<td>" + inputFileSize + "</td>" +
				"<td>" + executer + "</td>" +
				"<td>" + jobSize + "</td>" +
				"<td>" + executeTime + "</td>" +
				"<td>" + outputFileSize + "</td>" +
			"</tr>";
//		String result =
//			"<tr>" +
//				"<td>" + 1024 + "</td>" +
//				"<td>" + 4 + "</td>" +
//				"<td>" + 1 + "</td>" +
//				"<td>" + jobSize + "</td>" +
//				"<td>" + mapNumber + "</td>" +
//				"<td>" + inputFileSize + "</td>" +
//				"<td>" + executeTime + "</td>" +
//				"<td>" + outputFileSize + "</td>" +
//			"</tr>";
		return result;
	}

	@Override
	public String transferString() {
		return "UID:" + this.UID + "\n" +
		       "InputFolder:" + this.inputFolder + "\n" +
		       "OutputFolder:" + this.outputFolder + "\n" +
		       "Parameter:" + this.command + "\n" +
		       "BinaryDataLength:" + this.binaryFile.length + "\n";
	}
	
}