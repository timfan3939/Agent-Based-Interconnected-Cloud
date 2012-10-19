package tw.idv.ctfan.cloud.middleware.policy.data;

public class JavaJobNode extends JobNodeBase {

	public JavaJobNode(String jobType, String executer, String setting, byte[] job) {
		super(jobType, executer, setting, job);
	}
	
	public static final String JOBTYPENAME = "java";
	
	
	//Pregiven input data path
	public String inputData;

	@Override
	public String transferString() {
		return "UID:" + this.UID + "\n" +
	       "Parameter:" + this.command + "\n" +
	       "BinaryDataLength:" + this.binaryFile.length + "\n";
	}

}