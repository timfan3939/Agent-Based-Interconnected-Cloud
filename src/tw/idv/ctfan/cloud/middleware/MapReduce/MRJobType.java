package tw.idv.ctfan.cloud.middleware.MapReduce;

import java.net.URI;

import jade.core.ContainerID;
import jade.lang.acl.ACLMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;

/**
 * 
 * @author C.T.Fan
 * 
 * Accepted job type: java hadoop runnable archive, which takes inputFolder and outputFolder as the parameter
 * The size of the job will be the file size in the inputFolder.
 * 
 * Must have attributes:
 * Jobtype:MapReduce
 * Command:<InputFolder and OutputFolder>
 * InputFolder:<path/to/inputFolder>
 * OutputFolder:<path/to/outputFolder>
 * Name:<Program Name> (Optional)
 * Deadline:<The Deadline of the job> (Optional)
 * BinaryDataLength:<length of the Archive> <binaryFileContent>
 *
 */

public class MRJobType extends JobType {

	public MRJobType() {
		super("MapReduce");
	}

	@Override
	public int DecodeClusterLoadInfo(String line) {
		if(line.matches("Free")) 
			return 0;
		else
			return 100;
	}

	@Override
	public ContainerID ExtractContainer(ACLMessage msg) {
		String content = msg.getContent();
		String[] line = content.split("\n");
		ContainerID cid = new ContainerID();
		cid.setName(line[2]);
		return cid;
	}

	@Override
	public String GetExtension() {
		return ".jar";
	}

	@Override
	public String OnDispatchJobMsg(JobNode jn) {
		return ("Command:" + jn.GetDiscreteAttribute("Command"));
	}

	@Override
	public void SetJobInfo(JobNode jn) {
		try {
			FileSystem fs = FileSystem.get(new URI("hdfs://10.133.200.203:9000"), new Configuration());
			
			Path path = new Path(jn.GetDiscreteAttribute("InputFolder"));
			
			FileStatus[] files = fs.listStatus(path);
			
			long fileSize = 0;
			long fileCount = 0;
			if(files!=null) {
				for(FileStatus file:files) {
					fileCount++;
					fileSize += file.getLen();
				}
			}
			
			jn.AddContinuousAttribute("InputFileSize", fileSize);
			jn.AddContinuousAttribute("InputFileCount", fileCount);		
			
			fs.close();
		}
		catch (Exception e) {
			System.err.println("Getting File system info error");
			e.printStackTrace();
			jn.AddContinuousAttribute("InputFileSize", 0);
			jn.AddContinuousAttribute("InputFileCount", 0);		
		}
	}

	@Override
	public void UpdateJobNodeInfo(String line, JobNode jn) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean varifyJob(JobNode jn) {
		if(jn.GetDiscreteAttribute("Command")==null) return false;
		if(jn.GetDiscreteAttribute("InputFolder")==null) return false;
		if(jn.GetDiscreteAttribute("OutputFolder")==null) return false;
		return true;
	}

}
