package tw.idv.ctfan.cloud.middleware.Java;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;

import tw.idv.ctfan.cloud.middleware.Cluster.*;

public class JavaAdminAgent extends AdminAgent {

	public JavaAdminAgent() {
		super(new JavaJobType());
	}

	private static final long serialVersionUID = 1L;

	@Override
	public JobListNode OnDecodeNewJob(byte[] data) {
		int c=0;
		int buffLen;
		byte[] buff = new byte[0x400], binary = null;
		ArrayList<String> cmd = new ArrayList<String>();
		
		String UID = "";
		
		ByteArrayInputStream dataInput = new ByteArrayInputStream(data);
		
		while(dataInput.available()>0) {
			buffLen =0;
			while( (c=dataInput.read()) != '\n' ) {
				buff[buffLen] = (byte)c;
				buffLen++;
			}
			
			String line = new String (buff, 0, buffLen);
			int index = line.indexOf(":");
			String head = line.substring(0, index);
			String tail = line.substring(index+1);
			
			if(head.matches("UID")) {
				UID = tail;
			} else if(head.matches("BinaryDataLength")){
				try {
					int jobLength = Integer.parseInt(tail);
					binary = new byte[jobLength];
					dataInput.read(binary, 0, jobLength);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if(head.matches("Parameter")) {
				cmd.add(tail);
			}
		}
		
		if(UID.isEmpty()||binary==null) return null;
		return new JobListNode(UID, cmd, binary);
	}

	@Override
	protected String OnEncodeJobInfo(JobListNode jn) {
		long currentTime = System.currentTimeMillis();
		return "java " + jn.name + " running " + (currentTime-jn.lastExist) + " " + (currentTime-jn.executedTime);
	}

	@Override
	protected String OnEncodeLoadInfo() {
		return (super.m_jobList.size()>0?"Busy":"Free");
	}

	@Override
	public void OnTerminateCluster() {
		System.out.println("I'm going to be terminated.");
	}
}
