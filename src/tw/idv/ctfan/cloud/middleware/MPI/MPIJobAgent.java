package tw.idv.ctfan.cloud.middleware.MPI;

import java.io.BufferedInputStream;
import java.io.IOException;

import jade.core.Agent;
import jade.core.behaviours.Behaviour;
import tw.idv.ctfan.cloud.middleware.Cluster.JobAgent;

public class MPIJobAgent extends JobAgent {
	private static final long serialVersionUID = -3787070421334031975L;
	
	@Override
	protected String OnHeartBeat() {
		return "Hello, There";
	}

	@Override
	protected void StartJob(Agent myAgent, String info) {
		WriteLog("Info: " + info);
		String[] cmd = info.split("\t");
		if(cmd.length!=3) {
			WriteLog("Info size is not 2");
			WriteLog(info);
			JobFinished();
			return;
		}
		String command = "mpiexec --host " + cmd[2] + " -n " + cmd[0] + " " + GetBinaryFullPath() + " " + cmd[1];
		WriteLog(command);
		
		Runtime rt = Runtime.getRuntime();
		Process p;
		
		try {
			rt.exec("chmod 777 " + GetBinaryFullPath());
		} catch(IOException e) {
			e.printStackTrace();
			WriteLog("Error while chmod of the job " + GetBinaryFullPath());
			JobFinished();
			return;
		}
		
		try {
			p = rt.exec(command);
			myAgent.addBehaviour(new MonitorProcessBehaviour(myAgent, p));
		} catch (IOException e) {
			e.printStackTrace();
			WriteLog("Error while starting job");
			this.doDelete();
		}
	}
	
	private class MonitorProcessBehaviour extends Behaviour {
		private static final long serialVersionUID = 2107706107208498708L;
		
		Process m_process;
		BufferedInputStream m_buffInput;
		BufferedInputStream m_buffInput2;
		StringBuffer m_output;
		
		boolean doneYet = false;
		byte[] buff2k = new byte[0x400];
		
		public MonitorProcessBehaviour(Agent a, Process p) {
			super(a);
			
			m_process = p;
			m_output = new StringBuffer();
			m_buffInput = new BufferedInputStream(m_process.getErrorStream());
			m_buffInput2= new BufferedInputStream(m_process.getInputStream());
		}
		
		int bufflen;

		@Override
		public void action() {
			try {				
				if(m_buffInput2.available()>0) {
					if( (bufflen = m_buffInput2.read(buff2k))>0 ) {
						m_output.append(new String(buff2k, 0, bufflen));
						m_output.append("\n==\n");
					}
				}
				
				if(m_buffInput.available()>0) {
					if( (bufflen = m_buffInput.read(buff2k))>0 ) {
						m_output.append(new String(buff2k, 0, bufflen));
						m_output.append("\n--\n");
					}
				}
				WriteLog("--------------------");
				WriteLog(m_output.toString());
				
				if(m_buffInput2.available()>0 || m_buffInput.available()>0) {
					block(1000);
					return;
				}
				
				try {
					m_process.exitValue();
					doneYet = true;
				} catch (IllegalThreadStateException e) {
					System.out.println("Not Finished Yet");
					block(5000);
				}
				
			} catch (Exception e){
				e.printStackTrace();
			}			
		}

		@Override
		public boolean done() {
			if(doneYet == true) JobFinished();
			return doneYet;
		}		
	}
}
