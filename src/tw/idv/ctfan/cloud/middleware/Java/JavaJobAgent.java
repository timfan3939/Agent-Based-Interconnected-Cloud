package tw.idv.ctfan.cloud.middleware.Java;

import jade.core.Agent;
import jade.core.behaviours.Behaviour;

import java.io.BufferedInputStream;
import java.io.IOException;

import tw.idv.ctfan.cloud.middleware.Cluster.JobAgent;

public class JavaJobAgent extends JobAgent {
	private static final long serialVersionUID = 1L;

	@Override
	protected String OnHeartBeat() {
		return "Hello, There.";
	}

	@Override
	protected void StartJob(Agent myAgent, String info) {
		String command = "java -jar " + GetBinaryFullPath() + " " + info;
		WriteLog(command);
		Runtime rt = Runtime.getRuntime();
		Process p;
		try {
			p = rt.exec(command);
			myAgent.addBehaviour(new MonitorProcessBehaviour(myAgent, p));
		} catch (IOException e) {
			e.printStackTrace();
			WriteLog("Error while Starting job");
			this.doDelete();
		}
	}
	
	private class MonitorProcessBehaviour extends Behaviour {
		private static final long serialVersionUID = 1L;
		
		Process m_process;
		BufferedInputStream m_buffInput;
		StringBuffer m_output;
		
		boolean doneYet = false;
		byte[] buff2k = new byte[0x400];
		
		public MonitorProcessBehaviour(Agent myAgent, Process p) {
			super(myAgent);
			
			m_process = p;
			m_output = new StringBuffer();
			m_buffInput = new BufferedInputStream(m_process.getErrorStream());
		}
		
		int bufflen;

		@Override
		public void action() {
			try {
				if(m_buffInput.available()>0) {
					if( (bufflen = m_buffInput.read(buff2k))>0 ) {
						m_output.append(new String(buff2k, 0, bufflen));
						m_output.append("\n--\n");
					}
					else {
						block(5000);
						return;
					}
				}
				WriteLog("--");
				WriteLog(m_output.toString());
				
				try {
					m_process.exitValue();
					doneYet = true;
				} catch (IllegalThreadStateException e) {
					System.out.println("Not Finished Yet");
					block(5000);
				}
				
			} catch (Exception e) {
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
