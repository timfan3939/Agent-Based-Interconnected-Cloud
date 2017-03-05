package tw.idv.ctfan.cloud.middleware.Workflow;

import jade.core.Agent;

import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

//import sun.org.mozilla.javascript.internal.json.JsonParser;
import tw.idv.ctfan.cloud.middleware.policy.*;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;
import jade.core.behaviours.OneShotBehaviour;
import jade.core.behaviours.ThreadedBehaviourFactory;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;
import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.Java.JavaJobType;
import tw.idv.ctfan.cloud.middleware.Workflow.WorkflowStatus;

import org.json.*;

public class WorkflowAgent extends Agent{
	private static final long serialVersionUID = -5271213701466983534L;
	ThreadedBehaviourFactory tbf;
	
	public String zipFilePath;
	public String destDirectory;
	String m_pathToFile;
	private final String fileDirectory = "C:\\ctfan\\MYPAPER\\testfile\\middlewareFile\\";
	
	public boolean childrenIDTF[][] = new boolean[210][210] ;
	public int tpSortResult [] = new int[210];
	public int tasksID[] = new int[210];
	public int parentsID[][] = new int[210][210];
	public int childrenID[][] = new int[210][210];
	public int numberOfParentTasks[] = new int[210];
	public int numberOfChildTasks[] = new int[210];	
	public String program[] = new String[210];
	public int taskLevelSort[][] = new int[210][210];
	public int VMnum = 0;
	public int taskExecutionTime[] = new int[210];
	public int taskLevelExecutionTime[] = new int[210];
	private static final int BUFFER_SIZE = 40960;
	public ArrayList<Integer> taskDispatchOrderList = new ArrayList<Integer>();
	//static WorkflowStatus Test2 = new WorkflowStatus();
	Policy policy = MultiTypePolicy.GetPolicy();
	
	public void setup() {
		super.setup();
		
		tbf = new ThreadedBehaviourFactory();	
		
		policy = MultiTypePolicy.GetPolicy();
	
		this.addBehaviour(new JobParseBehavior(this));
		//this.addBehaviour(tbf.wrap(new SubmitBehaviour(this) ) );
		//this.addBehaviour(tbf.wrap(new HTTPServerBehaviour(this, policy) ) );
		this.addBehaviour(tbf.wrap(new ListeningBehaviour(this) ) );
		
	}
		
	/*******************parse job**************************************/
	private class JobParseBehavior extends OneShotBehaviour{
		
		private static final long serialVersionUID = 1L;
		
		public JobParseBehavior(Agent agent){
			super(agent);
		}
		
		@Override
		public void action() {
			zipFilePath = "C:\\ctfan\\MYPAPER\\testfile\\middlewareFile\\" + myAgent.getLocalName() + ".zip";
			destDirectory = "C:\\ctfan\\MYPAPER\\testfile\\unziptest\\" + myAgent.getLocalName() + "\\";
			
			File saveFile=new File("C:\\ctfan\\MYPAPER\\testfile\\exetime\\"+myAgent.getLocalName()+"exetime.txt");
			try
			{
				FileWriter fwriter=new FileWriter(saveFile, true);
				fwriter.write("startparsetime:"+System.currentTimeMillis()+"\n");
				fwriter.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
			
//			System.out.println("test agentlocalname:" + myAgent.getLocalName()
//					+ "\nzipFilePath: " + zipFilePath 
//					+ "\ndestDirectory: " + destDirectory);
			try {
				unzip(zipFilePath, destDirectory);
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {				
				//initial childrenIDTF[][]
				for(int i = 0; i < childrenIDTF.length; i++){
					for(int j = 0; j < childrenIDTF.length; j++){
						childrenIDTF[i][j] = false;
					}
				}			
				
				// read Job json file
//				FileReader fd = new FileReader("C:\\ctfan\\MYPAPER\\testfile\\unziptest\\" + myAgent.getLocalName() + "\\workflow.json");
				String content = new String(Files.readAllBytes(Paths.get("C:\\ctfan\\MYPAPER\\testfile\\unziptest\\" + myAgent.getLocalName() + "\\workflow.json")));
				JSONObject jsonObject = new JSONObject(content);
				 
				//read the taskworkflow array		
				JSONArray workflowTaskJsonArray = jsonObject.getJSONArray("taskworkflow");
					for(int i = 0; i < workflowTaskJsonArray.length(); i++){
						JSONObject jTemp = workflowTaskJsonArray.getJSONObject(i);
						// get taskname from the JSON object
//						String taskname = jTemp.getString("taskname");
						/***********************test******************************************/
//						System.out.println("taskname =" + taskname);
						/*********************************************************************/
						
						// get program from the JSON object
						String programname = jTemp.getString("program");
						/*************************test****************************************/
//						System.out.println("programname =" + programname);
						/*********************************************************************/
						program[i] = programname;
						//m_task.taskprogram = programname;
		
						// get taskID from the JSON object
						int TID =  jTemp.getInt("task_id");
						/*************************test****************************************/
//						System.out.println("TID =" + TID);
						/*********************************************************************/
						tasksID[i] = TID;
						//m_task.TID = TID;
						
						// get taskID from the JSON object
						int command =  jTemp.getInt("command");
						/*************************test****************************************/
//						System.out.println("exetime =" + command);
						/*********************************************************************/
						taskExecutionTime[i] = command;
						//m_task.TID = TID;
						
						// get _inE array from the JSON object. parents node			
						if ( ! jTemp.isNull("_inE")  ){
							JSONArray inE = jTemp.getJSONArray("_inE");
							for(int j=0; j<inE.length(); j++){
								JSONObject jTemp2 = inE.getJSONObject(j);
								// get _outV from the JSON object
								Integer PID =  jTemp2.getInt("_outV");
								/********************test*********************************************/
//								System.out.println("PID =" + PID);
								/*********************************************************************/
								parentsID[i][j] = PID;
							}
							// get _outE array from the JSON object
							numberOfParentTasks[i] = inE.length();
							/*********************test********************************************/
//							System.out.println("parentnum =" + parentnum[i]);
							/*********************************************************************/
						}else{
							numberOfParentTasks[i] = 0;
							/*********************test********************************************/
//							System.out.println("parentnum =" + parentnum[i]);
							/*********************************************************************/
						}
						
						// get _outE array from the JSON object. children node
						if ( ! jTemp.isNull("_outE")  ){				
							JSONArray outE = jTemp.getJSONArray("_outE");
							for(int k=0; k<outE.length(); k++){
								JSONObject jTemp3 = outE.getJSONObject(k);
								// get _inV from the JSON object
								Integer CID =  jTemp3.getInt("_inV");
								/*******************test**********************************************/
//								System.out.println("CID =" + CID);
								/*********************************************************************/
								childrenID[i][k]=CID;
								childrenIDTF[i][CID-1] = true;
							}
							// get _outE array from the JSON object
							numberOfChildTasks[i] = outE.length();
							/*********************test********************************************/
//							System.out.println("childnum =" + childnum[i] + "\n");
							/*********************************************************************/
						}
						else{
							numberOfChildTasks[i] = 0;
							/************************test*****************************************/
							System.out.println("childnum =" + numberOfChildTasks[i]);
							/*********************************************************************/
						}
					}
					/*************test childrenIDTF[][] boolean*******************************/
//					for(int i = 0; i < taskworkflow.length(); i++){
//						for(int j = 0; j < taskworkflow.length(); j++){
//							System.out.print(childrenIDTF[i][j] + "\t");
//						}
//						System.out.print("\n");
//					}
					/*************************topological sort****************************/
					topological(workflowTaskJsonArray.length());
					/*********************************************************************/
					/********************************test*********************************/
					System.out.println("topological sort result");
					for(int i = 0; i < workflowTaskJsonArray.length();i++){
						System.out.println("task:" + (tpSortResult[i] + 1));
					}
					/*********************************************************************/
					
					/**********************level sort******************************/
					int taskLV[] = new int[workflowTaskJsonArray.length()];
					for(int i = workflowTaskJsonArray.length()-1; i >= 0; i--){
						int a = tpSortResult[i];
						if(numberOfChildTasks[a] == 0){						
							taskLV [a] = 0;
							taskLevelExecutionTime[a] = taskExecutionTime[a];
						}
						else{
							int taskLVexchange[] = new int[2];
							int taskLVtimeexchange[] = new int[2];
							for(int k = 0; k < 2; k++){
								taskLVexchange[k] = 0;
								taskLVtimeexchange[k] = 0;
							}
							for(int j = 0; j < numberOfChildTasks[a]; j++){
								int b = childrenID[a][j] - 1;
	//							System.out.println(b);
								taskLVexchange[1] = taskLV[b] + 1;
								if(taskLVexchange[1] > taskLVexchange[0]){
									taskLVexchange[0] = taskLVexchange[1];
								}
								taskLVtimeexchange[1] = taskLevelExecutionTime[b];
								if(taskLVtimeexchange[1] > taskLVtimeexchange[0]){
									taskLVtimeexchange[0] = taskLVtimeexchange[1];
								}
							}
							taskLV[a] = taskLVexchange[0];
							taskLevelExecutionTime[a] = taskExecutionTime[a] + taskLVtimeexchange[0];
						}
					}
					/*********************************************************************/
					/*******************************test**********************************/
					System.out.println("level sort result");
					for(int i = 0; i < workflowTaskJsonArray.length();i++){
						System.out.println("task " + tasksID[i] +" level:" + taskLV[i] + 
								"\t" + "  taskexetime: " + taskExecutionTime[i] + 
								"\t" + "  total exetime: " +taskLevelExecutionTime[i]);
					}
					/*********************************************************************/
					/************************taskLV sort*********************************/
					//initial taskLVsort
					for(int i=0;i<workflowTaskJsonArray.length();i++){
						for(int j=0;j<workflowTaskJsonArray.length();j++){
							taskLevelSort[i][j] = -2;
						}
					}
					int count = 0;
					for(int i = 0; i < workflowTaskJsonArray.length(); i++){
						int k = 0;
						for(int j = 0; j < workflowTaskJsonArray.length(); j++){
							if(taskLV[j] == i){
								taskLevelSort[i][k] = j;
								k++;
								count++;							
							}
						}
						if(count == workflowTaskJsonArray.length()){
							break;
						}
						if(VMnum < k){
							VMnum = k;
						}
					}
					/***********test taskLVsort********************/
					count=0;
					for(int i=0;i<workflowTaskJsonArray.length();i++){
						System.out.print("taskLVsort"+i+": ");
						for(int j=0;j<workflowTaskJsonArray.length();j++){
							if(taskLevelSort[i][j] >= 0){
								System.out.print(taskLevelSort[i][j]+1+" ");
								count++;
							}
							else{
								break;
							}
						}
						System.out.println("");
						if(count==workflowTaskJsonArray.length())
							break;
					}
					System.out.print("VM number: " + VMnum + "");
					/**************dispatchorder******************/
					int newsort[]=new int[workflowTaskJsonArray.length()];
					for(int i = 0; i < workflowTaskJsonArray.length(); i++){
						newsort[i] = tasksID[i]-1;
					}
					int j, m;
					for(int i = 1; i < newsort.length; i++){
							m = newsort[i];
							for(j = i - 1; j >= 0 && taskLevelExecutionTime[newsort[j]] < 
														taskLevelExecutionTime[m]; j--){
								newsort[j + 1] = newsort[j];
							}
							newsort[j + 1] = m;
					}

					
					for(int i = 0; i < newsort.length; i++){
						taskDispatchOrderList.add(newsort[i]+1);
					}
					System.out.println("\ndispatch sequence: ");
					for(int i = 0; i < taskDispatchOrderList.size(); i++){
						System.out.print(taskDispatchOrderList.get(i) + " ");
					}
					System.out.println("");
					/*********************longest path******************************/
					//File saveFile=new File("D:\\MYPAPER\\testfile\\exetime\\"+myAgent.getLocalName()+"exetime.txt");
					int longestpath = newsort[0];
					try
					{
						FileWriter fwriter=new FileWriter(saveFile, true);
						fwriter.write(longestpath+1+ " ");
						fwriter.close();
					}catch(Exception e){
						e.printStackTrace();
					}
					while(numberOfChildTasks[longestpath] != 0){
						int longestpathchild = childrenID[longestpath][0];
						if(numberOfChildTasks[longestpath] == 1){
							longestpath = longestpathchild - 1;
						}
						else{
							for(int i = 1; i < numberOfChildTasks[longestpath]; i++){
								if(taskLevelExecutionTime[childrenID[longestpath][i]-1] 
										> taskLevelExecutionTime[longestpathchild-1]){
									longestpathchild = childrenID[longestpath][i];
								}
								longestpath = longestpathchild-1;
							}
						}
						try
						{
							FileWriter fwriter=new FileWriter(saveFile, true);
							fwriter.write(longestpath+ 1 + " ");
							fwriter.close();
						}catch(Exception e){
							e.printStackTrace();
						}
					}
					try
					{
						FileWriter fwriter=new FileWriter(saveFile, true);
						fwriter.write("\n parsefinishtime" + System.currentTimeMillis()+"\n");
						fwriter.close();
					}catch(Exception e){
						e.printStackTrace();
					}
					/***************************************************************/
					for(int i = 0; i < newsort.length; i++){
						String m_pathToFile = destDirectory + program[newsort[i]];
						FileInputStream fin = new FileInputStream(m_pathToFile);
						ByteArrayOutputStream binary = new ByteArrayOutputStream();
						byte[] buff = new byte[1024];
						int len = 0;
						while( (len = fin.read(buff)) > 0)
						{
							binary.write(buff, 0, len);
						}
						fin.close();
						
//						int read = input.read(jobBinaryFile, 0, binary.size());
//						while(read<binary.size() && (ch = input.read(buff)) > 0) {
//							for(int l=0; l<ch; l++) {
//								jobBinaryFile[read+l] = buff[l];
//							}
//							read += ch;
//						}
						
						JobNode newTask = new JobNode();
						newTask.jobType = new JavaJobType();
//						
						long agentlocalname = Long.parseLong(myAgent.getLocalName());
						String cmd = String.valueOf(taskExecutionTime[newsort[i]]*10000);						
						newTask.UID = agentlocalname*1000 + (long)newsort[i]+1;
						newTask.setNumberOfParentTasks(numberOfParentTasks[newsort[i]]);
						newTask.setWorkflowTotalTaskNumber(taskDispatchOrderList.size());
//						System.out.println("dispatchnum:"+m_task.getdispatchnum());
						for(int d = 0; d < newsort.length; d++){
							newTask.setWorkflowDispatchSequence(String.valueOf(agentlocalname*1000 + ((long)newsort[d]+1)));
						}
//						for(int d = 0; d < newsort.length; d++){
//							System.out.println("dispatchsequence:"+m_task.getdispatchsequence(d));
//						}
//						System.out.println("");
						for(int p = 0; p < numberOfParentTasks[newsort[i]]; p++){
							newTask.addParentTasksUID(String.valueOf(agentlocalname*1000 + (long)parentsID[newsort[i]][p]));
						}
						newTask.AddDiscreteAttribute("Name", program[newsort[i]]);
//						m_task.submitTime = System.currentTimeMillis();
						newTask.AddDiscreteAttribute("Command", cmd);
						newTask.AddDiscreteAttribute("JobType", "Java");
//						m_task.AddContinuousAttribute("JobSize", size);
//						m_task.deadline = Long.parseLong(String.valueOf(taskLVexetime[newsort[i]]*1000));
//						m_task.DisplayDetailedInfo();
						
						File file1=new File(m_pathToFile);  
						File file2=new File(destDirectory + newTask.UID + newTask.jobType.GetExtension());  
//						boolean flag = file1.renameTo(file2);
						java.nio.file.Files.copy(file1.toPath(), file2.toPath());
						File f1 = new File(destDirectory + newTask.UID + newTask.jobType.GetExtension());
					    File f2 = new File(fileDirectory + newTask.UID + newTask.jobType.GetExtension());
					    InputStream in = new FileInputStream(f1);
					    OutputStream out = new FileOutputStream(f2);

					      byte[] buf = new byte[1024];
					      int lenth;
					      while ((lenth = in.read(buf)) > 0){
					        out.write(buf, 0, lenth);
					      }
					    in.close();
					    out.close();
					    byte[] jobBinaryFile = new byte[binary.size()];
						//m_task.jobType.SetJobInfo(m_task);
						myAgent.addBehaviour(tbf.wrap(new GetTaskInfoBehaviour(myAgent, newTask, jobBinaryFile)));												
//						if(parentnum[newsort[i]]!=0){
//							for(int p = 0; p < m_task.getparentsnum(); p++){
//								System.out.println(m_task.UID+"'s parentsUID:"+m_task.getparentsUID(p)+"\n");
//							}
//						}else{
//							System.out.println(m_task.UID+"'s parents:0");
//						}
					}
					System.out.println();
											
					
					/***************************************************************/
					//set tasks' UUID all to null;
					synchronized (WorkflowStatus.class) {
						for(int i = 0; i < workflowTaskJsonArray.length(); i++){
							WorkflowStatus.setUUID(i, null);
						}
					}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NullPointerException ex) {
	            ex.printStackTrace();
	        } catch (JSONException e) {
				e.printStackTrace();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	private class GetTaskInfoBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 1L;
		JobNode m_job;
//		byte[] m_binary;
		GetTaskInfoBehaviour(Agent a, JobNode jn, byte[] binaryFile) {
			super(a);
			m_job = jn;
			m_job.submitTime = System.currentTimeMillis();
//			m_binary = binaryFile;
		}
		@Override
		public void action() {
			String jobType = m_job.GetDiscreteAttribute("JobType");
			System.out.println("\njobType:"+jobType);
			if(jobType == null) {
				System.err.println("Job Type not found");
//				m_binary = null;
				return;
			}
			JobType jt = null;
			synchronized (policy) {
				for(JobType jobTypeIter : policy.GetJobTypeList()){
//					System.out.println("\nforsize"+policy.GetJobTypeList());
					if(jobTypeIter.getTypeName().compareTo(jobType)==0){
						jt = jobTypeIter;
//						System.out.println("\njt"+jt);
						break;
					}
				}
			}
			if(jt == null) {
				System.err.println("No Such Job Type Exists");
//				m_binary = null;
				return;
			}
			m_job.jobType = jt;
//			try {
//				FileOutputStream fos = new FileOutputStream(fileDirectory + m_job.UID + m_job.jobType.GetExtension());
//				fos.write(m_binary);
//				fos.close();				
//			} catch(Exception e) {
//				System.err.println("Writing binary file error");
//				e.printStackTrace();
//				m_binary = null;
//				return;
//			}
			m_job.jobType.SetJobInfo(m_job);
//			m_binary = null;
//			System.out.println("test1:"+m_job.jobType.getTypeName());
			synchronized(policy) {
				policy.AppendNewJob(m_job);
			}
//			m_job.DisplayDetailedInfo();
		}
	}

		// topological sort
	private void topological(int a){
		int newsize = a;
	    int ref [] = new int[newsize];
				
		for(int i = 0; i < a; i++){
			ref[i] = 0;
		}
		for(int i = 0; i < a; i++){
			for(int j = 0; j < a; j++){
				if(childrenIDTF[i][j]){
//				if(parentsIDTF[i][j]){
					ref[j]++;
				}
			}
		}
		for(int i = 0; i < a; i++){
			int s = 0;
			while(s < a && ref[s] != 0){
				++s;
			}
			if(s == a){
				break;
			}
			ref[s] = -1;
				
			tpSortResult[i] = s;
			for(int t = 0; t < a; t++){
				if(childrenIDTF[s][t]){
					ref[t]--;
				}
			}
		}
	}
	
	//unzip file
	private void unzip(String zipFilePath, String destDirectory) throws IOException {
        File destDir = new File(destDirectory);
        if (!destDir.exists()) {
            destDir.mkdir();
        }
        ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
        ZipEntry entry = zipIn.getNextEntry();
        // iterates over entries in the zip file
        while (entry != null) {
            String filePath = destDirectory + File.separator + entry.getName();
            if (!entry.isDirectory()) {
                // if the entry is a file, extracts it
                extractFile(zipIn, filePath);
            } else {
                // if the entry is a directory, make the directory
                File dir = new File(filePath);
                dir.mkdir();
            }
            zipIn.closeEntry();
            entry = zipIn.getNextEntry();
        }
        zipIn.close();
    }
	private void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
        byte[] bytesIn = new byte[BUFFER_SIZE];
        int read = 0;
        while ((read = zipIn.read(bytesIn)) != -1) {
            bos.write(bytesIn, 0, read);
        }
        bos.close();
    }
	
	private class ListeningBehaviour extends OneShotBehaviour
	{
		private static final long serialVersionUID = 1L;

		public ListeningBehaviour(Agent agent){
			super(agent);
		}

		
		/*
		 * Request	: new job
		 * Propose	: job Execution (but have to wait)
		 * Inform	: job execution/migration
		 * Confirm	: job exists 
		 */
		
		@Override
		public void action() {
			try {
				ACLMessage msg = myAgent.receive(MessageTemplate.MatchAll());
				System.err.println("Workflow Agent got a message");
				Thread.sleep(315000);
				myAgent.doDelete();
				
				if(msg==null) {
					block();
					return;
				}
				
				switch(msg.getPerformative()) {
				
				case ACLMessage.INFORM: {
					String info = msg.getContent();
					if(info.matches("killWA")) {
						myAgent.doDelete();
					}
				} break;
				case ACLMessage.PROPOSE:
				}				
				
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
//	private JobNode FindAndUpdateJobNode(String line) {
//		String[] subLine = line.split(" ");
//		JobNode jn = null;
//		for(JobNode j:policy.GetRunningJob()) {
//			if(Long.parseLong(subLine[0])==j.UID) {
//				jn = j;
//				if(subLine[1].matches("Finished")) {
//					policy.GetRunningJob().remove(jn);
//					policy.GetFinishJob().add(jn);
//					jn.finishTime = System.currentTimeMillis();
//					jn.completionTime = Long.parseLong(subLine[3]);
//					String name = jn.GetDiscreteAttribute("Name");
//					String cmd = jn.GetDiscreteAttribute("Command");
//					((MultiTypePolicy)policy).WriteLog("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
//							  "<td>" + jn.jobType.getTypeName() + "</td>" +
//							  "<td>" + (name==null?"N/A":name) + "</td>" +
//							  "<td>" + (cmd==null?"N/A":cmd) + "</td>" +
//							  "<td>" + jn.runningCluster.clusterName + "</td>" +
//							  "<td>" + jn.submitTime + "</td>" +
//							  "<td>" + jn.startTime + "</td>" +
//							  "<td>" + jn.finishTime + "</td>" +
//							  "<td>" + jn.completionTime + "</td>" +
//							  "<td>" + jn.GetContinuousAttribute("PredictionTime") + "</td>" +
//							  "<td>" + jn.deadline + "</td>" +
//							  "</tr>");
//				}
//				else if(subLine[1].equals("Running")){
//					jn.lastSeen = Long.parseLong(subLine[2]);
//					jn.completionTime = Long.parseLong(subLine[3]);
//				}
//				else if(subLine[1].matches("Waiting")) {
//					jn.lastSeen = Long.parseLong(subLine[2]);
//				}
//				break;
//			}
//		}
//		if(jn != null){
//			// TODO: parse the line
//		}
//		
//		return jn;
//	}
}