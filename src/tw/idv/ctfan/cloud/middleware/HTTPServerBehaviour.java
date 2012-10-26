package tw.idv.ctfan.cloud.middleware;

import java.io.BufferedInputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

import tw.idv.ctfan.cloud.middleware.policy.Policy;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;
import tw.idv.ctfan.cloud.middleware.policy.data.VMMasterNode;

import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.OneShotBehaviour;


public class HTTPServerBehaviour extends CyclicBehaviour {
	private static final long serialVersionUID = 1L;
	private final byte[] EOL = { (byte)'\r', (byte)'\n' };
	ServerSocket server;
	Policy policy;
	
	static final Date m_initTime = new Date();
	
	private static final int HTTP_POST = 0x301;
	private static final int HTTP_GET  = 0x302;

	public HTTPServerBehaviour(AdministratorAgent agent, Policy policy) {
		super(agent);
		this.policy = policy;
		
		try {
			server = new ServerSocket(50071);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void action() {
		try {
			Socket s = server.accept();
			BufferedInputStream input  = new BufferedInputStream(s.getInputStream());
			boolean endOfHeader = false;
			
			int connectionType = 0;
			long contentLength = 0;
			String file = "";
			int line = 0;
			
			do {
				boolean endOfField = false;
				String field = "";
				int ch = input.read();
				do {
					if(ch=='\r') {
						if((ch=input.read())=='\n'){
							endOfField = true;
						} else {
							field += '\r';
						}
					} else {
						field += (char)ch;
						ch = input.read();
					}
				} while (!endOfField);
				if(field.isEmpty()) {
					endOfHeader = true;
					break;
				}
				System.out.println("" + (++line) + ": " + field);
				
				String[] info = field.split(" ");
				if(info.length==0) break;
				
				if(line == 1) {
					if(info[0].matches("GET")) {
						connectionType = HTTP_GET;
						file = info[0];					
					} else if(info[0].matches("POST")) {
						connectionType = HTTP_POST;
						file = info[0];
					} else {
						PrintStream output = new PrintStream(s.getOutputStream());
						output.print("HTTP/1.0 501 Not Implemented" + EOL);
						output.print("Server: tw.idv.ctfan.cloud.middleware.HTTPServerBehaviour" + EOL);
						output.print("Date: " + new Date() + EOL);
						output.print("Content-Type: text/html" + EOL);
						output.print(EOL);
						output.print("<HTML><HEAD><TITLE>Error 501 Not Implemented</TITLE></HEAD>");
						output.print("<BODY><H2>501 Not Implemented</H2>");
						output.print("<p />" + field + "</BODY></HTML>");
						output.flush();
						s.close();
						return;
					}
				} else {
					if(info[0].matches("Content-Length:")) {
						contentLength = Long.parseLong(info[1]);
					}
				}				
			} while(!endOfHeader);	
			
			if(connectionType==HTTP_GET ) {
				if(file.equals("/"))
					myAgent.addBehaviour(new StatusResponse(myAgent, s));
//				else
//					myAgent.addBehaviour(new GetFileResponse(myAgent, s));
			} else if(connectionType==HTTP_POST) {
//				myAgent.addBehaviour(new UploadFileResponse(myAgent, s, input));
			}
			
			
		} catch(Exception e){
			e.printStackTrace();
		}		
	}
	
	private class StatusResponse extends OneShotBehaviour {
		private static final long serialVersionUID = 1L;
		Socket client;
		
		public StatusResponse(Agent a, Socket s) {
			super(a);
			client = s;
		}

		@Override
		public void action() {
			try {
				PrintStream output = new PrintStream(client.getOutputStream());
				
				//=====
				
				output.print("HTTP/1.1 200 OK");
				output.write(EOL);
				output.print("Content-Type: text/html");
				output.write(EOL);
				output.write(EOL);
				
				output.flush();
				
				// HTML Starter
				output.print("<HTML>");
				
				// Header
				output.print("<HEAD><TITLE>Hybrid Cloud Information Viewer</TITLE>" +
						//"<meta http-equiv=\"refresh\" content=\"5\" />" +
						"</HEAD>");
				
				// Start of Body
				output.print("<BODY>");
				
				// cluster+running list
				output.print("<DIV>");
				output.print("<H1><img src=\"http://dmclab.csie.ntpu.edu.tw/web/media/logo_action.gif\" />Hybrid Cloud Information Viewer</H1>");
				output.print("<h3>Copyright: C.T.Fan</h3>");
				output.print("<h3>Uptime: "+ m_initTime.toString() +"</h3>");
				output.print("</DIV>");
				output.print("<DIV>");
				output.print("<H1>Private Cluster/Job Information</H1>");
				output.print("<TABLE style=\"text-align:center; border-collapse:collapse; border:1px black solid; width:100%\">");
				
				output.print("<THEAD>" +
								"<TR style=\"border-top:1px black solid\"><TH style=\"width:25%\">Cluster Name</TH>" +
																		 "<TH style=\"width:25%\">Remain Time</TH>" +
																		 "<TH style=\"width:25%\">Core</TH>" +
																		 "<TH style=\"width:25%\">Memory</TH></TR>"+
							 "</THEAD>");
				output.print("<TBODY>");
				
				for(ClusterNode cn : policy.GetRunningCluster()) {
					if(cn.vmMaster!=null&&cn.vmMaster.masterType!=VMMasterNode.PRIVATE)
						continue;
					long remainTime=0;
					for(JobNodeBase jn : policy.GetRunningJob()) {
						if(jn.currentPosition!=null&&jn.currentPosition.compare(cn)) {
							if(jn.predictTime-jn.hasBeenExecutedTime<0)
								remainTime=Long.MAX_VALUE;
							else if(remainTime!=Long.MAX_VALUE)
								remainTime+=(jn.predictTime-jn.hasBeenExecutedTime);
						}
					}
					output.print("<TR style=\"border-top:1px solid black\"><TD>" + cn.name + "</TD>" +
								     									  "<TD>" + remainTime + "</TD>" +
								     									  "<TD>" + cn.core+"</TD>" +
								     									  "<TD>" + cn.memory+"</TD></TR>");
					output.print("<tr><td>&nbsp;</td><td colspan=\"3\">");
					
					output.print("<table style=\"text-align:center; border-collapse:collapse; border:1px black solid;width:100%;background-color:#dddddd;\">");
					output.print("<thead>" +
							"<tr style=\"border-top:1px solid black\"><th style=\"width:20%\">Job Name</th>" +
							                                         "<th style=\"width:20%\">Job Type</th>" +
							                                         "<th style=\"width:20%\">Estimated Time</th>" +
							                                         "<th style=\"width:20%\">Running Time</th>" +
							                                         "<th style=\"width:20%\">Deadline</th></tr></thead>");
					for(JobNodeBase jn: policy.GetRunningJob()) {
						if(jn.currentPosition!=null&&jn.currentPosition.compare(cn))
							output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
									                                              "<td>" + jn.jobName + "</td>" +
									                                              "<td>" + jn.predictTime/1000 + "</td>" +
									                                              "<td>" + jn.hasBeenExecutedTime + "</td>" +
									                                              "<td>" + jn.deadline + "</tr>");
					}
					
					output.print("</table>");
					
					output.print("</td></tr>");
				}
				output.print("</TBODY>");
				
				output.print("</TABLE>");
				
				output.print("</DIV>");
				

				output.print("<DIV>");
				output.print("<H1>Public Cluster/Job Information</H1>");
				output.print("<TABLE style=\"text-align:center; border-collapse:collapse; border:1px black solid; width:100%\">");
				
				output.print("<THEAD>" +
								"<TR style=\"border-top:1px black solid\"><TH style=\"width:25%\">Cluster Name</TH>" +
																		 "<TH style=\"width:25%\">Remain Time</TH>" +
																		 "<TH style=\"width:25%\">Core</TH>" +
																		 "<TH style=\"width:25%\">Memory</TH></TR>"+
							 "</THEAD>");
				output.print("<TBODY>");
				
				for(ClusterNode cn : policy.GetRunningCluster()) {
					if(cn.vmMaster!=null&&cn.vmMaster.masterType!=VMMasterNode.PUBLIC)
						continue;
					long remainTime=0;
					for(JobNodeBase jn : policy.GetRunningJob()) {
						if(jn.currentPosition!=null&&jn.currentPosition.compare(cn)) {
							if(jn.predictTime-jn.hasBeenExecutedTime<0)
								remainTime=Long.MAX_VALUE;
							else if(remainTime!=Long.MAX_VALUE)
								remainTime+=(jn.predictTime-jn.hasBeenExecutedTime);
						}
					}
					output.print("<TR style=\"border-top:1px solid black\"><TD>" + cn.name + "</TD>" +
								     									  "<TD>" + remainTime + "</TD>" +
								     									  "<TD>" + cn.core+"</TD>" +
								     									  "<TD>" + cn.memory+"</TD></TR>");
					output.print("<tr><td>&nbsp;</td><td colspan=\"3\">");
					
					output.print("<table style=\"text-align:center; border-collapse:collapse; border:1px black solid;width:100%;background-color:#dddddd;\">");
					output.print("<thead>" +
							"<tr style=\"border-top:1px solid black\"><th style=\"width:20%\">Job Name</th>" +
							                                         "<th style=\"width:20%\">Job Type</th>" +
							                                         "<th style=\"width:20%\">Estimated Time</th>" +
							                                         "<th style=\"width:20%\">Running Time</th>" +
							                                         "<th style=\"width:20%\">Deadline</th></tr></thead>");
					for(JobNodeBase jn: policy.GetRunningJob()) {
						if(jn.currentPosition!=null&&jn.currentPosition.compare(cn))
							output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
									                                              "<td>" + jn.jobName + "</td>" +
									                                              "<td>" + jn.predictTime/1000 + "</td>" +
									                                              "<td>" + jn.hasBeenExecutedTime + "</td>" +
									                                              "<td>" + jn.deadline + "</tr>");
					}
					
					output.print("</table>");
					
					output.print("</td></tr>");
				}
				output.print("</TBODY>");
				
				output.print("</TABLE>");
				
				output.print("</DIV>");
				
				
				// waiting list
				
				output.print("<div>");
				output.print("<h1>Waiting Job List</h1>");
				output.print("<TABLE style=\"text-align:center; border-collapse:collapse; border:1px black solid; width:100%\">");
				
				output.print("<THEAD>" +
								"<TR style=\"border-top:1px black solid\"><TH style=\"width:34%\">Job Name</TH>" +
																	     "<TH style=\"width:33%\">Job Type</TH>" +
																	     "<TH style=\"width:33%\">Deadline</TH>" +
							 "</THEAD>");
				
				for(JobNodeBase jn:policy.GetWaitingJob()) {
					output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
																		  "<td>" + jn.jobName + "</td>" +
																		  "<td>" + jn.deadline + "</td>" +
																		  "</tr>");
				}
				
				output.print("</TBODY>");
				
				output.print("</TABLE>");
				
				output.print("</DIV>");
				
				// finish list			
				
				output.print("<div>");
				output.print("<h1>Finished Job List</h1>");
				output.print("<TABLE style=\"text-align:center; border-collapse:collapse; border:1px black solid; width:100%\">");
				
				output.print("<THEAD>" +
								"<TR style=\"border-top:1px black solid\"><TH style=\"width:15%\">Job Name</TH>" +
																		 "<TH style=\"width:15%\">Job Type</TH>" +
																		 "<TH style=\"width:15%\">Finished Time</TH>" +
																		 "<TH style=\"width:15%\">Differences%</TH>" +
																		 "<TH style=\"width:15%\">Start Time</TH>" +
																		 "<TH style=\"width:15%\">Finish Time</TH></TR>"+
							 "</THEAD>");
				
				for(JobNodeBase jn:policy.GetFinishJob()) {
					output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
																		  "<td>" + jn.jobName + "</td>" +
																		  "<td>" + jn.executeTime + "</td>" +
																		  "<td>" + (((double)jn.predictTime - (double)jn.executeTime)/(double)jn.executeTime) + "</td>" +
																		  "<td>" + jn.startTime+ "</td>" +
																		  "<td>" + jn.finishTime + "</td></tr>");
				}
				
				output.print("</TBODY>");
				
				output.print("</TABLE>");
				
				output.print("</DIV>");
				
				// End of Body
				output.print("</BODY>");
				
				// end of HTML
				output.print("</HTML>");
				
				output.flush();
				
				client.close();
				//=====
				
			} catch(Exception e) {
				System.err.println("Error in tw.idv.ctfan.cloud.middleware.HTTPServerBehaviour.StatusResponse");
				e.printStackTrace();
			}
		}
		
	}
	
}
