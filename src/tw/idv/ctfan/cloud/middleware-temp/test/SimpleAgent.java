package tw.idv.ctfan.cloud.Middleware.test;

import jade.core.AID;
import jade.core.Agent;
import jade.core.ContainerID;
import jade.core.PlatformID;
import jade.core.behaviours.Behaviour;
import jade.core.behaviours.ThreadedBehaviourFactory;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;

public class SimpleAgent extends Agent {

	private static final long serialVersionUID = 1L;
	
	ThreadedBehaviourFactory tbf;
	
	public void setup()
	{
		super.setup();
		
		//ProfileImpl pi = new ProfileImpl(null, -1, null, false);
		//pi.setParameter("container-name", "hdp005");
		//jade.core.Runtime.instance().createAgentContainer(pi);
		
		//this.addBehaviour(new testBehaviour(this));
		
		System.out.println("Address :\t" + this.here().getAddress());
		System.out.println("ID      :\t" + this.here().getID());
		System.out.println("Name    :\t" + this.here().getName());
		System.out.println("Protocol:\t" + this.here().getProtocol());
		String[] a = this.getAMS().getAddressesArray();
		
		System.out.println("AMS address");
		for(int i=0; i<a.length; i++)
		{
			System.out.println("" + i + "\t" + a[i]);
		}
		
		System.out.println("AgentName:\t" + this.getName());
		//System.out.println("Address :\t" + this.getAID().getAddress());
		//System.out.println("ID      :\t" + this.getAID().getID());
		System.out.println("Name    :\t" + this.getAID().getName());
		System.out.println("Hap     :\t" + this.getAID().getHap());
		//System.out.println("Protocol:\t" + this.getAID().getProtocol());
		
		String[] hereArray = this.getAID().getAddressesArray();
		for(int i=0; i<hereArray.length; i++)
			System.out.println(hereArray[i]);
		

		System.out.println("AgentName:\t" + this.getAMS().getName());
		System.out.println("Address :\t" + this.getAMS().getAddressesArray()[0]);
		//System.out.println("ID      :\t" + this.getAMS().getID());
		System.out.println("Name    :\t" + this.getAMS().getName());
		//System.out.println("Protocol:\t" + this.getAID().getProtocol());//*/
		//this.addBehaviour(new receBehaviour(this));
		
		//tbf = new ThreadedBehaviourFactory();		
		//this.addBehaviour(tbf.wrap(new GetMessageBehaviour(this)));
	}
	
	public void beforeMove()
	{
		System.out.println("Before Move");
	}
	public void afterMove()
	{
		System.out.println("After Move");
	}
	
	@SuppressWarnings("unused")
	private class GetMessageBehaviour extends Behaviour
	{
		private static final long serialVersionUID = 1L;

		public GetMessageBehaviour(Agent agent)
		{
			super(agent);
		}

		@Override
		public void action() {
			ACLMessage msg = myAgent.blockingReceive(MessageTemplate.MatchAll());
			
			System.out.println(msg.getContent());
			
		}

		@Override
		public boolean done() {
			// TODO Auto-generated method stub
			return false;
		}
	}
	
	@SuppressWarnings("unused")
	private class receBehaviour extends Behaviour
	{
		private static final long serialVersionUID = 1L;
		int step = 0;

		public receBehaviour(Agent agent)
		{
			super(agent);
		}

		@Override
		public void action() {
			System.out.print(myAgent.getName() + "\tStep in as\t" + step + "\t");
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			switch(step)
			{
			case 0:
			{
				step++;
				System.out.println("I'm going to jump as local");
				ContainerID cid00 = new ContainerID();
				cid00.setName("Main-Container");
				cid00.setAddress(myAgent.getAMS().getAddressesArray()[0]);
				myAgent.doMove(cid00); //*/
				break;
			}
			case 2:
			{
				step++;
				System.out.println("I'm going to jump as remote");
				AID remoteAMM = new AID("amm@120.126.145.117:1099/JADE", AID.ISGUID);
				remoteAMM.addAddresses("http://120.126.145.117:7778/acc");
				PlatformID dest03 = new PlatformID(remoteAMM);
				myAgent.doMove(dest03);
				break;
				
			}
			case 4:
			{
				step++;
				System.out.println("I'm going to jump as remote:");
				AID remoteAMM04 = new AID("amm@120.126.145.114:1099/JADE", AID.ISGUID);
				remoteAMM04.addAddresses("http://120.126.145.114:7778/acc");
				PlatformID dest04 = new PlatformID(remoteAMM04);
				myAgent.doMove(dest04);
			}
			default:
				System.out.println("Default");
				step++;
				break;
			}
			
			
		}

		@Override
		public boolean done() {
			return (step > 10);
		}
		
	}

}
