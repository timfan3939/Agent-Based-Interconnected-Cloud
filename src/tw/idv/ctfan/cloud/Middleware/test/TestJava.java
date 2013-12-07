package tw.idv.ctfan.cloud.middleware.test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.net.Socket;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.Java.JavaJobType;

public class TestJava {
	public static void main(String[] args) {
		try {
		
			//String m_URL = "10.133.200.1";
			//String m_URL = "120.126.145.117";
			//String m_URL = "120.126.145.114";
//			String m_URL = "10.133.70.64";
//			String m_URL = "120.126.145.3";
			String m_URL = "120.126.145.102";
			String m_port = "50031";
			String m_pathToFile = "C:\\ctfan\\ComputePi.jar";
			

			int size = 30;
			int testSize[] = new int[size];
			java.util.Random rand = new java.util.Random();
			for(int i=0; i<size; i++)
				testSize[i] = rand.nextInt(10000/*0*/)+50000;
			JobType java = new JavaJobType();
			
//			int testSize[] = {113065, 75788, 109289, 149627, 78317, 72055, 53281, 92010, 51899, 87078, 
//					122866, 77011, 125768, 63909, 147655, 57122, 98212, 130441, 89968, 77316, 
//					102966, 62502, 126604, 83510, 86334, 63559, 118450, 85127, 53982, 75428, 
//					146642, 118772, 97273, 105650, 94820, 60012, 74759, 109042, 116271, 104306, 
//					54610, 109929, 94103, 131169, 100816, 118383, 82436, 120714, 61309, 61984, 
//					89772, 74008, 84019, 83793, 121404, 59765, 108105, 139485, 94344, 126693, 
//					93638, 108861, 119532, 117552, 113089, 60917, 55550, 119038, 69015, 77547, 
//					65043, 51482, 146247, 72883, 120970, 86649, 91853, 93510, 56057, 142420, 
//					63033, 70385, 122610, 98960, 89931, 121578, 140497, 71327, 117102, 86365, 
//					149509, 96767, 124686, 63044, 99832, 114433, 137271, 88373, 56157, 141380, 
//					97868, 80277, 62165, 90432, 63328, 50250, 149406, 138622, 134836, 113711, 
//					95353, 90852, 69678, 147680, 55126, 61295, 58994, 76085, 125406, 86713, 
//					65663, 107532, 69853, 77914, 87881, 93727, 95914, 78122, 75599, 89940, 
//					140756, 59492, 105657, 100915, 52651, 103943, 60719, 88402, 140392, 92470, 
//					63800, 70252, 144153, 92543, 102830, 52753, 73773, 105369, 131322, 88887, 
//					107800, 92486, 142386, 123694, 71780, 96792, 141132, 101393, 109653, 91984, 
//					53981, 74039, 119543, 85761, 146738, 130057, 112390, 84787, 63562, 71631, 
//					121050, 141188, 133838, 84964, 74221, 80393, 121390, 104968, 142026, 64114, 
//					141562, 125427, 62227, 93771, 78649, 107974, 126031, 149566, 120650, 97758, 
//					57198, 113750, 79348, 75490, 67579, 104223, 54060, 119196, 143069, 129065, 
//					75907, 99301, 147817, 106955, 146761, 135096, 76776, 118341, 73218, 112968, 
//					114241, 77405, 107907, 119565, 140610, 64811, 71242, 58874, 72048, 109552, 
//					146775, 57162, 130537, 88033, 73802, 97849, 80050, 116450, 142266, 51279, 
//					103394, 110253, 123535, 55479, 94570, 135857, 112477, 62926, 136799, 84343, 
//					90942, 66006, 64326, 97152, 130347, 72906, 108314, 71275, 124535, 120268, 
//					71613, 58931, 96168, 136186, 90745, 64148, 60306, 52704, 69874, 61422, 
//					78353, 102374, 63754, 52514, 58306, 80170, 83632, 83801, 62111, 144617, 
//					122502, 78628, 73750, 142198, 107533, 142942, 88998, 81549, 127375, 108014, 
//					72277, 137441, 98281, 86345, 69616, 127217, 100147, 83092, 52447, 92474, 
//					89069, 53095, 119556, 54502, 144360, 84716, 60208, 98434, 110927, 128551, 
//					94947, 111117, 104752, 72016, 115230, 113900, 53418, 107931, 117062, 50330, 
//					61428, 141273, 124600, 112989, 120808, 108567, 128099, 133076, 86155, 55364, 
//					144806, 138284, 74233, 79377, 96775, 146738, 96945, 91824, 133645, 102050, 
//					114011, 120127, 148187, 113854, 142610, 148702, 98954, 137307, 50034, 98939, 
//					106594, 52725, 144158, 119183, 123572, 96867, 85902, 74402, 120694, 106405, 
//					96852, 120521, 92924, 66746, 50378, 82320, 119951, 77486, 119001, 132530, 
//					88845, 103536, 81416, 71781, 111230, 71393, 100703, 138163, 140110, 86344, 
//					93879, 130562, 56171, 94595, 149313, 101173, 123965, 59960, 129551, 59948, 
//					149916, 126474, 135427, 119591, 91987, 77351, 140626, 117960, 112624, 148798, 
//					93332, 68725, 79140, 77294, 57706, 145949, 133821, 97369, 63533, 74226, 
//					129469, 90903, 98383, 98772, 67735, 105351, 109776, 137423, 119046, 69089, 
//					71328, 93274, 112554, 93574, 53643, 78145, 130656, 51481, 136166, 148317, 
//					81020, 92679, 127445, 114046, 108902, 140417, 89846, 105930, 78734, 143978, 
//					116205, 110560, 137869, 82892, 93549, 128661, 136282, 102149, 123744, 121952, 
//					138757, 110943, 69971, 125169, 76051, 91646, 75105, 120475, 58071, 108370, 
//					121115, 109267, 50697, 70252, 54737, 125072, 133830, 120621, 149151, 135506, 
//					64548, 55673, 73046, 63328, 75856, 99467, 51429, 132490, 137766, 112322, 
//					89829, 59276, 143852, 119353, 111006, 143763, 115916, 52149, 59321, 145370, 
//					109644, 52695, 141283, 148942, 101227, 91281, 97147, 62544, 142586, 78293, 
//					65625, 55096, 140372, 142185, 85210, 126615, 128316, 75831, 110060, 109173, 
//					58930, 119777, 129266, 70104, 59729, 82289, 124820, 82406, 86792, 104399, 
//					146503, 147458, 131570, 135468, 125972, 138226, 66549, 102778, 75144, 111585, 
//					134548, 117566, 86936, 54913, 65137, 143518, 69134, 149608, 144259, 144277, 
//					54036, 123259, 110863, 55174, 57180, 79784, 55270, 88685, 102185, 142477, 
//					138203, 74320, 94110, 52982, 111295, 141140, 57441, 135427, 122683, 83500, 
//					81068, 55592, 62569, 53389, 78704, 61135, 120720, 97917, 59269, 66424, 
//					70778, 59273, 130035, 121568, 100851, 106293, 71766, 145791, 94080, 102159, 
//					54706, 91211, 98554, 89789, 143187, 131963, 106694, 53428, 56927, 126822, 
//					120978, 140234, 114266, 127369, 119022, 145603, 106037, 148007, 109508, 69096, 
//					81653, 126058, 105685, 108663, 118971, 114856, 135122, 106771, 141589, 149730, 
//					99923, 109147, 76481, 123526, 136504, 65208, 74553, 75732, 68634, 80048, 
//					69280, 99357, 72237, 140793, 95943, 126503, 123317, 127399, 68956, 54029, 
//					122273, 74565, 55535, 103907, 61608, 136450, 104738, 97144, 67295, 97859, 
//					112685, 95668, 109012, 93943, 132460, 149792, 123306, 127272, 114917, 134501, 
//					91185, 109004, 147801, 142061, 114753, 87163, 129447, 117766, 50835, 82523, 
//					86359, 74885, 74140, 91448, 144778, 82599, 85255, 100751, 56187, 111826, 
//					124910, 81129, 146482, 72986, 120149, 88133, 113413, 147615, 88867, 118062, 
//					114603, 120308, 122493, 70956, 70399, 130554, 100513, 54294, 60397, 144811, 
//					63324, 114981, 117532, 144115, 74376, 130691, 141080, 130745, 133414, 59682, 
//					52859, 140987, 98003, 78794, 128471, 143799, 131722, 63306, 135842, 121318, 
//					64311, 124880, 140607, 135394, 88851, 135027, 90533, 87433, 141409, 54573, 
//					109294, 106650, 115299, 136679, 148542, 57820, 117114, 103244, 144142, 81821, 
//					124248, 77951, 54772, 123256, 93187, 105097, 89457, 58416, 78441, 130858, 
//					92150, 61913, 149083, 71057, 126804, 135158, 77543, 141511, 133951, 78588, 
//					64822, 100645, 75038, 134966, 110974, 63909, 134417, 85218, 120357, 123798, 
//					90015, 116632, 94387, 124906, 54332, 78507, 60466, 56010, 130259, 146312, 
//					118437, 137897, 109270, 129243, 82786, 64309, 104895, 80050, 67469, 88138, 
//					99393, 50809, 109075, 93920, 78224, 99619, 53536, 113541, 120682, 127018, 
//					122397, 137318, 131104, 105646, 145769, 95174, 84869, 144761, 56627, 113777, 
//					95474, 122564, 105649, 66474, 95924, 83666, 141569, 107969, 59795, 58522, 
//					149369, 84064, 70495, 70722, 128268, 111428, 104281, 65926, 112252, 126281, 
//					87963, 106808, 57335, 117727, 91397, 142338, 69360, 88056, 113189, 89223, 
//					130426, 69763, 65154, 64135, 128931, 114451, 78213, 76385, 53707, 135160, 
//					114821, 62451, 97599, 73775, 97070, 106909, 104278, 134709, 90157, 110947, 
//					149224, 55063, 143258, 75665, 120710, 107621, 144417, 87353, 122138, 73028, 
//					105103, 62327, 109756, 111203, 95543, 68918, 85485, 79686, 56542, 136492, 
//					59860, 91390, 124969, 119136, 78675, 148825, 140416, 97004, 66245, 78933, 
//					94495, 128029, 62070, 131440, 134265, 86263, 116127, 126364, 148170, 67190, 
//					80956, 117774, 91319, 95119, 145196, 109909, 110436, 73941, 88325, 91271, 
//					103698, 83959, 139697, 70903, 134578, 110374, 145961, 74787, 86123, 56085, 
//					88300, 60298, 58532, 106713, 120859, 113467, 126734, 112912, 59285, 106632, 
//					84561, 118302, 100618, 67738, 73618, 90578, 120958, 123934, 94429, 65127, 
//					75357, 62332, 66037, 57574, 101429, 119688, 141205, 142441, 103828, 126161, 
//					53602, 53690, 123711, 83767, 117775, 148058, 55563, 62523, 53932, 122846, 
//					126524, 62862, 126893, 136660, 132472, 86315, 80434, 103555, 91399, 86720, 
//					83960, 84592, 68842, 87975, 105819, 83293, 137717, 124948, 125766, 110700, 
//					90666, 85543, 125945, 134017, 135506, 52737, 89942, 57221, 101275, 132435, 
//					98312, 60743, 129961, 106385, 108048, 121789, 96282, 132102, 113677, 69384, 
//					132091, 72737, 143879, 62197, 50944, 133997, 146648, 59424, 142596, 103935, 
//					119704, 76755, 100541, 65224, 56388, 102860, 147162, 60683, 62730, 73341	
//			};
			
			for(int i=0; i<testSize.length; i++)
			{
				FileInputStream fin = new FileInputStream(m_pathToFile);
				ByteArrayOutputStream binary = new ByteArrayOutputStream();

				byte[] buff = new byte[102400];
				int len = 0;
				long counter = 0;
				while( (len = fin.read(buff)) > 0)
				{
					counter += len;
					binary.write(buff, 0, len);
					//System.out.println("Current:\t" + counter/1024);
				}
				fin.close();		
				
				Socket s = new Socket(m_URL, Integer.parseInt(m_port));
				
				DataOutputStream stream = new DataOutputStream(s.getOutputStream());
				
				stream.write("JobType:".getBytes());
				stream.write(java.getTypeName().getBytes());
				stream.write("\n".getBytes());
												
				stream.write("Command:".getBytes());
				stream.write(Integer.toString(testSize[i]).getBytes());
				stream.write("\n".getBytes());
				
				stream.write("Name:".getBytes());
				stream.write("ComputePi".getBytes());
				stream.write("\n".getBytes());
				
				//if(rand.nextInt(2)==0)
				if(i%2==0 && i>5) {
					stream.write("Deadline:".getBytes());
					stream.write(Integer.toString(12000000).getBytes());
					stream.write("\n".getBytes());
				}
				else if(i>20) {
					stream.write("Deadline:".getBytes());
					stream.write(Integer.toString(120000).getBytes());
					stream.write("\n".getBytes());
				}
				else {
//					stream.write("Deadline:".getBytes());
//					stream.write(Integer.toString(100000).getBytes());
//					stream.write("\n".getBytes());
				}
				
				stream.write("BinaryDataLength:".getBytes());
				stream.write(Integer.toString(binary.size()).getBytes());
				stream.write("\n".getBytes());
				
				stream.write(binary.toByteArray());				
				
				s.close();
				
				//System.out.println("" + i + "\tdone");
				
//				if(i>6)
					Thread.sleep(5000);
			}
			
			
		} catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
