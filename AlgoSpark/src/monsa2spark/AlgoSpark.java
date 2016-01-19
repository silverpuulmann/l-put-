package monsa2spark;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class AlgoSpark {

	static ArrayList<int[]> dataAsList;
	static ArrayList<String> rules = new ArrayList<>();
	
	static int[] sequencesZero;
	static int[] sequencesOne;
	static ArrayList<int[]> sequenceClass;
	static int[] subSequences;
	
	
	
	public static void main(String[] args) throws FileNotFoundException{
		
		
		/*if (args.length != 3) {
		      System.err.println("Midagi on valesti! Sisesta argumendid kujul <fail> <ridasid> <veerge>");
		      System.exit(1);
		    }
		File file = new File(args[0]);*/
		
		//File file = new File(args[0]);
		//int rows = Integer.parseInt(args[1]);
		//int columns = Integer.parseInt(args[2]);
		
		//long time1 = System.nanoTime(); // koos Sparki käimatõmbamisega!
		//Runtime runtime = Runtime.getRuntime(); // koos Sparki käimatõmbamisega!
		
		File file = new File("spect1m.csv");
		int rows = 1028500;
		int columns = 23;
	
		SparkConf sparkConf = new SparkConf().setAppName("MonsaSpark")
											.setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		Runtime runtime = Runtime.getRuntime();
		long time1 = System.nanoTime();
		int data[][] = readCSV(file, rows, columns); 	// CSV faili sisselugemine ja sellest 2d massiivi tegemine.
		dataAsList = dataToList(data); 					// 2d massiivi Arraylistiks teisendamine.
		
		System.out.println("Algab reeglite leidmine!");
		sequencesZero = getSequences(data, 0); // esialgse sagedustabeli loomine
		sequencesOne = getSequences(data, 1); 
		
		JavaRDD<int[]> dataRDD = sc.parallelize(dataAsList); // Muudab listi JavaRDD<int[]> tüüpi massiiviks, mis võimaldab
															 // teostada selle peal operatsioone paralleelselt.
		
		/*
		 * 	Veergude läbi käimine reeglite leidmiseks, mis kasutab Sparki pakutavat paralleelsust.
		 *  Tööpõhimõtet on täpselt seletatud töös peatükis 4.4.2
		 */
		
		for (int i = 0; i < columns; i++) {
			JavaRDD<int[]> thisRDD = getSubClasses(dataRDD, i);
			List<int[]> thisRDDAsList = thisRDD.collect();
			int[] thisRDDSequences = getSequences2(thisRDDAsList, 1);
			getRules(sequencesOne, thisRDDSequences, i, 1, 1);

		}
		 final long MEGABYTE = 1024L * 1024L;
		 long memory = runtime.totalMemory() - runtime.freeMemory();
		    System.out.println("Used memory is megabytes: " + memory / MEGABYTE);
		printRules();
		long time2 = System.nanoTime();
		long timeTaken = time2 - time1;  
		System.out.println("Time taken " + (timeTaken*0.000000001) + " ns");
	}
	
	/*
		  Täiendavalt Sparki jaoks lisatud funktsioon, mida kasutatakse alamhulkade leidmiseks.
		  .filter() funktsioon tagastab ainult need elemendid, mis vastavad if lauses seatud tingimusele.
	 */
	
	private static JavaRDD<int[]> getSubClasses(JavaRDD<int[]> dataRDD, int col) {
		
		JavaRDD<int[]> onesRDD = dataRDD.filter(
				data -> {
					if (data[col] == 1){
						return true;
					}
					return false;
				});
	
		return onesRDD;
	}
	
	/*
		CSV faili sisselugemise funktsioon. Saab sisendiks faili, ridade arvu ja veergude arvu.
		Tagastab 2-dimensioonilise täisarvude massiivi.
	 */
	public static int[][] readCSV(File file, int rows, int columns) throws FileNotFoundException{
		
		int[][] fxRates = new int[rows][columns];
		String delimiter = ";";
		int line = 0;
		
			Scanner sc = new Scanner(file);
		
		    while (sc.hasNextLine()) {
		        String line2 = sc.nextLine();
		        String[] fxRatesAsString = line2.split(delimiter);
		        for (int i = 0; i < fxRatesAsString.length; i++) {
		            fxRates[line][i] = Integer.parseInt(fxRatesAsString[i]);
		        }
		        line++;
		    }
		    sc.close();
		    System.out.println("Fail edukalt sisse loetud!");
			return fxRates;
	}
	
	/*
		Andmete teistendamise funktsioon 2d-massiivist list-massiiv tüüpi muutujasse.
		Tagastab list-massiiv tüüpi muutuja.
	 */
	
	private static ArrayList<int[]> dataToList(int[][] data) {
		
		ArrayList<int[]> list = new ArrayList<int[]>(data.length);
		for(int[] foo : data) {
		    list.add(foo);
		}
		return list;
	}
	
	/*
		 Sageduste leidmise funktsioon.
		 list - sisendiks saadav hulk
		 definer - määrab, millele sagedused leitakse (kas 0 või 1)
		 Tagastab hulga sagedused.
	 */
	private static int[] getSequences(int[][] data, int definer) {
		
		int[] sequences = new int[data[0].length];
		int count = 0;
	
		for (int i = 0; i < data[0].length; i++) {  // i on veeru indeks ehk antud juhul 0-6
			
			for (int j = 0; j < data.length; j++){ // j on rea indeks ehk antud juhul 0-23
				
				if(data[j][i] == definer){ // j on rea indeks ja i on veeru indeks
				count++; // leiti yks, yhtede arvu suurendatakse
				}
			}
			sequences[i] = count;
			count = 0;
		}
		System.out.println("Leitud esialgsed sagedused " + definer + "-de kohta: " + Arrays.toString(sequences));
		return sequences;
	}
	
	/*
		Reeglite leidmise funktsioon.
		sequences2 - esialgsed sagedused
		subSequences2 - alamhulga sagedused
		classVariable - klassitunnuse veeru number, mille kohta tunnuseid leitakse
		classValue - klassitunnuse väärtus (1 või 0)
		classChar - võrreldava veeru väärtus (1 või 0)
	 */
	
	private static void getRules(int[] sequences2, int[] subSequences2, int classVariable, int classValue, int classChar) {
		
		for (int i = 0; i < sequences2.length; i++){
		//System.out.println("i v22rtus on : " + i);
			if (sequences2[i] == subSequences2[i] && i != classVariable && classValue == classChar){
				System.out.println("Leidsin reegli!" + i + ", " + classVariable);
				rules.add("Klassiveeruks on " + (classVariable + 1) + ". veerg. Kui veerg " + (i+1) 
						+ " on " + classChar + " on klassiveerg (" + (classVariable + 1) + ". veerg) " + classValue + " !");
			}
			if (subSequences2[i] == 0 && classValue != classChar){
				System.out.println("Leidsin reegli!" + i + ", " + classVariable);
				rules.add("Klassiveeruks on " + (classVariable + 1) + ". veerg. Kui veerg " + (i+1) 
						+ " on " + classChar + " on klassiveerg (" + (classVariable + 1) + ". veerg) " + classValue + " !");
			}
			//else{System.out.println("Ei leidnud reeglit!");}
			
		}
	}

	/*
		Reeglite väljatrükkimise funktsioon. Trükib välja kõik 'rules' listis olevad reeglid.
	 */
	private static void printRules() {
	System.out.println("Andmestikus olevad reeglip2rasused: ");
		for (String rule : rules){
		System.out.println(rule);
		}
	}
	/*
	 	Alamhulgast sageduste leidmise funktsioon
	 	Saab sisendiks alamhulga andmestiku ja väärtuse millele sagedusi leida (0 või 1).
	 	Tagastab alamhulga elementide esinemise sagedused.
	 */
	
	private static int[] getSequences2(List<int[]> list, int definer) {
		
		int rowLength = list.get(0).length; //6
		int colLength = list.size(); //23
		int[] countOfVar = new int[rowLength];
		int count = 0;
		
		for (int col = 0; col < rowLength; col++){
		
			for (int row = 0; row < colLength; row++){
				
				int[] curRow = list.get(row);
				
				if (curRow[col] == definer){
					count++;		
				}
			}
			countOfVar[col] = count;
			count = 0;
		}
		
		System.out.println("Alamhulga sagedused loetud! " + Arrays.toString(countOfVar));
		return countOfVar;
		}
	}
