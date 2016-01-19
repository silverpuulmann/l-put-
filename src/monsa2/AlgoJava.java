package monsa2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
	
public class AlgoJava {
	
	static int[] sequencesZero;
	static int[] sequencesOne;
	
	static int[] subSequences;
	static ArrayList<int[]> dataAsList;
	static ArrayList<int[]> sequenceClass;
	private static Scanner sc;
	
	static ArrayList<String> rules = new ArrayList<>();
	
	public static void main(String[] args) throws IOException {
		
		long time1 = System.nanoTime();
		Runtime runtime = Runtime.getRuntime();
		runtime.gc();
		File file = new File("spect1m.csv"); //p2rast kasurealt esimese argumendina
		int data[][] = readCSV(file); // csv faili sisselugemine ja sellest 2d massiivi tegemine
		dataAsList = dataToList(data); // teeb tehtud 2d massiivi arraylistiks
		printData(dataAsList); //prindib sisseloetud csv v2lja massiivina
		
		System.out.println("Algab reeglite leidmine!");
		sequencesZero = getSequences2(dataAsList, 0); // esialgse sagedustabeli loomine
		sequencesOne = getSequences2(dataAsList, 1); 
	
		System.out.println("Nullide arv: " + Arrays.toString(sequencesZero));
		System.out.println("Yhtede arv: " + Arrays.toString(sequencesOne));
		monoSys(data);
		//printData(sequenceClass);
		//getRules(sequences, subSequences);
		 final long MEGABYTE = 1024L * 1024L;
		 long memory = runtime.totalMemory() - runtime.freeMemory();
		
		 printRules();
		long time2 = System.nanoTime();
		long timeTaken = time2 - time1;
		System.out.println("Used memory is megabytes: " + memory / MEGABYTE);
		System.out.println("Time taken " + (timeTaken*0.000000001) + " ns");
		//printData(data);
	}
	
	private static void getRules(int[] sequences2, int[] subSequences2, int classVariable, int classValue, int classChar) {
			
		for (int i = 0; i < sequences2.length; i++){
		//System.out.println("i v22rtus on : " + i);
			if (sequences2[i] == subSequences2[i] && i != classVariable && classValue == classChar){ // kui klassiv22rtus ja tunnuse v22rtus on sama, siis
																									 // sagedustabelid kattuvad.
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
	
	private static void printRules(){
		System.out.println("Andmestikus olevad reeglip2rasused: ");
		for (String rule : rules){
			System.out.println(rule);
		}
	}

	private static ArrayList<int[]> dataToList(int[][] data) {
		
		ArrayList<int[]> list = new ArrayList<int[]>(data.length);
		for(int[] foo : data) {
		    list.add(foo);
		}
		return list;
	}

	private static ArrayList<int[]> getByClass(int classDefiner, ArrayList<int[]> dataAsList2, int columnVariable) {
		
		//int columnVariable = 0;
		
		ArrayList<int[]> newList = new ArrayList<int[]>();
		
		/*for (int[] foo : list){
		System.out.println("list on siin: " + Arrays.toString(foo));
		}*/ //kontrollimiseks kas prindib oigesti valja koik
		
		for (int[] foo : dataAsList2){
			
			if (foo[columnVariable] == classDefiner) {
				newList.add(foo);
			}
		}
		
		for (int[] foo : newList) {
			System.out.println("list on siin: " + Arrays.toString(foo));
			}
		
		System.out.println("Alamhulk leitud!");
		
		return newList;
	}

	private static int[] getSequences2(ArrayList<int[]> list, int definer){  //leiab alamhulga sagedused
		
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
	
	private static int[] getSequences(int[][] data, int definer) {
		
		int[] sequences = new int[data[0].length];
		int count = 0;
		//System.out.println("data 0: " + data[0].length + " ;data length: " + data.length);
		for (int i = 0; i < data[0].length; i++) {  // i on veeru indeks ehk antud juhul 0-6
			//System.out.println("data[0] length " + i);
			for (int j = 0; j < data.length; j++){ // j on rea indeks ehk antud juhul 0-23
				
				if(data[j][i] == definer){ // j on rea indeks ja i on veeru indeks
				count++; // leiti yks, yhtede arvu suurendatakse
				}
				//System.out.println(count);
				
			}
			//System.out.println("Sagedused on " + count);
			sequences[i] = count;
			count = 0;
		}
		System.out.println("Esialgsed sagedused leitud! " + Arrays.toString(sequences));
		return sequences;
	}

	public static int[][] readCSV(File file) throws FileNotFoundException{
		
		int[][] fxRates = new int[1028500][23];
		String delimiter = ";";
		int line = 0;
		
			sc = new Scanner(file);
		
		    while (sc.hasNextLine()) {
		        String line2 = sc.nextLine();
		        String[] fxRatesAsString = line2.split(delimiter);
		        for (int i = 0; i < fxRatesAsString.length; i++) {
		            fxRates[line][i] = Integer.parseInt(fxRatesAsString[i]);
		        }
		        line++;
		    }
		    //System.out.println(Arrays.deepToString(fxRates));
		    System.out.println("Fail edukalt sisse loetud!");
			return fxRates;
	}
	
	public static void printData(ArrayList<int[]> data) {
				
		for (int[] foo : data) {
			System.out.println("Prindin v2lja listi" + Arrays.toString(foo));
			}
		System.out.println("Listi l6pp!");
	}
	
	public static void monoSys(int[][] data){
		
		for (int i = 0; i < data[0].length; i++){
			System.out.println("Leian reeglid kui klassiveerg on " + (i + 1) + " v22rtusega 1 ja tunnuse veerg on v22rtusega 1.");
			sequenceClass = getByClass(1, dataAsList, i); //klassiveeru j2rgi andmete jagamine, leitakse need kus klassiveerg on 1
			subSequences = getSequences2(sequenceClass, 1); // eelmise rea p6hjal sagedustabelite leidmine
			//getRules(sequencesZero, subSequences, i, 0, 0); //reeglite leidmine kui klassiveerg on 0 ja reegli tekkimise veerg on 0
			getRules(sequencesOne, subSequences, i, 1, 1); //reeglite leidmine kui klassiveerg on 1 ja reegli tekkimise veerg on 1
			
		}
		
		for (int i = 0; i < data[0].length; i++){
			System.out.println("Leian reeglid kui klassiveerg on " + (i + 1) + " v22rtusega 0 ja tunnuse veerg on v22rtusega 0.");
			sequenceClass = getByClass(0, dataAsList, i); //klassiveergude l2bi itereerimine kui klassiveeru v22rtuseks on 0.
			subSequences = getSequences2(sequenceClass, 0); // sellisel juhul sagedustabelite leidmine
			//getRules(sequencesZero, subSequences, i, 0, 0); //reeglite leidmine kui klassiveerg on 0 ja reegli tekkimise veerg on 0
			getRules(sequencesZero, subSequences, i, 0, 0); //reeglite leidmine kui klassiveerg on 0 ja reegli tekkimise veerg on 0
			
		}
		
		for (int i = 0; i < data[0].length; i++){
			System.out.println("Leian reeglid kui klassiveerg on " + (i + 1) + " v22rtusega 0 ja tunnuse veerg on v22rtusega 1.");
			sequenceClass = getByClass(0, dataAsList, i); //klassiveergude l2bi itereerimine kui klassiveeru v22rtuseks on 0.
			subSequences = getSequences2(sequenceClass, 0); // sellisel juhul sagedustabelite leidmine
			//getRules(sequencesZero, subSequences, i, 0, 0); //reeglite leidmine kui klassiveerg on 0 ja reegli tekkimise veerg on 0
			getRules(sequencesOne, subSequences, i, 1, 0); //reeglite leidmine kui klassiveerg on 1 ja reegli tekkimise veerg on 0
			
		}
		
		for (int i = 0; i < data[0].length; i++){
			System.out.println("Leian reeglid kui klassiveerg on  " + (i + 1) + " v22rtusega 1 ja tunnuse veerg on v22rtusega 0.");
			sequenceClass = getByClass(1, dataAsList, i); //klassiveergude l2bi itereerimine kui klassiveeru v22rtuseks on 0.
			subSequences = getSequences2(sequenceClass, 1); // sellisel juhul sagedustabelite leidmine
			//getRules(sequencesOne, subSequences, i, 0, 1); //reeglite leidmine kui klassiveerg on 1 ja reegli tekkimise veerg on 0
			getRules(sequencesZero, subSequences, i, 0, 1); //reeglite leidmine kui klassiveerg on 1 ja reegli tekkimise veerg on 1
			
		}
	}
}
