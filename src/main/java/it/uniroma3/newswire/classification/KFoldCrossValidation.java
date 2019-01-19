package it.uniroma3.newswire.classification;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import it.uniroma3.newswire.spark.SparkLoader;
import it.uniroma3.newswire.utils.EnvironmentVariables;
import it.uniroma3.newswire.utils.URLUtils;
import scala.Tuple2;

public class KFoldCrossValidation {
	private static Logger logger = Logger.getLogger(KFoldCrossValidation.class);
	
	/**
	 * Esegue l'addestramento di un modello basato su Decision Tree usando K-Fold Cross-Validation.
	 * @param websiteRoot è la home del sito che vogliamo studiare.
	 * @param snapshot è lo snapshot fino al quale vogliamo eseguire l'addetsramento.
	 * @param k è il numero di pezzi in cui vogliamo dividere i nostri dati per eseguire il K-Fold.
	 * @param featureCount è il numero di features che vogliamo considerare.
	 * @return FIXME: Delle metriche, in seguito dovrà restituire il modello vero e proprio.
	 * @throws Exception
	 */
	public static Tuple2<MulticlassMetrics, Double> run(String websiteRoot,int snapshot, int k, int featureCount) throws Exception {		
		String csvFolder = System.getenv(EnvironmentVariables.datasets);
		String databaseName = URLUtils.getDatabaseNameOf(websiteRoot);		
		String trainingSetPath = csvFolder + File.separator + databaseName + "_" + snapshot + "_training.csv";
		
		/*
		 * Se esiste un training set pre-calcolato usa quello.
		 */
		List<String> csvLines = null;
		try {
			csvLines = Files.readAllLines(Paths.get(trainingSetPath), StandardCharsets.UTF_8);
			logger.info("CSV already there @ " + snapshot);
		} catch(NoSuchFileException e) {
			/*
			 * Vuol dire che non è stato calcolato alcun traing set.
			 */
			logger.info("Generating CSV @ " + snapshot);
			(new TrainingDataGenerator()).generateTrainingSet(websiteRoot, snapshot);
			csvLines = Files.readAllLines(Paths.get(trainingSetPath), StandardCharsets.UTF_8);
		}
		
		/*
		 * In caso ci sia stato qualche problema nella creazione lancia un'eccezione.
		 */
		if(csvLines == null)
			throw new Exception("Unable to generate CSV");
		
		JavaRDD<LabeledPoint> data = SparkLoader.getInstance()
												.getContext()
												
												/*
												 * Qui splittiamo i valori sulla virgola e poi li mettiamo in un vettore che saraà quello delle features.
												 */
												.parallelize(csvLines.parallelStream().map(x -> {
													String[] commaSplit = x.split(",");
													double[] vec = new double[commaSplit.length - 2];
													
													double label = Double.parseDouble(commaSplit[commaSplit.length-1]);
													/*
													 * Aggiungiamo il numero di features richieste (in maniera ordinata).
													 * FIXME: Da far diventare casuale magari.
													 */
													for(int i = 0; i < featureCount; ++i) {
														vec[i] = Double.parseDouble(commaSplit[i+1]);
													}
													
													return new Tuple2<>(label, Vectors.dense(vec));
												})
														
												/*
												 * Trasformiamo i dati in det @link{LabeledPoint}		
												 */
												.map(x -> new LabeledPoint(x._1, x._2))
												.collect(Collectors.toList()));
		
		/*
		 * Effettuiamo uno shuffle dei dati per essere immuni da qualsivoglia influenza di ordinamento.
		 */
		data = shuffleData(data);
		
		/*
		 * Splittiamo i dati in K gruppi (5 - 10 empiricamente).
		 */
		List<JavaRDD<LabeledPoint>> groups = splitIntoKGroups(k, data);
		
		List<JavaRDD<LabeledPoint>> trainingSet = new ArrayList<>();
		JavaRDD<LabeledPoint> testSet;
		
		Tuple2<MulticlassMetrics, Double> bestModel = null;
		
		/*
		 * A turno ogni gruppo fa da test set e la restante parte dei dati fa da training set.
		 */
		for(int i = 0; i < groups.size(); ++i) {
			testSet = groups.get(i);
			trainingSet.addAll(groups);
			
			/*
			 * Se un test set è troppo piccolo potrebbe non essere significativo. Indicativamente più piccolo dell'80% del primo gruppo.
			 */
			if(testSet.count() < groups.get(0).count() * 0.8)
				continue;
			
			trainingSet.remove(i);
			
			JavaRDD<LabeledPoint> remainingData = SparkLoader.getInstance().getContext().emptyRDD();
			for(JavaRDD<LabeledPoint> groupRDD: trainingSet)
				remainingData = remainingData.union(groupRDD);
			
			/*
			 * Applichiamo del super sampling per migliorare la qualità dei dati e sopperire al problema delle classi sbilanciate.
			 */
			remainingData = OverSampler.applyTo(remainingData);
			
			Tuple2<MulticlassMetrics, Double> evaluation = evaluate(remainingData, testSet);
			double err = evaluation._2;
						
			if (bestModel == null)
				bestModel = evaluation;
			else if (err < bestModel._2) {
				bestModel = evaluation;
			}
			trainingSet.clear();
		}
		
//		MulticlassMetrics metrics = bestModel._1;
//		printMetrics(metrics);
		
		return bestModel;
	}
	
	private static List<JavaRDD<LabeledPoint>> splitIntoKGroups(int k, JavaRDD<LabeledPoint> data) {
		long groupSize = data.count() / k;
		List<JavaRDD<LabeledPoint>> groups = new ArrayList<>();
		
		JavaPairRDD<LabeledPoint, Long> indexedData = data.zipWithIndex().cache();
		
		for (int i = 0; i < data.count(); i+=groupSize) {
			final int idx = i;
			groups.add(indexedData.filter(indexed -> indexed._2 >= idx && indexed._2 < idx + groupSize).map(x -> x._1));
		}
		
		return groups;
	}
	
	@SuppressWarnings("unused")
	private static void printMetrics(MulticlassMetrics metrics) {
		for(int c=0; c < 3; c++) {
			System.out.println("Measures for class: " + c);	
			double precision = metrics.precision(c);
			double recall = metrics.recall(c);
			double f1 = metrics.fMeasure(c);
			System.out.println("p: " + precision + " r: " + recall + " f1: " + f1);
		}
	}
	
	private static Tuple2<MulticlassMetrics, Double> evaluate(JavaRDD<LabeledPoint> trainingSet, JavaRDD<LabeledPoint> testSet) {
		/*
		 * Imposta il numero delle classi in cui vogliamo classificare.
		 */
		int numClasses = 3;
		
		/*
		 * Lasciare vuoto questa mappa se le features sono continue.
		 */
		Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
		String impurity = "gini";
		int maxDepth = 5;
		int maxBins = 32;

		/*
		 * Addetsra un modello basato su Decision Tree.
		 */
		DecisionTreeModel model = DecisionTree.trainClassifier(trainingSet, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins);
		
		/*
		 * Effettuiamo delle predizioni sul test set e calcoliamo tutte le metriche relative.
		 */
		JavaPairRDD<Object, Object> predictionAndLabels = testSet.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
		double testErr = predictionAndLabels.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testSet.count();
		
		MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
		
		return new Tuple2<>(metrics,testErr);
	}
	
	/**
	 * Effettua lo shuffle dei dati per ragioni di non condizionamento dell'addestramento.
	 * @param data sono i dati che si vogliono mischiare.
	 * @return i dati mischiati.
	 */
	public static JavaRDD<LabeledPoint> shuffleData(JavaRDD<LabeledPoint> data){
		long countData = data.count();
		
		/*
		 * Assegnamo un id casuale alle osservazioni e poi le ordiniamo in qualche modo.
		 */
		return data.mapToPair(point -> new Tuple2<>(ThreadLocalRandom.current().nextLong(countData), point))
				   .sortByKey()
				   .map(x -> x._2);
	}
	
}
