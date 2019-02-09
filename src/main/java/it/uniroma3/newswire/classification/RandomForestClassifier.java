package it.uniroma3.newswire.classification;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
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
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import it.uniroma3.newswire.spark.SparkLoader;
import it.uniroma3.newswire.utils.EnvironmentVariables;
import it.uniroma3.newswire.utils.URLUtils;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Classificatore che addestra un modello basato su Random Forest
 * @author luigi
 *
 */
public class RandomForestClassifier {
	private static Logger logger = Logger.getLogger(RandomForestClassifier.class);

	/**
	 * Esegue l'addestramento di un modello basato su Decision Tree.
	 * @param websiteRoot è la home del sito che vogliamo studiare.
	 * @param snapshot è lo snapshot fino al quale vogliamo eseguire l'addetsramento.
	 * @param featureCount è il numero di features che vogliamo considerare.
	 * @return il modello con alcune metriche.
	 * @throws Exception
	 */
	public static Tuple3<RandomForestModel, MulticlassMetrics, Double> train(String websiteRoot,int snapshot, int featureCount) throws Exception {
		String csvFolder = System.getenv(EnvironmentVariables.datasets);
		String databaseName = URLUtils.getDatabaseNameOf(websiteRoot);		
		String trainingSetPath = csvFolder + File.separator + databaseName + "_" + snapshot + "_training.csv";
		
		/*
		 * Se esiste un training set pre-calcolato usa quello.
		 */
		List<String> csvLines = null;
		try {
			csvLines = Files.readAllLines(Paths.get(trainingSetPath), StandardCharsets.UTF_8);
			logger.info("Read lines: " + csvLines.size());
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
													int j = 0;
													
													if(featureCount == 1)
														j = 1;
													if(featureCount == 2)
														j = 2;

														vec[0] = Double.parseDouble(commaSplit[2]);
														vec[1] = Double.parseDouble(commaSplit[1]);
//													}
													
													/* XXX: Delete this shit for developing only
													 * 
													 */
													if (label == 2)
														label = 1;
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
		data = OverSampler.applyTo(data);
		data = shuffleData(data);
		
		
		/*
		 * Splittiamo i dati in training e test set.
		 */
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
		JavaRDD<LabeledPoint> trainingSet = splits[0];
		JavaRDD<LabeledPoint> testSet = splits[1];
		
		// Train a RandomForest model.
		// Empty categoricalFeaturesInfo indicates all features are continuous.
		Integer numClasses = 2;
		Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
		Integer numTrees = 5; // Use more in practice.
		String featureSubsetStrategy = "auto"; // Let the algorithm choose.
		String impurity = "gini";
		Integer maxDepth = 5;
		Integer maxBins = 32;
		Integer seed = 12345;

		RandomForestModel model = RandomForest.trainClassifier(trainingSet, numClasses,
		  categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
		  seed);
		
		/*
		 * Effettuiamo delle predizioni sul test set e calcoliamo tutte le metriche relative.
		 */
		JavaPairRDD<Object, Object> predictionAndLabels = testSet.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
		double testErr = predictionAndLabels.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testSet.count();
		
//		predictionAndLabels.foreach(x -> System.out.println(x));
		MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
//		printMetrics(metrics);
//		System.out.println(testErr);
		
		return new Tuple3<>(model,metrics,testErr);

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
	
	@SuppressWarnings("unused")
	public static void printMetrics(MulticlassMetrics metrics) {
		for(int c=0; c < 2; c++) {
			System.out.println("Measures for class: " + c);	
			double precision = metrics.precision(c);
			double recall = metrics.recall(c);
			double f1 = metrics.fMeasure(c);
			System.out.println("p: " + precision + " r: " + recall + " f1: " + f1);
		}
	}
	
}
