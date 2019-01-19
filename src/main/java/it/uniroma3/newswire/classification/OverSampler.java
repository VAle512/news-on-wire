package it.uniroma3.newswire.classification;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.spark_project.guava.collect.Iterables;

import com.google.common.collect.Lists;

import it.uniroma3.newswire.spark.SparkLoader;
import scala.Tuple2;

/**
 * Questa classe serve per migliorare quei training set che hanno una distribuzione sbilanciata delle classi.
 * Applica un'oversampling basato sulla sintetizzazione di dati somiglianti a quelli delle minority classes randomizzandone 
 * i valori in pools creati sulla base di quelli degli altri appartenenti alla classe.
 * @author luigi
 *
 */
public class OverSampler {
	/**
	 * Applica l'over-sampling ad un certo set di dati.
	 * @param unbalancedData sono i dati sui quali si vuole applicare l'oversampling.
	 * @return un set di dati arricchito con dati sintetici.
	 */
	public static JavaRDD<LabeledPoint> applyTo(JavaRDD<LabeledPoint> unbalancedData) {		
		/*
		 * Prima di tutto puliamo i dati dalle osservazioni che posseggono features sporche o non correttamente calcolate.
		 */
		unbalancedData = cleanData(unbalancedData).cache();
		
		List<LabeledPoint> newPoints = new ArrayList<>();
		Map<Double, Long> classCount = unbalancedData.map(point -> point.label())
													 .groupBy(point -> point.doubleValue())
													 .mapToPair(x -> new Tuple2<>(x._1, (long)Iterables.size(x._2)))
													 .collectAsMap();
		
		/*
		 * Troviamo la classe di maggioranza.
		 */
		Entry<Double, Long> majorityClass= classCount.entrySet().stream()
																.sorted((e1, e2) -> (-1) * e1.getValue()
																.compareTo(e2.getValue())).findFirst()
																.get();
		
		/*
		 * Per ogni classe (tranne quella di maggioranza) andiamo ad effettuare l'oversampling.
		 */
		for(Entry<Double, Long> entryClass: classCount.entrySet()) {
			if(entryClass.equals(majorityClass))
				continue;
			
			/*
			 * Costruiamo dei bins di valori per ogni features in modo da randomizzare la creazione di nuove istanze.
			 */
			double minClass = entryClass.getKey();
			Map<Integer, ArrayList<Double>> featuresBins = unbalancedData.filter(x -> x.label() == minClass)
																		 .flatMapToPair(point -> {
																				/*
																				 * Per ogni feature assegno un indice per effettuare la group.
																				 */
																				List<Double> vector = Arrays.stream(point.features().toArray())
																											.boxed()
																											.collect(Collectors.toList());
																				
																				Set<Tuple2<Integer, Double>> indexedVector = new HashSet<>();
																				
																				int i = 0;
																				for(Double value: vector)
																					indexedVector.add(new Tuple2<>(i++, value));
																					
																				return indexedVector.iterator();
																		 	})
																		 .groupByKey()
																		 .mapToPair(group -> new Tuple2<>(group._1, Lists.newArrayList(group._2)))
																		 .collectAsMap();
			/*
			 * Troviamo il numero delle osservazioni della classe di minoranza e calcoliamo quante ne vogliamo di sintetiche.
			 * In questo momento verranno pareggiate quelle della classe di maggioranza.
			 * FIXME: Rendere la quantità selezionabile in base ad una percentuale.
			 */
			long minorityClassCounter = entryClass.getValue();
			long nrOfPoints = majorityClass.getValue() - minorityClassCounter;
				
			for(int j = 0; j < nrOfPoints; ++j)
				newPoints.add(generatePoint(entryClass.getKey(), featuresBins));
			
		}
		/*
		 * Effettuiamo anche uno shuffle successivamente alla generazione poichè alternativamente i dati sarebbero stati generati in ordine ed avrebbero potuto creare
		 * delle influenze sugli esisti dell'addestramento.
		 */
		return KFoldCrossValidation.shuffleData(unbalancedData.union(SparkLoader.getInstance().getContext().parallelize(newPoints)));
	}
	
	/**
	 * Genera un {@link LabeledPoint} a partire da una classe e un pool di valori di features.
	 * @param label è la classe dell'osservazione.
	 * @param featuresValuesPool è la struttura che memorizza, per una determinata classe, un set di valori possibili per ogni feature che è possibile selezionare.
	 * @return una nuova osservazione.
	 */
	private static LabeledPoint generatePoint(Double label, Map<Integer, ArrayList<Double>> featuresValuesPool) {
		double[] vector = new double[featuresValuesPool.keySet().size()];
		
		for(Entry<Integer, ArrayList<Double>> bin: featuresValuesPool.entrySet())
			vector[bin.getKey()] = pickOneFrom(bin.getValue());
		
		return new LabeledPoint(label, Vectors.dense(vector));
	}
	
	/**
	 * Serve a pescare un valore casuale da una lista.
	 * @param lst è la lista dalla quale vogliamo pescare il valore.
	 * @return il valore pescato.
	 */
	private static Double pickOneFrom(ArrayList<Double> lst) {
		int randomElementIndex = ThreadLocalRandom.current().nextInt(lst.size());
		return lst.get(randomElementIndex);
	}
	
	/**
	 * Pulisce i dati da quelli non presenti o non ben calcolati.
	 * @param data sono i dati che si vogliono pulire.
	 * @return i dati puliti.
	 */
	private static JavaRDD<LabeledPoint> cleanData(JavaRDD<LabeledPoint> data) {
		return data.filter(point -> !Arrays.stream(point.features().toArray()).boxed().anyMatch(value -> value.doubleValue() == -1.0));
	}
}
