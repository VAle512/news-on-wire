package it.uniroma3.newswire.cli;

import static it.uniroma3.newswire.cli.Init.init;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import it.uniroma3.newswire.benchmark.BenchmarkDriver;
import it.uniroma3.newswire.classification.ThreeMeans;
import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;

public class CLI {
	public static void showCLI() throws Exception {
		Scanner scanner = new Scanner(System.in);

		try {
			System.out.println("Welcome " + System.getProperty("user.name") + "!");
			showChoices();

			int choice = scanner.nextInt();
			
			/* Executes the entire benchmark suite for all websites at the latest snapshot. */
			if(choice == 1) {
				(new BenchmarkDriver()).executeAllBenchmarksAllDAOsAtLatestSnapshot();
			}
			
			/* Execute the entire benchmark suite all across snapshots: All DAOs, All benchmaks. */
			if(choice == 2) {
				(new BenchmarkDriver()).executeAllBenchmarksFromTheBeginning();
			}
			
			/* Execute the entire benchmark suite for a specified snapshot */
			if(choice == 3) {
				System.out.println("Please insert the snapshot you want to execute the benchmark suite for:");
				int snapshot = scanner.nextInt();
				(new BenchmarkDriver()).executeAllBenchmarksAllDAOsAt(snapshot, true);
			}
			
			/* Execute the entire benchmark suite for a specified website */
			if(choice == 4) {
				init(false);
				AtomicInteger i = new AtomicInteger(0);

				System.out.println("Please insert the database you want to execute the benchmark suite for:");
				Map<Integer, DAO> choice2dao = DAOPool.getInstance().getDatabasesDAOs().stream().collect(Collectors.toMap(x -> i.incrementAndGet(), x -> x));
				choice2dao.entrySet().forEach(entry -> System.out.println("\t" + entry.getKey() + ". " + entry.getValue().getDatabaseName()));

				System.out.println("Your choice: ");
				int daoChoiceId = scanner.nextInt();
				
				DAO chosenDAO = choice2dao.get(daoChoiceId);
				
				System.out.println("Please insert the snapshot you want to execute the benchmark suite for:");
				int snapshot = scanner.nextInt();
				
				
				(new BenchmarkDriver()).executeAllBenchmarksAt(chosenDAO, snapshot, true);
			}

			if(choice == 5) {
				init(false);

				int fromSnapshot;
				int toSnapshot;
				
				int absoluteLatestSnapshot = DAOPool.getInstance().getAbsoluteMaximumSnapshot();
				
				do {
					System.out.println("Please insert the snapshot you want to start the benchmark:");
					System.out.println("Your choice: ");
					fromSnapshot = scanner.nextInt();
				} while(fromSnapshot < 0);

				do {
					System.out.println("Please insert the snapshot you want to end the benchmark:");
					System.out.println("Your choice: ");
					toSnapshot = scanner.nextInt();
				}while(toSnapshot > absoluteLatestSnapshot);


				(new BenchmarkDriver()).executeAllBenchmarksAllDAOsInRange(fromSnapshot, toSnapshot);
			}
			
			if(choice == 6) {
				init(false);

				AtomicInteger i = new AtomicInteger(0);

				System.out.println("Please insert the database you want to execute the benchmark suite for:");
				Map<Integer, DAO> choice2dao = DAOPool.getInstance().getDatabasesDAOs().stream().collect(Collectors.toMap(x -> i.incrementAndGet(), x -> x));
				choice2dao.entrySet().forEach(entry -> System.out.println("\t" + entry.getKey() + ". " + entry.getValue().getDatabaseName()));

				System.out.println("Your choice: ");
				int daoChoiceId = scanner.nextInt();
				
				DAO chosenDAO = choice2dao.get(daoChoiceId);
				
				System.out.println("Please insert the snapshot you want to execute the benchmark suite for:");
				int snapshot = scanner.nextInt();
				
				ThreeMeans.calculate(chosenDAO.getDatabaseName(), snapshot);
				
			}

		} catch(IllegalStateException | NoSuchElementException e) {
			// System.in has been closed
			e.printStackTrace();
		} finally {
			scanner.close();
		}

	}

private static void showChoices() throws IOException {
	init(false);
	int latestSnapshot = DAOPool.getInstance().getDatabasesDAOs().get(0).getCurrentSequence();

	System.out.println("Press the number corresponding to your choice:");
	System.out.println("\t1. Execute the entire benchmark suite for the latest snapshot: " + latestSnapshot);
	System.out.println("\t2. Execute the entire benchmark suite all across snapshots: [0 - " + latestSnapshot + "]");
	System.out.println("\t3. Execute the entire benchmark suite for a specified snapshot");
	System.out.println("\t4. Execute the entire benchmark suite for a specified website");
	System.out.println("\t5. Execute the entire benchmark suite for a specified snapshots range.");
	System.out.println("\t6. Cluster a specified website.");
}

}
