package BigData.RankedRetrieval;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.json.JSONArray;

public class Retrieve {

	/*
	 * args[0] = input file
	 * args[1] = reset?
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		long tim = System.currentTimeMillis();
		//set of all urls; can be used for the corpus size
		Set<String> urls = new HashSet<>();

		String ROOT = args[0].split("\\.")[0];

		Options options = new Options();
		options.createIfMissing(true);

		//if reset argument is populated then delete the levelDB and start over
		if(args.length == 2) {
			factory.destroy(new File(ROOT + "_tuple"), options);
			factory.destroy(new File(ROOT + "_accum"), options);
			factory.destroy(new File(ROOT + "_stats"), options);
		}
		DB tuple = null;
		DB accum = null;
		DB stats = null;
		//create DBs
		try {
			tuple = factory.open(new File(ROOT + "_tuple"), options);
			accum = factory.open(new File(ROOT + "_accum"), options);
			stats = factory.open(new File(ROOT + "_stats"), options);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {

			if(args.length == 2) {
				String line = "";

				//populate the tuple DB
				while ((line = br.readLine()) != null) {
					String[] vars = line.split("\\t");
					tuple.put(bytes(vars[0]), bytes(vars[1]));

					//create the url set for corpus size
					JSONArray post = new JSONArray(vars[1]);
					for(int i = 2; i < post.length(); i++) {
						JSONArray url = post.getJSONArray(i);
						urls.add(url.getString(0));
					}
				}

				stats.put(bytes("corpus"), bytes(String.valueOf(urls.size())));


				//loop over the posting list to create accumulators
				DBIterator iterator = tuple.iterator();
				try {
					for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
						String key = asString(iterator.peekNext().getKey());
						String value = asString(iterator.peekNext().getValue());

						//Post = [<df, tf, [url, count], [url, count]...]
						JSONArray post = new JSONArray(value);
						for(int i = 2; i < post.length(); i++) {
							//URL = [url, count]
							JSONArray urlArr = post.getJSONArray(i);
							String url = urlArr.getString(0);

							Double tfidf = (1 + Math.log10(urlArr.getDouble(1))) * (Math.log10(urls.size() / post.getDouble(0)));

							Double tot = 0.0;

							//if the accum DB already has a given url then update its value
							if(accum.get(bytes(url)) != null) {
								tot = Double.valueOf(asString(accum.get(bytes(url))));
								//square the value
								tot = tot * tot;
								//add the new tfidf
								tot += tfidf * tfidf;
								//square root the value so it is ready to use
								tot = Math.sqrt(tot);
							}
							//if the accum DB does not already have the url then add it with the tfidf
							else {
								tot = tfidf;
							}

							accum.put(bytes(url), bytes(String.valueOf(tot)));
						}
					}
				}
				finally {
					// Make sure you close the iterator to avoid resource leaks.
					iterator.close();
				}
			}
			System.out.println("processing time: " + ((System.currentTimeMillis() - tim) / 1000 / 60));
			Integer size = Integer.valueOf(asString(stats.get(bytes("corpus"))));
			Scanner myObj = new Scanner(System.in);  // Create a Scanner object

			System.out.println("Enter term");

			while(myObj.hasNext()) {

				String search = myObj.nextLine();  // Read user input
				if(search.equals("exit")) {
					break;
				}

				Map<String, Double> num = new HashMap<>();
				Map<Results, Double> css = new TreeMap<>();

				Map<String, Integer> termCount = new HashMap<>();

				//print the posting list of the queried terms
				for(String st : search.toLowerCase(Locale.US).split("\\s+")) {
					System.out.println(st);
					if(termCount.containsKey(st)) {
						termCount.put(st, termCount.get(st) + 1);
					}
					else {
						termCount.put(st, 1);
					}
				}

				//loop over the terms in the query
				for(String term : termCount.keySet()) {
					//ensure that the term was in the posting list
					if(tuple.get(bytes(term)) != null) {
						//Post = [<df, tf, [url, count], [url, count]...]
						JSONArray post = new JSONArray(asString(tuple.get(bytes(term))));
						Double secondPart = Math.log10((size + 1) / (post.getDouble(0) + 1));

						Double q_tfidf = (1 + Math.log10(termCount.get(term))) * secondPart;

						//loop over the urls that contain the given term
						for(int i = 2; i < post.length(); i++) {
							//URL = [url, count]
							JSONArray urlArr = post.getJSONArray(i);
							String url = urlArr.getString(0);


							//calculate tf-idf scores for the query and the document
							//use the term frequency in the query
							//increment the corpus size and document frequency by 1
							Double d_tfidf = (1 + Math.log10(urlArr.getDouble(1))) * secondPart;

							//if the document is already in the numerator map then add the new product
							if(num.containsKey(url)) {
								num.put(url, num.get(url) + (q_tfidf * d_tfidf));
							}
							else {
								num.put(url, q_tfidf * d_tfidf);
							}
						}
					}
				}

				//calculate the cosine similarity score
				for(String url : num.keySet()) {
					css.put(new Results(url, num.get(url) / Double.valueOf(asString(accum.get(bytes(url))))), num.get(url) / Double.valueOf(asString(accum.get(bytes(url)))));
				}


				System.out.println("");
				if(css.size() == 0) {
					System.out.println("No results were found");
				}
				else {
					System.out.println("Top 10 Results:");
				}
					
				Integer count = 0;
				for(Results url : css.keySet()) {
					if(count >= 10) {
						break;
					}
					System.out.println("    " + url.getUrl() + ": " + url.getScore());

					count++;
				}
				



				System.out.println("");
				System.out.println("");
				System.out.println("Enter term");
			}
		}

		//close the database
		finally {
			try {
				tuple.close();
				accum.close();
				stats.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
