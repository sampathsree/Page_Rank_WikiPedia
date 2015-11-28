/**
 * Name: Sampath Sree Kumar K
 * Email-id: skolluru@uncc.edu
 * Studentid: 800887568
 */
 package wiki.org;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InvertedIndexer {

	static ArrayList<String> stopwords = new ArrayList<String>();

	Map<String, List<Title_and_Position>> index = new HashMap<String, List<Title_and_Position>>();
	List<String> files = new ArrayList<String>();

	public void indexFile(File file) throws IOException {

		FileInputStream fstream = new FileInputStream("stopwords.txt");
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		while ((strLine = br.readLine()) != null) {
			stopwords.add(strLine);
		}
		// Close the input stream
		in.close();

		int pos = 0;
		BufferedReader reader = new BufferedReader(new FileReader(file));
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			int start = line.indexOf("<title>");
			int end = line.indexOf("</title>", start);
			start += 7; // add <title> length.

			String title = line.substring(start, end);
			int fileno = files.indexOf(title);
			if (fileno == -1) {
				files.add(title);
				fileno = files.size() - 1;
			}
			start = line.indexOf("<text");
			start = line.indexOf(">", start);
			end = line.indexOf("</text>", start + 1);
			start += 1;
			String text = line.substring(start, end);

			for (String _word : text.split("\\W+")) {
				String word = _word.toLowerCase();
				pos++;
				if (!stopwords.contains(word)) {
					List<Title_and_Position> idx = index.get(word);
					if (idx == null) {
						idx = new LinkedList<Title_and_Position>();
						index.put(word, idx);
					}
					idx.add(new Title_and_Position(fileno, pos));
				}
			}
		}
		System.out.println("indexed " + file.getPath() + " " + pos + " words");
	}

	public void search(List<String> words) {
		for (String _word : words) {
			Set<String> answer = new HashSet<String>();
			String word = _word.toLowerCase();
			List<Title_and_Position> idx = index.get(word);
			if (idx != null) {
				for (Title_and_Position t : idx) {
					answer.add(files.get(t.fileno));
				}
			}
			System.out.print(word + ": ");
			for (String f : answer) {
				System.out.print(f + "; ");
			}
			System.out.println("");
		}
	}

	public static void main(String[] args) {
		try {
			InvertedIndexer idx = new InvertedIndexer();

			String path = args[0];
			File[] f = new File(path).listFiles();
			
			idx.indexFile(f[0]);

			idx.search(Arrays.asList(args[1].split(",")));
		} catch (Exception e) {
			//e.printStackTrace();
		}
	}

	private class Title_and_Position {
		private int fileno;
		private int position;

		public Title_and_Position(int fileno, int position) {
			this.fileno = fileno;
			this.position = position;
		}
	}
}