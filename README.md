# Page_Rank_WikiPedia
Running PageRank Algorithm on Wikipedia Data Set

Steps to execute:
1. Setup Hadoop input folder with sample input.
2. Run Jar File
hadoop jar PageRank.jar wiki.org.WikiPageRank_Runner input output “William,Sakura,wikipedia,basketball”
Here the arguments for the above command are as follows:
First Argument: INPUT DIRECTORY
Second Argument: OUTPUT DIRECTORY
Third Argument: SEARCH WORDS FOR INVERTED INDEX (OPTIONAL)
- Here “input” is the input folder path.

Assumptions:
<p>Handled internal links within the pages. There are few links in the data in such format. Hence considered it as a self-link and calculated the page rank.</p>
2. Replaced spaces with underscore(“_”) in title and page links as my logic of handling intermediate data is using spaces. So I just replaced spaces with underscores in title and page links, so that there would be no change in the data.
3. Considered semi-colon (“;”) as my separator for the links.
4. Calculated page rank only for pages in the corpus.
5. Have calculated page rank even for pages which has no out links if and only if they are present in the corpus.

Implemented Inverted Indexer for text in the xml documents. InvertedIndexer.java files takes input folder and string of search words separated by comma.
Java InvertedIndexer input “William,Sakura,wikipedia,basketball”
