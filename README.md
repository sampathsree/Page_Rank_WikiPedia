# Page_Rank_WikiPedia
Running PageRank Algorithm on Wikipedia Data Set<br/>
<b>Steps to execute:</b><br/>
1. Setup Hadoop input folder with sample input.<br/>
2. Run Jar File<br/>
<i>hadoop jar PageRank.jar wiki.org.WikiPageRank_Runner input output “William,Sakura,wikipedia,basketball”</i><br/>
Here the arguments for the above command are as follows:<br/>
First Argument: INPUT DIRECTORY<br/>
Second Argument: OUTPUT DIRECTORY<br/>
Third Argument: SEARCH WORDS FOR INVERTED INDEX (OPTIONAL)<br/>
 - Here “input” is the input folder path.<br/>

<b>Assumptions:</b>
<p>1. Handled internal links within the pages. There are few links in the data in such format. Hence considered it as a self-link and calculated the page rank.</p>
<p>2. Replaced spaces with underscore(“_”) in title and page links as my logic of handling intermediate data is using spaces. So I just replaced spaces with underscores in title and page links, so that there would be no change in the data.
</p><p>3. Considered semi-colon (“;”) as my separator for the links.
</p><p>4. Calculated page rank only for pages in the corpus.
</p><p>5. Have calculated page rank even for pages which has no out links if and only if they are present in the corpus.
</p>
Implemented Inverted Indexer for text in the xml documents. InvertedIndexer.java files takes input folder and string of search words separated by comma.<br/>
Java InvertedIndexer input “William,Sakura,wikipedia,basketball”
