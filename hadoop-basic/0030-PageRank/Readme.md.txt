Dataset
The Internet is a pool of millions of domains and billions of pages. Each domain has multiple pages and a number of links from a page to another page within the domain is much higher than the number of links to a page from a different domain. For example, a Wikipedia page has many more in-links to other Wikipedia pages than to pages of other domains.

 

The same is true for UpGrad where the number of internal links (links to pages belonging to upgrad.com) is much higher than that of external links (links to pages outside upgrad.com). The dataset used for this demonstration is in adherence to the above property.

 

The dataset provided is a collection of 1 million web pages.  Each web page can be referred to as a node. The dataset contains two data files. The first file(pr1.txt) contains the initial PageRank values of the nodes. 

 

There are two columns in the file separated by tab(/t). The first column contains the node number and the second column contains the PageRank value for that node. The initial PageRank scores for each node is 0.000001(1/total number of nodes=10^6). The second file(snetwork.txt) is the network file. It contains two columns separated by tab(/t). The first column contains the node number, say u, and the second column contains a list of the comma(,) separated nodes to which links are going from u. There are 100 domains in the dataset each consisting of 10000 nodes.

 

For simplicity, the domains are divided in the following manner: first domain consists nodes from 0 to 9999, the 2nd domain contains nodes from 10000 to 19999 and so on. The 100th domain will hence contain nodes numbered from 990000 to 999999. The dataset is such that 80% of the outlines from a node are to the nodes from the same domain and 20% of the outlinks are to the pages from other domains.  The dataset used in the demonstration is given below.

 

To have a better idea about the dataset, Please look at the image provided below. Here we have plotted the network of first 18 Nodes from the snetwrok.txt file. you can see the graph clearly shows 18  strongly connected node of clusters.

Note: For the representation purpose( to visualize the data distribution) we have plotted the network as an Un-Directed graph. which by its very nature is represents a directed graph.

 

Here are the s3 links for the datasets. 

https://s3.amazonaws.com/page-rank-dataset/pr1.txt
https://s3.amazonaws.com/page-rank-dataset/snetwork.txt
 


