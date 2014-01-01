Goal: Use KMeans to cluster a Movie dataset.

Movid data format:

product/productId: B0071AD95K

review/userId: A31YQQYX9PU04Z

review/profileName: Stranger in the land

review/helpfulness: 0/0

review/score: 4.0

review/time: 1323043200

review/summary: Great encouragement

review/text: Great encouragement on the intricate involvement in the lives of people. I actually sent the DVD's to friends going through challenges with their 17 year old son who has left home.

Step 1. Use TFIDF to normalize the terms in all the documents.

Step 2. For each document, generate a sparse vector with the TFIDF score as the value.

Step 3. Initialize the first batch of centroids.

Step 4. Use KMeans to cluster the movie data.

Initial Package is to do word count without normalization. In this way, kmeans algorithm will converge slowly. Not recommended.
