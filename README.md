# Million Song Dataset Analysis


A simple analysis on the Million Song Dataset. This analysis computes the average "tempo", "duration" and "loudness" of the top 100 songs for each year measured by the "song_hotness" value from the Million Song Dataset. 

It uses a simple MapReduce Task to read the input CSV and analyze the metadata of the million songs. 

### Mapper 
The Mapper task just reads the values, filter the songs without a year value and emits records (tempo, duration, loudness, song_hotness). 


### Reducer
The Reducer task takes in the input values and keeps a global Map of Priority Queues to only keep the top 100 songs for each year. 
Then on each reduce step the reducer checks if that specific year's Priority Queue exists in the Map and if it exists it updates the priority queue, and if it doesn't exist it creates a new Priority Queue and adds the value. 

Then in the final cleanup phase, it aggregates the values of the top100 songs and writes the output.
