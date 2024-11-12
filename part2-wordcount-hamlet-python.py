import re
import time

pattern = re.compile("^[mM][a-z]+$")

print("Start")
start_time = time.time()

# Initializing Dictionary
d = {}

with open('dataset/wordcount/hamlet.txt', 'r') as f:
    for line in f:
        for word in line.strip().split(" "):
            word_lower = word.lower()
            if pattern.match(word_lower):
                d[word_lower] = d.get(word_lower, 0) + 1

print(sorted(d.items(), key=lambda x: x[1], reverse=True)[:10])

print("End")
print("--- %s seconds ---" % (time.time() - start_time))
