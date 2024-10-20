print("Start")

letter_count = 0

with open("dataset/wordcount/hamlet.txt", "r") as f:
    for line in f:
        print(line)
        # your code here

print(letter_count)

print("End")
