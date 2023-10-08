import pandas as pd
import random

n = 10
maxInt = 10000


features = [
    "x",
    "y"
]
dataset = pd.DataFrame(columns=features)
kmeans_dataset = pd.DataFrame(columns=features)


def dataset_creation():

    dataset["x"] = [random.randint(0, maxInt) for i in range(n)]
    dataset["y"] = [random.randint(0, maxInt) for i in range(n)]

    print(dataset)
    dataset.to_csv("datasetTest.csv", index=False, header=None)

x_list = []
y_list = []
def kmeans(k):
    for i in range(k):
        x = random.randint(0, maxInt)
        y = random.randint(0, maxInt)

        check_dups(x, y)

    kmeans_dataset["x"] = x_list
    kmeans_dataset["y"] = y_list
    print(kmeans_dataset)
    kmeans_dataset.to_csv("kmeansTest.csv", index=False, header=None)

dups = []
def check_dups(x, y):
    if ("x y" in dups):
        x = random.randint(0, maxInt)
        y = random.randint(0, maxInt)
        check_dups(x, y)
    else:
        dups.append(str(x) + " " + str(y))
        x_list.append(x)
        y_list.append(y)


if __name__ == '__main__':

    print("Main Function")
    dataset_creation()
    kmeans(2)

