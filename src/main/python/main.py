import pandas as pd
import random

n = 5000
maxInt = 10000


features = [
    "x",
    "y"
]
dataset = pd.DataFrame(columns=features)
kmeans_dataset = pd.DataFrame(columns=features)
kmedoids_dataset = pd.DataFrame(columns=features)


def dataset_creation():

    dataset["x"] = [random.randint(0, maxInt) for i in range(n)]
    dataset["y"] = [random.randint(0, maxInt) for i in range(n)]

    print(dataset)
    dataset.to_csv("datasetTest.csv", index=False, header=None)

x_list = []
y_list = []
dups = []

def kmeans(k):
    kmeans_dataset = pd.DataFrame(columns=features)
    x_list.clear()
    y_list.clear()
    dups.clear()

    for i in range(k):
        x = random.randint(0, maxInt)
        y = random.randint(0, maxInt)

        check_dups(x, y)

    kmeans_dataset["x"] = x_list
    kmeans_dataset["y"] = y_list
    print(kmeans_dataset)
    kmeans_dataset.to_csv("kmeans" + str(k) + ".csv", index=False, header=None)


def check_dups(x, y):
    if ("x y" in dups):
        x = random.randint(0, maxInt)
        y = random.randint(0, maxInt)
        check_dups(x, y)
    else:
        dups.append(str(x) + " " + str(y))
        x_list.append(x)
        y_list.append(y)

medoidsList = []
def duplicates(x):
    if x in medoidsList:
        x = random.randint(0, 5000)
        duplicates(x)
    else:
        medoidsList.append(x)



def kmedoids(filepath, k):

    data = pd.read_csv(filepath)
    xlist = []
    ylist = []

    for i in range(k):
        current = random.randint(0, 5000)
        duplicates(current)

    for i in range(k):
        print(dataset.values[medoidsList[i]][0])
        xlist.append(data.values[medoidsList[i]][0])
        ylist.append(data.values[medoidsList[i]][1])
    kmedoids_dataset["x"] = xlist
    kmedoids_dataset["y"] = ylist

    print(kmedoids_dataset)

    kmedoids_dataset.to_csv("kmedoidsTest.csv", index=False, header=None)




if __name__ == '__main__':

    print("Main Function")
    dataset_creation()
    x_list = []
    y_list = []
    dups = []
    kmeans(5)
    kmeans(10)
    kmeans(100)
    # kmedoids("dataset.csv", 5)

