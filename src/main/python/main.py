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

medoidsList = []
def duplicates(x):
    if x in medoidsList:
        x = random.randint(0, 5000)
        duplicates(x)
    else:
        medoidsList.append(x)



# def kmedoids(k):
#     xlist = []
#     ylist = []
#
#     for i in range(k):
#         current = random.randint(0, 5000)
#         duplicates(current)
#
#     for i in range(k):
#         print(dataset.values[medoidsList[i]][0])
#         xlist.append(dataset.values[medoidsList[i]][0])
#         ylist.append(dataset.values[medoidsList[i]][1])
#     kmedoids_dataset["x"] = xlist
#     kmedoids_dataset["y"] = ylist
#
#     print(kmedoids_dataset)
#
#     # kmedoids_dataset.to_csv("kmedoidsTest.csv", index=False, header=None)
#

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
    # kmeans(2)
    kmedoids("dataset.csv", 5)

