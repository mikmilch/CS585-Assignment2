import pandas as pd
import random


features = [
    "x",
    "y"
]
BYOD_kmeans_dataset = pd.DataFrame(columns=features)

dups = []
x_list = []
y_list = []
def check_dups(x, y):
    if ("x y" in dups):
        x = random.randint(0, 150)
        y = random.randint(0, 100)
        check_dups(x, y)
    else:
        dups.append(str(x) + " " + str(y))
        x_list.append(x)
        y_list.append(y)


def kmeans(k):
    BYOD_kmeans_dataset = pd.DataFrame(columns=features)
    for i in range(k):
        x = random.randint(0, 150)
        y = random.randint(0, 100)

        check_dups(x, y)

    BYOD_kmeans_dataset["x"] = x_list
    BYOD_kmeans_dataset["y"] = y_list
    # print(BYOD_kmeans_dataset)
    BYOD_kmeans_dataset.to_csv("BYOD_kmeans" + str(k) + ".csv", index=False, header=None)


if __name__ == '__main__':
    kmeans(5)
    dups = []
    x_list = []
    y_list = []
    kmeans(10)
    dups = []
    x_list = []
    y_list = []
    kmeans(100)