from matplotlib import pyplot as plt
import time
import math
import os

# Every operation is a dict with items "id", "type", and "time"
x = {}
y = {}
current_list = {}
addresses = {}


def process_line(line):
    if line[0] != "#":
        parts = line.split()
        timestamp = float(parts[3][:-1])
        if parts[4] == "nvme_setup_cmd:":
            cmd_type = 0
        elif parts[4] == "nvme_complete_rq:":
            cmd_type = 1
        else:
            return
        nvme = parts[5]
        qid = 0
        cmdid = 0
        for part in parts:
            if "qid" in part:
                qid = int(part.split('=')[1][:-1])
            if "cmdid" in part:
                cmdid = int(part.split('=')[1][:-1])
        if cmd_type == 0:
            if nvme not in addresses:
                addresses[nvme] = {}
            rw = 4
            if "nvme_cmd_read" in line:
                rw = 0
            elif "nvme_cmd_write" in line:
                rw = 1
            elif "nvme_cmd_dsm" in line:
                rw = 2
            elif "nvme_admin_identify" in line:
                rw = 3
            addresses[nvme][(qid, cmdid)] = (timestamp, rw)
        elif cmd_type == 1:
            if nvme not in x:
                x[nvme] = [[], [], [], [], []]
                y[nvme] = [[], [], [], [], []]
            try:
                start_time = addresses[nvme][(qid, cmdid)][0]
                rw = addresses[nvme][(qid, cmdid)][1]
                x[nvme][rw].append(start_time)
                latency = (timestamp - start_time) * 1000
                y[nvme][rw].append(latency)
            except KeyError:
                return


def parse_input(address):
    size = (os.path.getsize(address)) // 152.3
    i = 0
    start = time.time()
    with open(address, "r") as f:
        for line in f:
            process_line(line)
            i += 1
            if i % (size // 10000) == 0:
                print(round(i / (size // 100), 2))
            # if i == 100000:
            #     break
    print("Time taken: " + str(time.time() - start))


parse_input("Input_0.log")

start = time.time()

for nvme in x:
    colors = ["lightblue", "red", "green", "orange", "black"]
    labels = ["read", "write", "trim", "admin", "others"]
    fig, ax = plt.subplots()
    for i in range(0, 5):
        ax.scatter(x[nvme][i], y[nvme][i], s=2, c=colors[i], label=labels[i])
    ax.set_title(nvme[:-1])
    ax.set_xlabel("Time")
    ax.set_ylabel("Latency (in ms)")
    ax.tick_params(axis='x', which='both', bottom=False, top=False,
                   labelbottom=False)
    leg = ax.legend(loc='upper left')
    for i in range(0, 5):
        leg.legendHandles[i]._sizes = [30]
    plt.savefig("Output.png")
    plt.show()

print("Time taken for plotting: " + str(time.time() - start))

for nvme in x:
    print(nvme)
    print("\n")
    for i in range(0, 5):
        if len(y[nvme][i]) > 0:
            print(len(y[nvme][i]))
            print(max(y[nvme][i]))
            print(sum(y[nvme][i]) / len(y[nvme][i]))
            print()
