import sys

from matplotlib import pyplot as plt
import time
import sys
import math
import os
import tqdm

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
    prog = 0
    fin = False
    dur = False
    start = time.time()
    bar = tqdm.tqdm(total=10000)
    with open(address, "r") as f:
        for line in f:
            process_line(line)
            i += 1
            if size > 10000:
                if i % (size // 10000) == 0:
                    if prog < 100:
                        # time.sleep(0.1)
                        bar.update(1)
                    prog += 0.01
                if prog > 100.5 and not fin:
                    print("\nFinishing up")
                    fin = True
                if prog > 115 and not dur:
                    print("\nTaking longer than expected")
                    print("Check for IRQ or block commands")
                    dur = True
        while prog < 100:
            prog += 0.01
            bar.update(1)
    bar.close()
    time_taken = time.time() - start
    print("Time taken: " + str(round(time_taken, 2)) + " s")
    print("Plotting in progress")
    print("Expected duration: " + str(round(time_taken / 1.5, 2)) + " s")


def main():
    input_file = sys.argv[1]
    print(input_file)
    parse_input(input_file)

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

    print("Time taken for plotting: " + str(round(time.time() - start, 2))
          + " s")
    print()

    for nvme in x:
        print(nvme)
        labels = ["read", "write", "trim", "admin", "others"]
        for i in range(0, 5):
            if len(y[nvme][i]) > 0:
                print("Maximum " + labels[i] + " latency: "
                      + str(round(max(y[nvme][i]), 3)) + " ms")
                print("Average " + labels[i] + " latency: " +
                      str(round(sum(y[nvme][i]) / len(y[nvme][i]), 3))
                      + " ms")
                print("Minimum " + labels[i] + " latency: "
                      + str(round(min(y[nvme][i]), 3)) + " ms")
                print("Number of " + labels[i] + " commands: " +
                      str(len(y[nvme][i])))
                print()


main()
