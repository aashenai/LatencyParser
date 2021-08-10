import sys
import re
from matplotlib import pyplot as plt
import time
import sys
import mmap
import os
import tqdm

# Every operation is a dict with items "id", "type", and "time"
x = {}
y = {}
current_list = {}
lines = {}
addresses = {}
mylines = []


def process_line(line):
    if line[0] != "#":
        parts = line.split()
        try:
            timestamp = float(parts[3][:-1])
        except ValueError:
            return
        except IndexError:
            return
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
            try:
                if "qid" in part:
                    qid = int(part.split('=')[1][:-1])
                if "cmdid" in part:
                    cmdid = int(part.split('=')[1][:-1])
                if qid != 0 and cmdid != 0:
                    break
            except IndexError:
                return
        if cmd_type == 0:
            if nvme not in addresses:
                addresses[nvme] = {}
                lines[nvme] = {}
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
            lines[nvme][(qid, cmdid)] = line
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
                threshold = 10
                if len(sys.argv) > 2:
                    threshold = float(sys.argv[2])
                if latency > threshold and rw <= 1:
                    mylines.append(process_big_latency(str(threshold), round(latency / 1000, 6), qid, cmdid,
                                                       lines[nvme][(qid, cmdid)], start_time, timestamp))
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
        m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        while True:
            line = m.readline().decode("utf-8")
            if line == '':
                break
            process_line(line)
            i += 1
            if size > 10000:
                if i % (size // 10000) == 0:
                    if prog < 100:
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
    print()
    for line in mylines:
        print(line)
    print()
    time_taken = time.time() - start
    print("Time taken: " + str(round(time_taken, 2)) + " s")
    print("Plotting has started. Expected duration: " + str(round(time_taken / 7, 2)) + " s")


def process_big_latency(limit, latency, qid, cmdid, line, start_time, end_time):
    r1 = re.findall("slba=\w+", line)
    r2 = re.findall("len=\w+", line)
    lba = r1[0].split('=')[1]
    length = r2[0].split('=')[1]
    st = "latency > " + str(limit) + "ms " + str(latency) + " qid=" + str(qid) + ", cmdid=" + str(cmdid) + ", " + \
         "\033[1m" + lba + " (" + hex(int(lba)) + ")" + "\033[0m" + ", " + length + ", " + str(start_time) + ", " + str(end_time) + ", " + \
         str(latency)
    return st


def main(input_file):
    print(input_file)
    parse_input(input_file)

    start = time.time()

    for nvme in x:
        colors = ["bo", "ro", "go"]
        labels = ["read", "write", "trim"]
        fig, ax = plt.subplots()
        for i in range(0, 3):
            ax.plot(x[nvme][i], y[nvme][i], colors[i], markersize=1, label=labels[i])
        ax.set_title(nvme[:-1])
        ax.set_xlabel("Time")
        ax.set_ylabel("Latency (in ms)")
        ax.tick_params(axis='x', which='both', bottom=False, top=False,
                       labelbottom=False)
        leg = ax.legend(loc='upper left')
        for i in range(0, 3):
            leg.legendHandles[i]._sizes = [30]
        plt.savefig("Output.png")
        plt.show()

    print("Time taken for plotting: " + str(round(time.time() - start, 2))
          + " s")
    print()

    for nvme in x:
        labels = ["read", "write", "trim"]
        for i in range(0, 3):
            if len(y[nvme][i]) > 0:
                if i == 0:
                    print(nvme)
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


main(sys.argv[1])
