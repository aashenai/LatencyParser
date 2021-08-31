import re
from matplotlib import pyplot as plt
import time
import sys
import mmap
import os
import tqdm
import csv

# Every operation is a dict with items "id", "type", and "time"
x = {}
y = {}
lines = {}
addresses = {}
read_latencies = []
write_latencies = []
thresholds = []


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
                addresses[nvme].pop((qid, cmdid))
                latency = (timestamp - start_time) * 1000
                x[nvme][rw].append(start_time)
                y[nvme][rw].append(latency)
                for i in range(len(thresholds) - 1, -1, -1):
                    if latency > thresholds[i]:
                        if rw == 0:
                            process_big_latency(read_latencies[i], nvme, rw, str(thresholds[i]), round(latency / 1000, 6),
                                                qid, cmdid, lines[nvme][(qid, cmdid)], start_time, timestamp)
                        if rw == 1:
                            process_big_latency(write_latencies[i], nvme, rw, str(thresholds[i]), round(latency / 1000, 6),
                                                qid, cmdid, lines[nvme][(qid, cmdid)], start_time, timestamp)
                        break
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
    print_counts(read_latencies, "read", address)
    print_counts(write_latencies, "write", address)
    time_taken = time.time() - start
    print("Time taken: " + str(round(time_taken, 2)) + " s")
    print("Plotting has started. Expected duration: " + str(round(time_taken / 7, 2)) + " s")


def process_big_latency(append_dict, nvme, rw, limit, latency, qid, cmdid, line, start_time, end_time):
    if nvme not in append_dict:
        append_dict[nvme] = []
    r1 = re.findall("slba=\w+", line)
    r2 = re.findall("len=\w+", line)
    lba = r1[0].split('=')[1]
    length = r2[0].split('=')[1]
    st = ""
    if rw == 0:
        st = "read "
    if rw == 1:
        st = "write "
    st += "latency > " + str(limit) + "ms " + str(latency) + " qid=" + str(qid) + ", cmdid=" + str(cmdid) + ", " + \
          lba + " (" + hex(int(lba)) + ")" + ", " + length + ", " + str(start_time) + ", " + str(end_time) + ", " + \
          str(latency)
    append_dict[nvme].append(st)


def print_counts(latency_list, cmd_type, address):
    counts = []
    filename = generate_file(address, "_" + cmd_type + "s", ".txt")
    f = open(filename, "w")
    f.write(cmd_type + " latency > limit latency qid, cmdid, LBA (LBA hex), length, start time, end time, latency\n")
    for i in range(len(thresholds)):
        for nvme in latency_list[i]:
            for line in latency_list[i][nvme]:
                f.write(line + "\n")
            f.write("\n")
            counts.append((thresholds[i], len(latency_list[i][nvme]), nvme))
    for triad in counts:
        print("Number of " + cmd_type + " latencies in " + triad[2][:-1] + " over " + str(triad[0]) + ": " +
              str(triad[1]))
    print()


def generate_file(input_file, tag, ext):
    output_dir = "./" + input_file[:-4] + "_results"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    output_file = output_dir + "/" + input_file[:-4] + tag + ext
    if os.path.exists(output_file):
        os.remove(output_file)
    return output_file


def main(input_file):
    if len(sys.argv) > 2:
        for num in sys.argv[2:]:
            thresholds.append(float(num))
            read_latencies.append({})
            write_latencies.append({})
    else:
        thresholds.extend([10, 15, 20, 25])
        read_latencies.extend([{}, {}, {}, {}])
        write_latencies.extend([{}, {}, {}, {}])

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
            leg.legendHandles[i]._sizes = [100]
        fig_file = generate_file(input_file, "_graph_" + nvme[:-1], ".png")
        plt.savefig(fig_file)
        plt.clf()

    print("Time taken for plotting: " + str(round(time.time() - start, 2))
          + " s")

    output_file = generate_file(input_file, "_output", ".csv")

    with open(output_file, "w", encoding="UTF8", newline='') as f:
        writer = csv.writer(f)
        headers = ["NVMe device",
                   "Min read lat", "Avg read lat", "Max read lat", "Num reads",
                   "Min write lat", "Avg write lat", "Max write lat", "Num writes",
                   "Min trim lat", "Avg trim lat", "Max trim lat", "Num trims",
                   "Min lat", "Avg lat", "Max lat", "Num operations"]
        writer.writerow(headers)
        for nvme in x:
            maxs = []
            mins = []
            ops = 0
            w_total = 0
            line = [nvme[:-1]]
            for i in range(0, 3):
                if len(y[nvme][i]) > 0:
                    _max = max(y[nvme][i])
                    _min = min(y[nvme][i])
                    _len = len(y[nvme][i])
                    _sum = sum(y[nvme][i])
                    _avg = _sum / _len
                    ops += _len
                    maxs.append(_max)
                    mins.append(_min)
                    w_total += _sum
                    line.extend([str(round(_min, 3)), str(round(_avg, 3)), str(round(_max, 3)), str(_len)])
                else:
                    line.extend([0, 0, 0, 0])
            try:
                line.append(str(round(min(mins), 3)))
                line.append(str(round(w_total / ops, 3)))
                line.append(str(round(max(maxs), 3)))
                line.append(str(ops))
            except ValueError:
                pass
            writer.writerow(line)


if len(sys.argv) > 0:
    main(sys.argv[1])
else:
    print("Correct syntax is python parser.py input_file.log")
    print("Enter input file: ")
    file = input()
    main(file)
