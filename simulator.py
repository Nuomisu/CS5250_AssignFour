'''
CS5250 Assignment 4, Scheduling policies simulator
Sample skeleton program
Author: Minh Ho
Input file:
    input.txt
Output files:
    FCFS.txt
    RR.txt
    SRTF.txt
    SJF.txt
Apr 10th Revision 1:
    Update FCFS implementation, fixed the bug when there are idle time slices between processes
    Thanks Huang Lung-Chen for pointing out
Revision 2:
    Change requirement for future_prediction SRTF => future_prediction shortest job first(SJF), the simpler non-preemptive version.
    Let initial guess = 5 time units.
    Thanks Lee Wei Ping for trying and pointing out the difficulty & ambiguity with future_prediction SRTF.
'''
import sys
from collections import defaultdict

input_file = 'input.txt'

class Process:
    last_scheduled_time = 0
    def __init__(self, id, arrive_time, burst_time):
        self.id = id
        self.arrive_time = arrive_time
        self.burst_time = burst_time
        self.remain_time = burst_time
        self.last_scheduled_time = arrive_time
    #for printing purpose
    def __repr__(self):
        return ('[id %d : arrive_time %d,  burst_time %d]'%(self.id, self.arrive_time, self.burst_time))

def FCFS_scheduling(process_list):
    #store the (switching time, proccess_id) pair
    schedule = []
    current_time = 0
    waiting_time = 0
    for process in process_list:
        if(current_time < process.arrive_time):
            current_time = process.arrive_time
        schedule.append((current_time,process.id))
        waiting_time = waiting_time + (current_time - process.arrive_time)
        current_time = current_time + process.burst_time
    average_waiting_time = waiting_time/float(len(process_list))
    return schedule, average_waiting_time

#Input: process_list, time_quantum (Positive Integer)
#Output_1 : Schedule list contains pairs of (time_stamp, proccess_id) indicating the time switching to that proccess_id
#Output_2 : Average Waiting Time
def RR_scheduling(process_list, time_quantum ):
    schedule = []
    current_time = 0
    waiting_time = 0
    queue = []
    if (len(process_list) == 0):
        return schedule, float(waiting_time)
    current_job = -1
    # if queue is empty try to add next job
    # init the ready queue, put first task in
    while (1):
        if (len(queue) == 0):
            if (current_job == len(process_list) - 1):
                # end
                break
            else:
                # add next job in processlist and adjust current time
                current_job += 1
                current_time = process_list[current_job].arrive_time
                queue.append(process_list[current_job])
        else:
            # if there is some tasks in ready queue
            running_task = queue.pop(0)
            schedule.append((current_time, running_task.id))
            waiting_time += current_time - running_task.last_scheduled_time
            #print waiting_time, current_time, running_task
            if (running_task.remain_time <= time_quantum):
                # task done
                current_time += running_task.remain_time
                running_task.remain_time = 0
            else:
                # task not done
                current_time += time_quantum
                running_task.remain_time -= time_quantum
            running_task.last_scheduled_time = current_time
            # check if new task come
            while (current_job < len(process_list) -1 and current_time >= process_list[current_job+1].arrive_time):
                current_job += 1 
                queue.append(process_list[current_job])
            if (running_task.remain_time > 0):
                queue.append(running_task)
    average_waiting_time = waiting_time/float(len(process_list))
    return schedule, average_waiting_time

def SRTF_scheduling(process_list):
    return (["to be completed, scheduling process_list on SRTF, using process.burst_time to calculate the remaining time of the current process "], 0.0)

def SJF_scheduling(process_list, alpha):
    init_guess = 5
    schedule = []
    current_time = 0
    waiting_time = 0
    queue = []
    predict = {}
    if (len(process_list) == 0):
        return schedule, float(waiting_time)
    current_job = -1
    while (1):
        if (len(queue) == 0):
            if (current_job == len(process_list) - 1):
                # end
                break
            else:
                # add next job in processlist and adjust current time
                current_job += 1
                current_time = process_list[current_job].arrive_time
                queue.append(process_list[current_job])
        else:
            # if there is some tasks in ready queue
            # need to pick smallest predict burst time
            min_burst = 999999
            min_task = queue[0]
            min_index = -1
            for i, task in enumerate(queue):
                pburst = predict.get(task.id, init_guess)
                if pburst < min_burst:
                    min_burst = pburst
                    min_task = task
                    min_index = i
            # running task = min_task
            queue.pop(min_index)
            schedule.append((current_time, min_task.id))
            waiting_time += current_time - min_task.arrive_time
            current_time += min_task.burst_time
            # update predict for n+1
            predict[min_task.id] = alpha*min_task.burst_time + (1-alpha)*min_burst
            while (current_job < len(process_list) -1 and current_time >= process_list[current_job+1].arrive_time):
                current_job += 1 
                queue.append(process_list[current_job])
        #print current_time, queue
    average_waiting_time = waiting_time/float(len(process_list))
    return schedule, average_waiting_time


def read_input():
    result = []
    with open(input_file) as f:
        for line in f:
            array = line.split()
            if (len(array)!= 3):
                print ("wrong input format")
                exit()
            result.append(Process(int(array[0]),int(array[1]),int(array[2])))
    return result
def write_output(file_name, schedule, avg_waiting_time):
    with open(file_name,'w') as f:
        for item in schedule:
            f.write(str(item) + '\n')
        f.write('average waiting time %.2f \n'%(avg_waiting_time))


def main(argv):
    process_list = read_input()
    print ("printing input ----")
    for process in process_list:
        print (process)
    print ("simulating FCFS ----")
    FCFS_schedule, FCFS_avg_waiting_time =  FCFS_scheduling(process_list)
    write_output('FCFS.txt', FCFS_schedule, FCFS_avg_waiting_time )
    print ("simulating RR ----")
    RR_schedule, RR_avg_waiting_time =  RR_scheduling(process_list,time_quantum = 2)
    write_output('RR.txt', RR_schedule, RR_avg_waiting_time )
    print ("simulating SRTF ----")
    SRTF_schedule, SRTF_avg_waiting_time =  SRTF_scheduling(process_list)
    write_output('SRTF.txt', SRTF_schedule, SRTF_avg_waiting_time )
    print ("simulating SJF ----")
    a = 0.01
    min = 1000
    min_a = 0
    while a < 1.0:
        SJF_schedule, SJF_avg_waiting_time =  SJF_scheduling(process_list, alpha = a)
        print a, SJF_avg_waiting_time
        if SJF_avg_waiting_time < min:
            min = SJF_avg_waiting_time
            min_a = a
        a += 0.01
        #write_output('SJF.txt', SJF_schedule, SJF_avg_waiting_time )
    print min_a, min
if __name__ == '__main__':
    main(sys.argv[1:])
